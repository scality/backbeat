<!-- markdownlint-disable -->
<!-- A verbatim copy of the POC write-up. Reflowing it would make
     this copy differ from the original, so it is not linted. -->

# BNaaS migration edge-case experiments - results

Rig: `/Users/anurag/capsule-corp/bnaas-poc/rig` (see `README.md` for how it
runs). Code under test: worktree
`/Users/anurag/capsule-corp/scality/backbeat-wg-merge` @ `poc/S3C-11127-workgroups`.
Nothing in any git repository was modified, committed, stashed or pushed.

**Caveat that applies to every number below.** Every backbeat process in these
scenarios, legacy *and* new, is started with the same three rig preload shims
(`--require conf/oplog-h-shim.js`, `conf/oplog-v2diff-shim.js`,
`conf/kafka-metadata-refresh-shim.js`). Without them the mongo `logSource`
cannot be decoded on MongoDB 5 and no notification consumer stays assigned long
enough to fetch. The legacy-versus-pool comparison is therefore fair, because
both sides carry the identical set, but neither side is stock on this host.

Instrumentation, identical in every scenario: the workload driver
(`scripts/driver.js`) carries a monotonic sequence number in the object **size**
(size = 1000 + seq) and logs every completed operation in order. The checker
(`scripts/check.py`) identifies an event as `(object key, Put|Delete, size)`,
which is unique per run, and reports per key: expected sequence, delivered
sequence, gaps, duplicates, inversions.

<!-- SUMMARY-START -->
## Summary

| # | scenario | loss | duplicates | per-key inversions | verdict |
|---|---|---|---|---|---|
| M2 | planned cutover with a backlog | **0** of 1168 | **0** | **0** | **PASS** |
| M2b | drain-then-switch cutover, no drainer | **0** of 601 | **69**, the worker's first-join replay of the delivery-topic backlog | **1**, inside the legacy processor before the switch, cause not established | **PASS** on loss; the duplicates are avoidable |
| M2c | drain-then-switch done too early, on purpose | **0** of 598 | **113** (20 populator + 93 worker replay) | **284** on 3 keys | the error costs ordering, not data |
| M4 | rollback after cutover | **161** stranded on the delivery topic | **106**, exactly the drained backlog | 0 | works, lossy by construction |
| M4b | mirror rollback of M2b, no drainer | **0** of 602 | **0** | **0** | **PASS**, and 0 M2b records re-delivered |
| M5 | mixed-version window | **0** of 657 | 0 for M5 events, plus 5 from M4's replay | 3190, all on the 3 keys spanning the rollback | **PASS**, and "every event delivered twice" does not reproduce |
| M6 | downgrade tolerance, populator | 0 in (a) and (b); a total stall in (c) | 0 | 0 | **worse than the design's own verify item** |
| M7 | downgrade tolerance, processor | **0** of 24 | 0 in window (24 later, on the roll-forward) | 0 | **PASS** |
| M8 | overlapping rules | 0 | 0 | 0 | both paths fan out; **neither implements FR10** |
| M9 | detach with backlog | legacy **20 of 20 dropped**, pool 0 | 0 | 0 | both expectations confirmed |
| M10 | destination down | legacy: 20 stuck, offsets never advance, no counter. Pool: 60 of 60 counted as dropped | 0 | 0 | legacy fails harder than predicted |
| M11 | populator `kill -9` mid-stream | **0** of 658 across two kills | **0** | **0** | **PASS** |
| M12 | worker `kill -9` mid-delivery | **0** of 489 | **89**, the uncommitted window | **0** | **PASS** |

M1 (legacy baseline) was done by the rig agent and is unchanged. M3 (cutover
with the populator left running) was not in this brief and was not run. M2b,
M2c and M4b test the design document's own migration path, which needs no replay
tool; "The two migration paths, side by side" compares it against the drainer
path measured by M2 and M4.

### The three findings that matter most

1. **An account-scoped ARN whose last segment matches a global destination is
   accepted by CloudServer and delivered to the global destination** (M6b).
   The populator and the processor match with `queueArn.split(':').pop()`, and
   CloudServer validates `queueArn.split(':')[5]`; all three discard the account
   field. `arn:scality:bucketnotif::123456789012:poc-dest-1` returned HTTP 200,
   was read back verbatim, and all 12 of its events arrived on the global
   `poc-dest-1` carrying a `configurationId` that destination never configured.
   Any tenant who can PUT a bucket notification configuration can do this today,
   with no new code deployed. Anything that introduces account-scoped
   destinations has to fix the matcher first.
2. **A bucket notification configuration the code cannot parse bricks the bucket
   and stops notifications for every tenant** (M6c). A `queueConfig` entry with
   no `queueArn` fails the `validateConfig` assert inside `BucketInfo`'s
   constructor, so `PutObject`, `GetObject` **and
   `PutBucketNotificationConfiguration` itself** return 500 on that bucket: it
   cannot be repaired through the API at all, only by a direct metadata write.
   The same configuration crashes the populator into
   `bin/queuePopulator.js:42 process.exit(1)` with its ZooKeeper offset
   unchanged, so it crash-loops forever and a healthy tenant's events in a
   different bucket stopped being delivered for the whole window. Nothing is
   lost, but the stall is unbounded and needs a metadata write to clear.
3. **A dead destination stalls today's pipeline silently rather than dropping**
   (M10), and the pool is the fix. A per-destination processor whose destination
   is unreachable **cannot start at all** (`Client is disconnected`, then exit).
   One that is already running attempts only `concurrency` records, gives up
   after librdkafka's 300 s default, **never advances its consumer offset on any
   partition**, writes nothing to the configured failed topic (no code reads
   `notificationFailedTopic`), exposes no counter, and stays up after its group
   assignment has been lost. The pool worker started fine with four dead
   destinations, dropped all 60 records with `dropped_total{target,reason}`
   correct per destination, gave up in ~30 s (`deliveryTimeoutMs`) or instantly
   on a connect failure, committed past the drops, and kept delivering 5 of 5 to
   a healthy destination at the same time.

### Every other defect or surprise hit along the way

- **The `BackbeatConsumer` wedge fired on 5 of 21 consumer starts** (M2 and M5
  on the delivery worker, M8, M10 and M4b on the legacy processor), and in M4b it
  landed on a step the migration procedure cannot skip. A wedged consumer
  is a live group member holding all its partitions with `CURRENT-OFFSET '-'`,
  cycling `assign -> revoke -> "processing queue idle, un-assigning"` about once
  a second and delivering nothing, while `isReady()` and therefore the liveness
  probe answer 200 and `/metrics` shows no delivery counters at all. It is not
  caused by the delivery pool, and the rig's 2000 ms
  `topic.metadata.refresh.interval.ms` shim does not prevent it. Every
  occurrence cleared on restart, but the restart costs up to 40 s waiting for
  the wedged member's group session to expire. Any runbook needs an explicit
  "is it actually delivering" check after every process start, because
  "connected and assigned" is not it.
- **One unexplained per-key inversion inside the legacy processor.** In M2b a
  DELETE was delivered before the PUT of the same object, both by the legacy
  processor before the switch. The populator published them in the right order
  (internal topic partition 3, offsets 346 then 347, same record key), and the
  processor's own log shows it issued the two sends in the wrong order 5 ms
  apart, so librdkafka did not reorder anything. Both paths **do** serialise
  same-key entries: `lib/BackbeatConsumer.js:116-118` defaults `orderByFunc` to
  the kafka message key, the legacy populator keys records
  `<bucket>/<objectKey>`, and the delivery worker uses the finer
  `destinationId|bucket/key`; a probe of the repository's own `TaskScheduler`
  confirms same-key serialisation, and the callback it waits on is the
  destination delivery report, not the hand-off. The remaining candidate is
  consume() responses arriving out of order, which `BackbeatConsumer` warns
  about at lines 425-428, but that could not be confirmed because per-entry
  consumption order is logged only at `debug`. Frequency: 1 in roughly 1200
  legacy-delivered events; M4b's 602 had zero. **Neither path should be
  described as ordering per key without qualification until this is
  reproduced.**
- **A destination the populator does not know about is lost silently**
  (`evidence/M10/silent-skip-finding.md`). 21 objects PUT into a bucket whose
  configuration CloudServer had accepted produced 0 internal-topic records while
  the populator's ZooKeeper offset kept advancing, with no log line at all,
  because the populator process predated the destination being added to its
  config. `lib/queuePopulator/LogReader.js:353` logs `batch completed` at info
  only when a batch queued something or skipped over a thousand records, so a
  populator that has stopped matching anything is indistinguishable from an idle
  one. Both destination lists are read once at process start, so restarting
  CloudServer before the populator opens a window of permanent, silent loss.
- **`delivery_timeout` is dead code.** `DeliveryWorker.processKafkaEntry`
  compares `sendErr.code` against `ERR__MSG_TIMED_OUT` (-192), but the error
  arriving from `BackbeatProducer._onDeliveryReport` carries `code: -1`, so a
  genuine `message.timeout.ms` expiry is labelled `delivery_error`.
  `dropped_total{reason}` cannot tell a timeout from a rejection.
- **One destination occupies exactly one partition of the delivery topic.**
  `buildDeliveryKey` returns the bare resource name when `spreadFactor` is 1, so
  all 1168 M2 records keyed `poc-dest-1` went to partition 2 and partitions 0
  and 1 stayed empty. Single-destination parallelism is one worker regardless of
  how many are started; capacity planning goes through `spreadFactor`, not the
  topic's partition count.
- **The pool's duplicate bound is the librdkafka auto-commit interval, not
  `concurrency`.** M12 produced 89 duplicates against a configured concurrency
  of 1000, because `lib/BackbeatConsumer.js:210` auto-commits every 5 s. That
  interval is not exposed anywhere in the `deliveryPool` schema, so an operator
  who needs a tighter bound cannot ask for one.
- **The populator's duplicate window is narrower than the criterion assumes.**
  A batch accumulates in memory, is produced to Kafka in one `send`, and only
  then writes its ZooKeeper offset. A kill during read/filter costs nothing; a
  kill between the Kafka acknowledgement and the offset write duplicates the
  **whole** batch, up to `batchMaxRead` (10000 here). Two `kill -9`s never hit
  the narrow window.
- **Adding one destination costs two static lists and two service restarts.**
  CloudServer's `bucketNotificationDestinations` and backbeat's
  `extensions.notification.destinations` are both read once at startup, and the
  legacy path also needs one more `queueProcessor` process per destination.
- **CloudServer's filter rule names are case sensitive, the matcher's are not.**
  `"Name": "prefix"` is refused with `MalformedXML: filter Name must be one of
  Prefix or Suffix`, while `validateEntryWithFilter` compares
  case-insensitively.
- **A rollback followed by a roll-forward reorders per-key history.** M5
  recovered all 161 records M4 had stranded, so cumulative loss over M2+M4+M5 is
  zero, but the replay arrived after the legacy path had already delivered later
  operations on the same keys: 3190 pairwise inversions, all of them on the
  three keys that had operations on both sides of the rollback.
- **The M7 downgrade route duplicates on the way back up.** Pointing legacy
  processors at the delivery topic works and never crashes, but they consume
  under their own consumer group, so restarting the pool re-delivered all 24
  records (verified against the M7 driver log).
- **There is no reverse drainer.** `bin/notificationDeliveryReplay.js` only goes
  internal-topic -> delivery-topic. Recovering records stranded on the delivery
  topic after a rollback would need the opposite tool, and M4's 161 stranded
  records are the case it would cover.
- Rig-level, not product defects: `min.insync.replicas=2` on an RF=1 topic did
  not block an `acks=all` produce on this broker, and
  `auto.create.topics.enable` cannot be turned off dynamically, which is why the
  M10 third failure class had to be built with `--replica-assignment 99`.
<!-- SUMMARY-END -->

---

## M2 - planned cutover with a backlog

**Start state:** the rig exactly as parked (legacy populator running, queue
processor `poc-dest-1` stopped, 10 undelivered events `k-100`..`k-109` on the
internal topic, `customer-topic-1` at 25, no delivery topic).
Evidence: `evidence/M2/`.

**Procedure:** a driver PUT to `poc-bucket` at 2 ops/s for the entire nine
minutes of the procedure, with three fixed keys taking a PUT-then-DELETE every
fifth operation so they straddle every boundary. Then: record offsets; SIGINT
the populator (it finished its batch, the ZooKeeper log offset was identical
before and after); create `bucket-notification-delivery` P=3 RF=1; run
`bin/notificationDeliveryReplay.js m2cutover` with the pool config; restart the
populator on `conf/backbeat-notification-pool.json`; start one
`deliveryWorker/task.js`; two more minutes of load; stop the driver; wait for
lag 0.

| measurement | value |
|---|---|
| driver operations (rc=ok) | 1158 |
| parked backlog objects | 10 |
| expected events | 1168 |
| delivered to `customer-topic-1` | 1168 (offsets 25..1192) |
| distinct object keys | 785 |
| Put / Delete events | 975 / 193 |
| **gaps** | **0** |
| **duplicate extras** | **0** |
| **per-key inversions** | **0** |
| unexpected events | 0 |
| drainer drained / produced / skipped | 106 / 106 / 0, exit 0 |
| legacy group offsets before -> after | 10/12/11/12 -> 10/12/11/12 |
| `bn-replay-*` groups registered on the broker | 0 |
| delivery topic spread | 1168 records, all on partition 2 of 3 |

**Verdict: PASS** on every criterion. Zero loss across the boundary including
the parked backlog, zero duplicates, zero per-key inversions on keys that were
PUT before and DELETEd after each step, and the drainer left the legacy group's
committed offsets untouched (its lag afterwards is exactly the 106 records it
copied, so a rollback can still resume there).

Duplicates were zero because the populator stopped on a batch boundary. The
duplicate window the plan predicted is real but did not open here; M11 measures
it deliberately.

**Deviations and findings.**

1. **The design/06 consumer wedge fired on the first worker start.** The worker
   connected, was assigned all three partitions, and cycled
   `assign -> revoke -> "processing queue idle, un-assigning" -> resume` 67 times
   in 141 s while delivering nothing. The delivery topic grew from 325 to 601+
   records; `customer-topic-1` stayed at 25. From outside it looked healthy: a
   live group member holding all three partitions, and `isReady()` (hence the
   liveness probe) would have answered 200, because it only asks whether the
   consumer object is ready. The rig already carries the 2000 ms
   `topic.metadata.refresh.interval.ms` shim, so that is not sufficient to
   prevent it. Restarting the process cured it: 1 assign, 0 revoke, stable for
   the rest of the run. Backbeat was not patched.
   Evidence: `evidence/M2/worker-wedged.log`.
2. **A destination occupies exactly one partition of the delivery topic.**
   `buildDeliveryKey` returns the bare resource name when `spreadFactor` is 1,
   so all 1168 records keyed `poc-dest-1` hashed to partition 2 and partitions 0
   and 1 stayed empty. Single-destination throughput in the pool is therefore
   one worker no matter how many are started, and capacity planning has to go
   through `spreadFactor` rather than through the topic's partition count.

---

## M2b - drain-then-switch cutover, no drainer

The design document's own migration path: the legacy per-destination
`QueueProcessor` is the doc's "single-destination-mode worker, generation v0",
so the cutover is to point the populator at the delivery topic, let the legacy
processors finish the legacy topic, start the pool, stop legacy. No replay CLI,
no throwaway consumer group. Script `scripts/m2b-drain-then-switch.sh`,
evidence `evidence/M2b/`.

**Start state:** legacy path with both processes running and **caught up**
(legacy group lag 0 on all four partitions), `customer-topic-1` at 4122,
internal topic at 1174, delivery topic at 3543 with its group at lag 0, no
worker. Getting there from M10 drained a 60-record legacy backlog that delivered
**nothing**, because all 60 belonged to buckets configured for `poc-dest-3/4/6`
and the `poc-dest-1` processor re-matched and skipped every one.

**Procedure:** driver at 2 ops/s with three straddle keys throughout. Record
offsets; restart the populator on the pool config (internal topic froze at 1223
and never moved again); wait for the legacy group to reach lag 0; start the
delivery worker; stop the legacy processor; two more minutes of load; wait for
lag 0.

| measurement | value |
|---|---|
| driver operations | 601 |
| delivered | 670 |
| unique delivered | 601 |
| **gaps** | **0** |
| **duplicate extras** | **69** |
| **per-key inversions** | **1** |
| delivered by the legacy processor | 49 (offsets 4122..4170) |
| delivered while both consumers were alive | 321 |
| delivered after legacy stopped | 300 |
| legacy group committed at step (e) / internal head | 1223 / 1223, lag 0 |
| populator restart gap | 9.1 s (the scripted wait, not a code property) |
| **legacy drain time after the switch** | **0 s of real work**: lag was already 0 at the switch |
| end-to-end latency of all 670 events | min 1.5 s, p50 9.1 s, p95 45.1 s, max 63.1 s, 126 over 30 s |

Latency here is the customer-topic record timestamp minus the event's own
`eventTime`, which the oplog truncates to the second. The p95 of 45 s is the
records that waited on the delivery topic during the switch plus the worker's
catch-up; the 1.5 s floor is the populator's batch cadence, which every path on
this rig pays.

**Verdict: PASS on loss, and the procedure works with no new tooling.** Zero
gaps over 404 keys, and the legacy processor's committed offset ended exactly at
the frozen internal-topic head, which is what makes the mirror rollback clean.

**The 69 duplicates are not the cause the plan predicted.** Both copies of every
duplicated event were delivered by the **worker**, and its own log says why:

```
12:46:21.763  rdkafka.assign
12:46:22.773  rdkafka.revoke                 queueLen=0 running=0
12:46:22.774  processing queue idle, un-assigning
12:46:22.778  rdkafka.assign
```

69 is exactly the delivery-topic backlog that had accumulated between the
populator switch and the worker start (3060 - 2991). The worker fetched that
backlog under its first assignment, took the ordinary first-join revoke before
those offsets were committed, and the reassignment replayed all of it. Same
mechanism as M12, reached by a completely normal startup rather than a kill.
**The fix is free: start the worker before switching the populator.** It then
joins a topic with nothing to replay, takes its first-join rebalance harmlessly,
and is already stable when the first addressed record arrives.

**The single inversion happened inside the legacy processor before the switch,
and its root cause is not established.** `Delete:1005` on `m2b-strad-1` arrived
at customer-topic offset 4128 and `Put:1005` at 4131, both delivered by the
legacy processor at 12:45:24.79, before the populator was switched. Four causes
were checked and ruled out:

- **Not a checker artefact.** The driver logged `PUT ... 1005 ok` at
  12:45:19.801 and `DELETE ... 1005 ok` at 12:45:19.840, and both delivered
  events carry `size` 1005 and the record key `poc-bucket/m2b-strad-1`.
- **Not the populator.** On the internal topic both records are on partition 3,
  offset **346 = Put**, **347 = Delete**, with the identical record key
  (`poc-bucket%2Fm2b-strad-1` on the wire). Published order is correct.
- **Not producer-side reordering.** The processor's own log shows
  `sending message to external destination` for the **Delete at 12:45:24.790**
  and for the **Put at 12:45:24.795**, and the customer-topic offsets and
  CreateTimes match that send order exactly. librdkafka delivered what it was
  handed, in the order it was handed it.
- **Not a missing ordering guarantee, which is what an earlier draft of this
  section claimed.** `lib/BackbeatConsumer.js:116-118` defaults `orderByFunc` to
  `ctx => ctx?.entry?.key?.toString()`, so the legacy processor **does**
  serialise same-key entries, and the populator keys internal-topic records
  `<bucket>/<objectKey>` (`NotificationQueuePopulator.js:260-263`). A probe of
  the repository's own `TaskScheduler` with that default and two entries sharing
  a key serialises them correctly (`scripts/probe-taskscheduler.js`:
  `START 346, SEND 346, START 347, SEND 347`). The serialisation also waits for
  the destination ack rather than the hand-off: `processKafkaEntry` ends with
  `this._destination.send([msg], done)`
  (`queueProcessor/QueueProcessor.js:305`), and `BackbeatProducer._sendToTopic`
  resolves that callback only once every delivery report has arrived
  (`pendingReportsCount` / `cbOnce`).

What is left is that the two entries reached the scheduler out of order.
`BackbeatConsumer` documents exactly that hazard at
`lib/BackbeatConsumer.js:425-428` ("there might be delay in the transport layer
causing the responses of the consume() calls to be received out of order in a
case of concurrent calls") and guards it with `_nConsumePendingRequests`; the
inversion appeared in the processor's very first consume burst after assignment.
This could not be confirmed from the captured logs, because per-entry
consumption order is recorded only at `debug` level and the rig ran at `info`.

**Frequency: one inversion in roughly 1200 events delivered by the legacy
processor across the whole experiment**, and M4b's 602 legacy-delivered events
had zero. It is rare, it is not caused by the migration, and it is not
explained. Reproducing it deliberately would need the processor re-run at debug
level under the same first-burst conditions, which is worth doing before the
migration document claims either path orders per key without qualification.

---

## M2c - drain-then-switch done wrong: starting the worker before lag 0

The operator error inside the M2b procedure. Script
`scripts/m2c-switch-too-early.sh`, evidence `evidence/M2c/`.

**This had to be forced.** On a rig whose legacy processor is keeping up the
error is unreachable: M2b measured a legacy lag of exactly 0 at the instant of
the switch, so "too early" and "on time" are the same moment. The error only
exists when the legacy side is genuinely behind, so this run `SIGSTOP`s the
legacy processor for 60 s to build a real backlog, switches the populator while
it is still paused, then releases it **at the same instant** the worker starts.

At that instant: legacy lag **159**, pool lag **93**.

| measurement | value |
|---|---|
| driver operations | 598 |
| delivered | 711 |
| unique delivered | 598 |
| **gaps** | **0** |
| **duplicate extras** | **113** |
| **per-key inversions** | **284, on 3 keys** |
| delivered before the release (legacy only) | 57 |
| delivered while both drained concurrently | 396 |
| delivered after legacy stopped | 258 |
| end-to-end latency | min 1.2 s, p50 17.2 s, p95 108.0 s, max 136.0 s |

**Same-key inversions: yes, 284.** All of them on the three straddle keys, the
only keys with operations on both sides of the boundary (`m2c-strad-2` 140,
`m2c-strad-0` 126, `m2c-strad-1` 18, over 66 operations each). The mechanism,
from `m2c-strad-2`:

```
expected  : ... Put:1040 Delete:1040   Put:1055 Delete:1055 Put:1070 Delete:1070 ...
delivered : ... Put:1040 Delete:1040   Put:1175 Delete:1175 Put:1190 Delete:1190 ...
```

Operations 1055 onwards were on the **legacy** topic behind the pause;
1175 onwards went to the **delivery** topic after the switch. With both
consumers running, the worker delivered the newer operations first, so a
customer tracking this object saw its state go forward to 1175 and then back to
1055. Single-operation keys are unaffected, which is why the count is large but
confined to three keys.

**The 113 duplicates decompose exactly:** 20 from the populator's uncommitted
batch at the switch, republished to the delivery topic (their first copy was
delivered by the legacy processor before the release, their second by the worker
after it, so those events existed on both topics), plus 93 from the worker's
first-join revoke and reassign, and 93 is **exactly** the pool lag when the
worker started. The 20 are the duplicate source the plan predicted for this step
and that M2b did not show; the 93 are M2b's mechanism again, sized by whatever
backlog was waiting when the worker joined.

**Verdict: no loss, 0 gaps over 403 keys. The cost of the error is ordering,
which is the one thing the migration is otherwise able to promise:** 284 per-key
inversions against 1 in M2b and 0 in M4b. So "wait for lag 0" is the only thing
standing between drain-then-switch and per-key reordering, because the two
topics are drained by two independent consumers with no barrier between them.

---

## M4 - rollback after cutover

**Start state:** end of M2 unchanged. Populator on the pool config, one healthy
worker, delivery group at lag 0 (committed 1168/1168), `customer-topic-1` at
1193, legacy processor stopped with committed offsets 10/12/11/12 and lag 106.
Evidence: `evidence/M4/`.

**Procedure:** driver on at 2 ops/s throughout. `kill -STOP` the worker until 75
records had piled up undelivered, then `kill -KILL` it. Stop the populator,
restart it on the legacy config. Start the legacy `queueProcessor poc-dest-1`.
Run 3 more minutes, stop the driver, wait for legacy lag 0.

| measurement | value |
|---|---|
| driver operations | 759 |
| delivered into the M4 window | 704 |
| **re-delivered by the legacy processor** | **106** (77 keys: 67 `m2-*`, 10 `k-10x`) |
| re-delivery set | exactly the 106 records the M2 drainer had copied |
| **stranded on the delivery topic, never delivered** | **161** |
| M4 events expected / delivered | 759 / 598 |
| M4 keys with gaps | 109 of 510 |
| per-key inversions | 0 |
| cumulative M2+M4 expected / delivered / gaps / duplicate extras | 1927 / 1872 / 161 / 106 |

The two effects separate perfectly: every duplicate is an M2 key, every gap is
an M4 key, no key has both. The loss arithmetic closes exactly. At the kill the
delivery group had committed 1238 and the topic ended at 1404, so 166 records
were uncommitted, 5 of which had actually been delivered, leaving 161.

**Verdict: rollback works but is lossy by construction.** Re-delivery is
unavoidable and bounded by the internal-topic lag at cutover (the drainer never
commits on the legacy groups, which is what makes rollback possible at all).
Loss is whatever the worker pool had not committed when it died, because the
populator's ZooKeeper log offset has already moved past those oplog entries, so
the legacy path never re-emits them. They sit on `bucket-notification-delivery`
with no consumer: readable, addressed, and orphaned.

**Reverse procedure that would be needed** (nothing in the tree does this):
stop the **populator** first, not the workers; let the pool reach lag 0 on the
delivery topic and gate on that explicitly; only then stop the workers and
restart the populator on the legacy config. Then either accept the re-delivery,
or reset the legacy groups to the internal-topic head, which trades the
duplicates for losing whatever the drainer copied but the pool had not
delivered. There is no ordering of today's tools that gives neither. If a worker
has already died with lag, recovering those records needs a **reverse drainer**
(read the delivery topic from the group's committed offset, strip
`destinationId`/`configurationId`, republish to each destination's internal
topic keyed `<bucket>/<key>`). `bin/notificationDeliveryReplay.js` only goes one
way. Recommendation: write it before the first customer cutover, because the
failure it covers is one `kill -9` of the last worker.

---

## M4b - mirror rollback of M2b, no drainer

The mirror of M2b: point the populator back at the legacy topic, let the pool
drain the delivery topic to lag 0, start the legacy processor, stop the worker.
Script `scripts/m4b-switch-back.sh`, evidence `evidence/M4b/`.

**Start state:** end of M2b unchanged. Populator on the pool config, one worker
at lag 0 with the delivery topic at 3543, legacy processor stopped with its
group committed at **1223**, exactly the internal topic's frozen head.
`customer-topic-1` at 4792.

**Procedure:** driver at 2 ops/s throughout. Restart the populator on the legacy
config (delivery topic froze at 3595 and never moved again); wait for the
delivery group to reach lag 0, which it already was; start the legacy processor,
whose group resumed at 1223 against an internal head of 1313, so a lag of
exactly the 90 records published since the switch; stop the worker; two more
minutes of load; wait for lag 0.

| measurement | value |
|---|---|
| driver operations | 602 |
| delivered | 602 |
| **gaps** | **0** |
| **duplicate extras** | **0** |
| **per-key inversions** | **0** |
| **M2b records re-delivered** | **0** |
| legacy group lag at step (c) | 90, exactly the post-switch records |
| legacy group at the end | lag 0 on all four partitions |
| delivery topic after the switch | frozen at 3595 |
| pool drain time after the switch | 0 s of real work: lag was already 0 |
| end-to-end latency | min 2.2 s, p50 112.2 s, p95 225.2 s, max 258.2 s |

The latency figures measure the wedge below, not the procedure: nothing was
delivered for about 90 s and then roughly 400 records had to be caught up.

**Verdict: PASS, 0 / 0 / 0.** Because M2b left the legacy group committed
exactly at the frozen internal-topic head, the legacy processor resumed at the
boundary and read only what was published after the switch. Nothing replayed,
nothing stranded.

| | M4, rollback after the drainer path | M4b, rollback after drain-then-switch |
|---|---|---|
| re-delivered | **106**, exactly what the drainer copied | **0** |
| stranded | **161**, everything the pool had not committed | **0** |
| why | the drainer deliberately never commits on the legacy groups, so the legacy processor resumes far behind, while the populator's log offset has already passed what the pool never delivered | the legacy group was left committed at the boundary and the pool was drained to lag 0 before its worker stopped |
| ordering | 0 in M4 itself, but rolling forward again reordered per-key history (M5: 3190 inversions on 3 keys) | 0 |

The drainer path's rollback cost is not a bug in the drainer. It is the price of
the drainer's own safety property, never moving the legacy offsets so a failed
replay can be rerun. Drain-then-switch pays nothing on rollback because it never
creates the divergence.

**The wedge landed on a procedure step this time.** Starting the legacy
processor in step (c) produced 53 assigns, 53 revokes and **0 sends**. That is
the fifth wedge of the experiment and the first on a step the procedure cannot
skip: drain-then-switch depends on a consumer reaching lag 0, and a wedged
consumer looks like a healthy group member with a lag figure that simply stops
falling. It cost 90 s and a restart here; on a cutover with a real backlog it
would stall indefinitely and look like slow progress. The runbook should gate on
lag 0 **and** on the consumer's own delivery counter moving, and treat "lag
stopped falling" as a restart trigger rather than as patience.

---

## The two migration paths, side by side

M2 and M4 measure the replay path: stop the populator, run
`bin/notificationDeliveryReplay.js` to copy the legacy topic's undelivered tail
into the delivery topic, then start the pool. M2b and M4b measure the design
document's own path, which needs no replay tool at all: the legacy
per-destination `QueueProcessor` **is** the doc's "single-destination-mode
worker, generation v0", so the cutover is to switch the populator to the
delivery topic, let the legacy processors finish the legacy topic, start the
pool, and stop legacy.

| | replay path (M2 / M4) | drain-then-switch (M2b / M4b) |
|---|---|---|
| operator steps | 7 | 5 |
| tools beyond a config change and a restart | the replay CLI plus a throwaway consumer group | **none** |
| populator downtime | one restart **plus the whole drainer run** | one restart |
| notifications paused | for the restart and the drain | for the restart only |
| precondition | the legacy topic must be static, so the populator must be down first | every legacy processor must reach lag 0, so each must be alive with a reachable destination |
| loss on cutover | **0** of 1168 | **0** of 601 |
| duplicates on cutover | **0** | **69**, the worker's first-join replay of the delivery-topic backlog; avoidable by starting the worker before the switch |
| inversions on cutover | **0** | **1**, delivered by the legacy processor **before** the switch, so not caused by the cutover. Root cause not established; see the M2b section |
| loss on rollback | **161** stranded on the delivery topic | **0** |
| duplicates on rollback | **106**, exactly what the drainer copied | **0** |
| ordering after a rollback and a second roll-forward | **3190 inversions on 3 keys** (M5) | not applicable, nothing diverges |
| the case it cannot handle | none operationally, but the rollback cost above is unavoidable, and there is no reverse drainer to recover records stranded on the delivery topic | a legacy processor that is **wedged, crash-looping, or whose destination is dead never reaches lag 0**, so the cutover cannot complete. M10 shows a dead destination stops a processor from even starting, and the wedge hit this very step in M4b |

**Recommendation.** Drain-then-switch is the better default: fewer steps, no new
tooling, no populator downtime beyond a restart, and a rollback that costs
nothing. Two conditions make it safe, and both are cheap:

1. **Start the delivery worker before switching the populator**, not after. That
   removes the 69 duplicates entirely, because the worker joins an empty topic
   and takes its first-join rebalance with nothing to replay.
2. **Gate step (c) on the legacy processor's delivery counter moving, not on its
   lag alone.** A wedged consumer holds its partitions with a lag figure that
   simply stops falling, and `isReady()` still answers 200 (M4b, and four
   earlier occurrences).

Keep the replay path for the case drain-then-switch cannot handle: a destination
that is down or a processor that cannot be brought up. There the legacy topic's
tail can only be moved by the drainer, and its rollback cost is then the price
of getting the cutover done at all.

---

## M5 - mixed-version window

**Start state:** end of M4. Populator on the legacy config, legacy processor
running at lag 0, no worker, `customer-topic-1` at 1897, delivery topic frozen
at 1404 with the group committed at 1238. Evidence: `evidence/M5/`.

**Procedure:** driver on at 2 ops/s. 30 s control with only the legacy path
live; start the delivery worker (both consumers live); flip the populator to the
pool config and hold the mixed window 60 s under load; stop the legacy
processor; stop the driver; wait for lag 0.

| measurement | value |
|---|---|
| driver operations | 657 |
| **M5 events expected / delivered / gaps / duplicates / inversions** | **657 / 657 / 0 / 0 / 0** |
| routed through the internal topic (legacy path) | 350 |
| routed through the delivery topic (pool path) | 307 |
| 350 + 307 | 657, exactly the driver's count |
| M4-stranded records replayed by the restarted worker | 166 |
| of which already delivered in M4 (new duplicates) | 5 |
| of which recovered (the M4 "loss") | 161 |
| cumulative M2+M4+M5 gaps | **0** |
| cumulative duplicate extras | 111 = 106 (M4 re-delivery) + 5 |
| cumulative per-key inversions | 3190, all on 3 keys (`m4-strad-0/1/2`) |

**Verdict: nothing lost, and the plan's expectation does not reproduce.** There
were zero double deliveries of M5 events, because
`NotificationQueuePopulator._processObjectEntry` is an if/else: one populator
publishes each event to exactly one topic. A legacy processor and a pool worker
running together therefore **partition** the stream instead of duplicating it,
which the 350 + 307 = 657 split proves exactly. The runbook does not need to
stop legacy processors before starting workers in order to avoid duplicates.
What it does need to stop them for is the drainer, which is where M4's 106
duplicates came from.

**Two further findings.**

1. **Roll-forward recovers the rollback's loss, but out of order.** Cumulative
   gaps across M2+M4+M5 are zero: the delivery group's committed offset survived
   the `kill -9`, so the restarted worker replayed all 166 uncommitted records
   from offset 1238. M4's loss is "stranded while the rollback stands", becoming
   permanent only if the delivery topic is deleted or the group is reset. The
   cost is ordering: all 3190 inversions land on the three keys that had
   operations on both sides of the rollback, because the stranded slice of their
   history was replayed after the legacy path had already delivered later
   operations on the same key. A consumer tracking per-key object state ends on
   a stale value.
2. **The wedge fired a second time on worker start** (18 assigns, 17 revokes in
   35 s, nothing delivered). Two of three worker starts in this experiment
   wedged. The cure-by-restart also cost 40 s of dead time waiting for the
   wedged member's group session to expire.

---

## M6 - downgrade tolerance, populator

**Start state:** rolled back to the legacy path for this scenario (worker
stopped, populator on the legacy config, legacy processor running),
`customer-topic-1` at 2720. Bucket notification configuration lives at
`metadata.__metastore`, `_id: <bucket>`, field `value.notificationConfiguration`.
Evidence: `evidence/M6/`.

**(a) Unknown account-scoped ARN next to the legacy one.** CloudServer
**rejects** the PUT atomically, HTTP 400 `InvalidArgument`, naming only the
offending ARN: `lib/api/apiUtils/bucket/getNotificationConfiguration.js` takes
`queueArn.split(':')[5]` and requires it to be in the static destination list.
The ARN parses fine in arsenal; it is CloudServer's destination check that
fails. Written straight into MongoDB instead, `GetBucketNotificationConfiguration`
returns it **verbatim**, so today's API serves a configuration it would refuse
to accept. Then 10 PUTs: 10 events delivered to `poc-dest-1`, all carrying
`configurationId: legacy-global`, zero populator errors. The populator skips the
unknown ARN silently, which is the good news and the bad news: nothing is logged
and no metric moves, so a rule pointing at no destination looks identical to a
rule that was never written.

**(b) Name collision - reproduced, headline finding.** A configuration whose
only rule is `arn:scality:bucketnotif::123456789012:poc-dest-1` is **accepted by
CloudServer with HTTP 200** (no MongoDB write needed), read back verbatim, and
all 12 of its events were delivered to the **global** `poc-dest-1`, carrying
`configurationId: acct-scoped-collision`. Both the populator and the legacy
processor match destinations with `queueArn.split(':').pop()`, which discards
the account field by construction. Any tenant who can PUT a bucket notification
configuration can do this today, with no new code deployed, and the global
destination's consumer receives events for a configuration it never set up.

**(c) Shape hazard - worse than expected.** Two distinct failures from one
`queueConfig` entry with no `queueArn`:

1. **The bucket is bricked at the S3 API and cannot be repaired through the
   API.** `BucketInfo`'s constructor calls
   `NotificationConfiguration.validateConfig()` at deserialize time, which
   asserts `typeof queueArn === 'string'`, so every request that reads the
   bucket's attributes returns 500: `PutObject`, `GetObject`, and
   `PutBucketNotificationConfiguration` itself. The only repair route is a
   direct metadata write. For the downgrade question, `validateConfig` asserts
   only that `events`, `queueArn` and `id` are strings and that `filterRules`
   names and values are strings, ignoring unknown fields, so a future version
   that *adds* fields downgrades safely, while one that makes `queueArn`
   optional bricks every bucket using it.
2. **The populator crash-loops and stalls every tenant.**
   `Cannot read properties of undefined (reading 'split')` in
   `_processObjectEntry`, propagated through `LogReader._processFilterEntries`
   and `QueuePopulator._processLogEntries` to `bin/queuePopulator.js:42`
   `process.exit(1)`. Two restarts, two exits, ZooKeeper log offset unchanged
   both times, so the poison entry stays at the head of the unprocessed oplog
   forever. A healthy tenant's canary event in a different bucket was not
   delivered for the whole window. After repairing the configuration in MongoDB
   the populator came up and delivered all 5 backlogged events including both
   canaries, so the stall loses nothing: it is a full stop, unbounded in time,
   clearable only by a metadata write.

| measurement | value |
|---|---|
| (a) PUT with unknown account-scoped ARN | HTTP 400, whole request refused |
| (a) same config via MongoDB, then GET | returned verbatim |
| (a) PUTs / delivered / populator errors | 10 / 10 / 0 |
| (b) PUT with colliding account-scoped ARN | **HTTP 200 accepted** |
| (b) operations / delivered to the global destination | 12 / 12 |
| (c) S3 API on the poisoned bucket | 500 on PutObject, GetObject, PutBucketNotificationConfiguration |
| (c) populator restarts attempted / survived | 2 / 0 |
| (c) ZooKeeper offset movement during the crash loop | none |
| (c) healthy-tenant events delivered during the stall | 0 |
| (c) events lost after repair | 0 |
| checker over the (a)+(b) window | 22 expected, 22 delivered, 0 gaps, 0 dups, 0 inversions |

**Verdict: the design's "sharp edge" verify item comes out worse than written.**
The populator does skip an unknown ARN silently (a), but a shape it cannot parse
takes down the whole pipeline for every tenant and cannot be undone through the
API (c), and the account-field collision misroutes a tenant's events to a
different tenant's destination through the supported API path (b).

---

## M7 - downgrade tolerance, processor

**Start state:** populator on the pool config (so the delivery topic carries
addressed records with `destinationId` and `configurationId`), no worker
running, `customer-topic-1` at 2748. Evidence: `evidence/M7/`.

**Procedure:** `conf/backbeat-notification-m7-legacy-on-delivery.json`, a copy
of the legacy config with `extensions.notification.topic` set to
`bucket-notification-delivery`. Deviation: its `queueProcessor.groupId` was
also changed to `m7-legacy-on-delivery`, so the real legacy group would not end
up holding offsets for two topics and stay usable in later scenarios. Then a
legacy `queueProcessor poc-dest-1` on that config, and 24 driver operations.

| measurement | value |
|---|---|
| addressed records produced | 24 |
| delivered to `customer-topic-1` | 24 |
| gaps / duplicates / inversions | 0 / 0 / 0 |
| processor errors / crashes | 0 / 0 |

**Verdict: PASS.** The legacy processor consumes addressed records normally and
never crashes. `messageUtil.transformToSpec` builds the customer event from
named fields only, so `destinationId` is ignored and `configurationId` is
overwritten from the bucket configuration the processor re-reads. The delivered
event is byte-shape identical to the M1 golden event, so rolling the image back
and repointing the legacy processors at the delivery topic is a viable downgrade
route with no schema work needed.

It is not, however, equivalent behaviour. The processor ignores `destinationId`
and re-matches the bucket configuration against its **own** destination id, so
on a delivery topic carrying several destinations a record addressed to
`poc-dest-2` is still delivered to `poc-dest-1` whenever the bucket also has a
matching `poc-dest-1` rule: records get fanned out rather than routed. It also
re-matches at delivery time, so a detached destination's queued events are
dropped (see M9). A downgrade of this shape changes what customers receive.

---

## M8 - overlapping rules (FR10 evidence)

**Start state:** end of M7. Adding `poc-dest-2` -> `customer-topic-2` required
stopping every backbeat process, editing **two** static destination lists
(CloudServer's `bucketNotificationDestinations` and
`extensions.notification.destinations` in each backbeat config), creating the
kafka topic, **restarting CloudServer** (the list is read at startup and is what
`PutBucketNotificationConfiguration` validates against), and running one legacy
`queueProcessor` **per destination**. Evidence: `evidence/M8/`.

Bucket `poc-bucket-overlap`: rule `all-to-dest1` = `s3:ObjectCreated:*`, no
filter, to `poc-dest-1`; rule `logs-to-dest2` = `s3:ObjectCreated:*`, prefix
`logs/`, to `poc-dest-2`. Neither destination has an `internalTopic` override.

| object | path | `poc-dest-1` | `poc-dest-2` |
|---|---|---|---|
| `logs/x` | legacy | delivered (`all-to-dest1`) | **delivered** (`logs-to-dest2`) |
| `other/y` | legacy | delivered | not delivered |
| `logs/x2` | pool | delivered (`all-to-dest1`) | **delivered** (`logs-to-dest2`) |
| `other/y2` | pool | delivered | not delivered |

**Verdict: both paths fan out, neither implements FR10 as written.** The
migration preserves today's customer-visible behaviour exactly. FR10 as drafted
(first matching rule wins, deliver only to `poc-dest-1`) is what *neither*
implementation does, so implementing it literally would be a silent behaviour
change: a bucket with a catch-all rule plus a prefix rule would stop delivering
to its second destination.

The paths differ in **where** the fan-out is decided, which matters
operationally. Legacy publishes **one** internal-topic record per object (the
`pushedToTopic` map in `_publishLegacyEntries` skips destinations sharing an
internal topic) and each processor re-matches at delivery time. The pool
publishes **one addressed record per matching destination** at publish time, so
detaching a destination is not retroactive (see M9). The two destinations landed
on different delivery partitions, `poc-dest-2` on p0 and `poc-dest-1` on p2,
which is the one-partition-per-destination consequence of `buildDeliveryKey`
from M2 seen from the other side.

**Three side findings.**

1. **Filter rule names are case sensitive at the API but not in the engine.**
   `"Name": "prefix"` is refused with `MalformedXML: filter Name must be one of
   Prefix or Suffix`, while `validateEntryWithFilter` compares
   case-insensitively.
2. **Wedge #3, this time on the legacy processor.** The `poc-dest-2` queue
   processor cycled 42 assigns / 41 revokes / 41 idle un-assigns while the
   `poc-dest-1` processor, subscribed to the same internal topic in a different
   group, sat at 2 assigns / 1 revoke. The wedge is a `BackbeatConsumer`
   condition, not a delivery-worker one, and it hit one of two sibling
   consumers. Restart cured it.
3. **The M7 downgrade route duplicates on roll-forward.** Starting the pool
   worker for M8(b) re-delivered all 24 M7 records (verified against the M7
   driver log, 0 gaps, 24 matches), because the M7 legacy processor had consumed
   them under its own consumer group and left the pool group's offsets where
   they were. Repointing legacy processors at the delivery topic is safe going
   down and duplicates everything coming back up, unless the pool group is
   advanced to the head first.

---

## M9 - detach with backlog (D15 evidence)

**Start state:** bucket `poc-bucket-detach` with two unfiltered
`s3:ObjectCreated:*` rules, one to `poc-dest-1` (the control, stays attached)
and one to `poc-dest-2`. Detaching means re-PUTting the configuration without
the second rule. Evidence: `evidence/M9/`.

| | legacy path | delivery-pool path |
|---|---|---|
| PUTs with the consumer stopped | 20 | 20 |
| backlog created | 20 records, group lag 6/5/4/5 | 40 addressed records (20 per destination) |
| after detach, consumer restarted | lag 20 -> 0 | lag -> 0 |
| **delivered to the detached destination** | **0 of 20** | **20 of 20** |
| delivered to the control destination | 20 | 20 |
| checker | - | 20 expected, 20 delivered, 0 gaps, 0 dups, 0 inversions |

**Verdict: both expectations confirmed.** The legacy processor consumed all 20
backlogged records, delivered none and committed: `processKafkaEntry` re-reads
the bucket configuration, filters it by its own destination id, finds nothing
and returns `done()`. Nothing above `debug` is logged and no metric moves, so
the drop is invisible. The pool worker delivered all 20 because the destination
was resolved at publish time and written into the record, so it never re-reads
the configuration.

This is a real customer-visible change in the migration, in both directions.
Today removing a destination silently suppresses its queued events, which reads
like a revocation; after the migration those events still arrive. It belongs in
the release notes, and the runbook should say that draining before a detach is
the only way to get deterministic behaviour on the legacy side.

**Method note:** before the legacy half, the `poc-dest-2` consumer group was
warmed with 40 PUTs so that it held committed offsets on all four internal
partitions. Without that, `auto.offset.reset: latest` on an uncommitted
partition would have skipped the backlog for an unrelated reason.

---

## M11 - populator kill -9 mid-stream

**Start state:** pool path, healthy worker at lag 0, `customer-topic-1` at
2880, driver on `poc-bucket` at 5 ops/s with two straddle keys.
Evidence: `evidence/M11/`.

| | attempt 1 (kill at wall-clock 30 s) | attempt 2 (kill forced mid-batch) |
|---|---|---|
| driver operations | 328 | 330 |
| delivered | 328 | 330 |
| **gaps** | **0** | **0** |
| **duplicate extras** | **0** | **0** |
| **per-key inversions** | **0** | **0** |
| entries published by the killed / new process | 109 / 219 | 316 / 14 |

Attempt 2 killed the process as soon as a `publishing addressed message` line
appeared with no `batch completed` after it, so it died 266 entries into an
in-flight batch. Still zero duplicates.

**Verdict: PASS, and the duplicate bound is tighter than the criterion
assumes.** `lib/queuePopulator/LogReader.js` accumulates a batch's entries in
memory while it reads and filters the log, produces all of them to Kafka in one
`send`, and only then writes the ZooKeeper offset. So there are two kill
windows: during read/filter (nearly the whole batch duration) nothing has been
produced and the restart simply redoes the work, giving zero duplicates and zero
gaps; between the Kafka acknowledgement and the ZooKeeper write the records are
on the topic with the offset unsaved, and the restart republishes the **entire
batch**, up to `batchMaxRead` (10000 here). Low probability, high amplitude. Two
`kill -9`s never hit it, which is the useful operational number: the populator is
safe to kill for practically all of its duty cycle.

Per-key order held across 658 operations and two kills, because a destination's
delivery key is a constant, so every record for `poc-dest-1` goes to one
partition and one worker in publish order.

---

## M12 - worker kill -9 mid-delivery

**Start state:** pool path, healthy worker at lag 0, `customer-topic-1` at 3538,
`deliveryPool.concurrency` 1000, `maxQueued` 1000. Driver at 5 ops/s for 90 s.
`kill -9` the worker at t+48 s with real lag on the delivery topic, restart 2 s
later. Evidence: `evidence/M12/`.

At the kill: delivery p2 head 2647, group committed 2522, so a 125-record
uncommitted window.

| measurement | value |
|---|---|
| driver operations | 489 |
| delivered | 578 |
| unique delivered | 489 |
| **gaps** | **0** |
| **duplicate extras** | **89** |
| **per-key inversions** | **0** |
| uncommitted window at the kill | 125 records |
| of which already delivered (the duplicates) | 89 |
| of which not yet delivered | 36 |

The arithmetic closes exactly: 125 = 89 redelivered + 36 first-delivered, and
unique deliveries equal the driver's count with zero gaps.

**Verdict: PASS.** Commit-on-terminal-resolution holds, redelivery is confined
to the uncommitted window, and per-key order survives because a destination's
delivery key is constant, so the whole destination is one partition read by one
worker in publish order.

**Duplicate count against the configured concurrency: 89 against 1000.** The
window is not bounded by concurrency. `lib/BackbeatConsumer.js:210` says it
outright: offsets are auto-committed in the background every 5 seconds
(`auto.commit.interval.ms`). So the exposure is roughly the delivery rate times
the commit interval plus in-flight sends, and 89 records is about five to six
seconds of this rig's rate. Raising `concurrency` raises throughput and
therefore duplicates linearly, while the knob that actually bounds them is not
exposed anywhere in the `deliveryPool` schema, so an operator who needs a
tighter bound cannot ask for one.

---

## M10 - destination down

**Start state:** the rig on the legacy path, then on the pool path for the
second half. Evidence: `evidence/M10/`.

Three dead destinations were built. `poc-dest-3` at `localhost:9999`
(connection refused, `nc` returns in 20 ms). `poc-dest-4` at
`10.255.255.1:9092` (blackhole, `nc -w 8` hung for 75 s). `poc-dest-6` at
`localhost:9092` with topic `customer-topic-6`, a **reachable broker with an
unwritable target**, created with `kafka-topics --replica-assignment 99` so the
partition has no leader; this one had to be invented because of what the first
two did. A fourth attempt using `min.insync.replicas=2` on an RF=1 topic was
abandoned: this broker accepted the `acks=all` produce anyway.

### Legacy half

**The per-destination processor cannot start when its destination is
unreachable.** Both `poc-dest-3` and `poc-dest-4` processors exited within
seconds: `QueueProcessor.start` -> `error setting up kafka notif destination` ->
`Client is disconnected` -> SIGTERM. `KafkaNotificationDestination._setupProducer`
waits for the producer's `ready` and treats the first `error` as fatal. Under a
supervisor that is a crash loop that consumes nothing, so the "does the offset
advance" question is unanswerable for those two: the consumer group is never
created.

`poc-dest-6`'s processor did start, and 20 PUTs were watched for 6.5 minutes:

| measurement | value |
|---|---|
| records published | 20 |
| records ever attempted | **10** (= `queueProcessor.concurrency`) |
| records delivered | 0 |
| time to first error | **~300 s** (librdkafka `message.timeout.ms` default, never overridden) |
| group offset advance | **none, on any partition, ever** |
| dead-letter records | 0 |
| metric for the failure | **none** |
| process exited | no |

At t+309 s the ten in-flight sends failed with `message timed out`, and the
commit attempt failed with `Local: Group partition assignment lost` (-142): the
five-minute stall had blown `max.poll.interval.ms` and the member was already
evicted, which is the BB-787 self-eviction chain. The other ten records were
never attempted at all.

**There is no dead-letter path.** `monitorNotificationFailures` and
`notificationFailedTopic` appear in exactly two places in the repository, the
joi schema and one unit test. No production code reads either, and
`backbeat-bucket-notification-failed` stayed at 0.

### Pool half

The worker **came up healthy with four unreachable destinations configured**
(1 assign, 0 revokes) and created producers lazily per endpoint.

| destination | records | dropped | `reason` | time to drop |
|---|---|---|---|---|
| `poc-dest-3` (refused) | 20 | **20** | `producer_error` | immediate, all 20 within 1 ms |
| `poc-dest-4` (blackhole) | 20 | **20** | `producer_error` | immediate, all 20 within 3 ms |
| `poc-dest-6` (leaderless) | 20 | **20** | `delivery_error` | **~30 s = `deliveryTimeoutMs`** |

Delivery group lag went to 0 on every partition, so the pool commits past a drop
instead of accumulating. Isolation held: during the drops, 5 PUTs to the healthy
`poc-dest-1` delivered 5 of 5.

### Side by side

| | legacy processor | pool worker |
|---|---|---|
| destination down at process start | **process exits**, crash loop, consumes nothing | starts normally |
| records attempted | 10 of 20 (the concurrency), then no more | all 20 |
| time to give up | ~300 s, not settable from the notification config | ~30 s (`deliveryTimeoutMs`), ~0 for a connect failure |
| consumer offset | **never advances** | advances to head |
| counter | **none** | `dropped_total{target,reason}`, exact |
| side effect | blows `max.poll.interval.ms`, group assignment lost, process still up | none observed |

**Verdict: the "today loses data silently" line is true but understated.** Today
a dead destination does not drop with the offsets advancing; it **stalls that
destination indefinitely with no counter at all**, and if the destination is
down when the process starts, the process cannot start. The pool turns the same
failure into a bounded, counted, per-destination drop visible on `/metrics`
within 30 seconds. Neither side has a dead-letter path.

**Defect found: `delivery_timeout` is unreachable.**
`DeliveryWorker.processKafkaEntry` picks the reason with
`sendErr.code === CODES.ERRORS.ERR__MSG_TIMED_OUT`, and that constant is -192,
but the error reaching the callback from `BackbeatProducer._onDeliveryReport`
carries `code: -1` (confirmed independently in the legacy processor's log for
the same failure). So the branch never matches, `delivery_timeout` is dead code,
and a real `message.timeout.ms` expiry is counted as `delivery_error`.
`dropped_total{reason}` therefore cannot distinguish "the destination was too
slow and we gave up" from "the destination rejected the record", which is the
distinction an operator most wants from that counter.

---

# Chaos and rebalance round

Scenarios **C1 to C4**, run 2026-09-10 on the same rig, on the **delivery-pool
path** with **three healthy destinations** and three buckets, one per
destination. Evidence per scenario in `evidence/C1` .. `evidence/C4`. The
reshard scenario of the brief is the other agent's **C5r** (its own section
below, evidence in `evidence/C5/`); a reshard run this agent completed on the
live pipeline before that scope change arrived is reported after C4, with its
evidence in `evidence/C5-cli-pipeline/`.

**Setup common to every scenario.** `poc-dest-1`, `poc-dest-2` and `poc-dest-3`
on `localhost:9092` to `customer-topic-1/2/3` (`conf/backbeat-chaos-base.json`),
each fed by its own bucket `chaos-bucket-1/2/3`. `poc-dest-3`'s endpoint was
moved off the dead `localhost:9999` it carried for M10; nothing else about the
destination list changed, so CloudServer needed no restart. The three
destinations land on **three distinct partitions** of a P=3 delivery topic:
`buildDeliveryKey` returns the bare resource name at `spreadFactor` 1, and the
partition is unsigned crc32 of that name modulo P, so

| destination | crc32 | partition of 3 | partition of 6 |
|---|---|---|---|
| `poc-dest-1` | 4280712389 | **2** | 5 |
| `poc-dest-2` | 1714367871 | **0** | 3 |
| `poc-dest-3` | 287841769 | **1** | 1 |

which reproduces M2's observed p2 for `poc-dest-1` and M8's p0 for
`poc-dest-2`. One destination is one partition is one worker, so the three
destinations are also three independent lanes through the pool.

Each scenario gets a **fresh delivery topic**, created and confirmed to have a
leader on every partition three consecutive times before any consumer object is
built, which is the design/06 mitigation. The driver
(`scripts/driver.js`, extended with `--buckets` for this round) runs at 5 ops/s
round-robin over the three buckets with the sequence in the object size, and
every fifth operation is a PUT-then-DELETE on one of three fixed keys.
Checking is `scripts/chaos-check.py` over the existing `scripts/check.py`, once
per destination with the driver log filtered to that destination's bucket.

**Consumer configuration, as the code and librdkafka 2.3.0 actually set it.**
Every pause below has to be read against these.

| knob | value | where from |
|---|---|---|
| `session.timeout.ms` | **45000** | librdkafka default; `BackbeatConsumer` never sets it |
| `heartbeat.interval.ms` | 3000 | librdkafka default |
| `max.poll.interval.ms` | **300000** | `lib/BackbeatConsumer.js:93` joi default, not overridden by the rig config |
| `auto.commit.interval.ms` | 5000 | librdkafka default, noted at `lib/BackbeatConsumer.js:210-213` |
| `partition.assignment.strategy` | **`range,roundrobin`** | librdkafka default, so **eager**: every membership change revokes every partition from every member |
| producer `topic.metadata.refresh.interval.ms` | **300000** | librdkafka default. The rig's shim patches `KafkaConsumer` only, so the populator's producer keeps it |

**One new pre-existing defect dominates every number in this round, so it is
stated before the scenarios.**

## The headline: an ordinary rebalance kills the process

`lib/BackbeatConsumer.js:851` calls `this._consumer.offsetsStore(...)` guarded
only by `!this.isPaused()`. When an entry finishes while the consumer is between
assignments, librdkafka raises `Local: Erroneous state` (code **-172**), and
**nothing catches it**: node-rdkafka rethrows at `lib/client.js:482`, there is no
`uncaughtException` handler in `deliveryWorker/task.js`, and the process exits
with rc=1.

```
LibrdKafkaError: Local: Erroneous state
    at KafkaConsumer.offsetsStore (node-rdkafka/lib/kafka-consumer.js:627:15)
    at BackbeatConsumer.onEntryCommittable (lib/BackbeatConsumer.js:851:28)
    at BackbeatConsumer._onEntryProcessingDone (lib/BackbeatConsumer.js:581:18)
  code: -172
```

`design/07-preexisting-findings.md` items 3 and 5 record this throw and its
data cost (delivering while never committing). What this round adds is that in a
**standalone worker process it is fatal**, and that in a three-member consumer
group it fires on ordinary events: a member joining, a member leaving, a member
being killed. The first attempt at C1 was abandoned because of it: `kill -9` on
worker 2 was followed, 52 s later, by worker 1 dying on its own with this throw,
so one deliberate kill removed two of three workers.

Every scenario from that point ran each worker under
`scripts/worker-supervisor.sh`, which restarts it and timestamps every exit into
`evidence/<Cx>/worker<n>.crashes`. That mirrors S3C, where systemd restarts
backbeat, and it means **process exits are counted rather than hidden**. It is
not a code change: nothing in any repository was modified.

The second thing worth knowing before the numbers: **SIGTERM does not reliably
stop a delivery worker.** In C1 phase (c) the worker logged
`received SIGTERM, exiting` at 14:36:18.856, then
`Timed out LeaveGroupRequest in flight`, and was **still alive four minutes
later**, still answering `/_/live` with 200. `BackbeatConsumer.close()` waits for
a revoke callback with no deadline of its own (design/06 records the same at
`:1122`). Evidence: `evidence/C1/sigterm-hang.txt`.

## C1 - one consumer dies in a three-worker group

Three workers in one consumer group on a P=3 topic, one partition and one
destination each. Driver at 5 ops/s over the three buckets for 700 s.
(a) `kill -9` worker 2, held dead 180 s. (b) release the hold so it rejoins.
(c) SIGTERM worker 2, held dead 180 s, released. Evidence: `evidence/C1/`.

| measurement | value |
|---|---|
| driver operations delivered as events | 3724 (1064 + 1064 + 1596) |
| **gaps** | **0** on all three destinations |
| **duplicate extras** | **122** = 99 (`poc-dest-1`) + 8 (`poc-dest-2`) + 15 (`poc-dest-3`) |
| **per-key inversions** | **52**, all on `poc-dest-3` |
| unexpected events | 0 |
| process exits | **5**: 4 uncaught `Local: Erroneous state` (rc=1) plus the 1 deliberate `kill -9` (rc=137) |
| rebalances (assign / revoke per worker) | worker1 8/7, worker2 2/1, worker3 3/2 |
| RSS range per worker | 71.0 to 163.1 MB (worker1 83.6-163.1, worker2 71.0-162.8, worker3 79.9-162.7) |
| worker2's partitions after the phase-b rejoin | **none, for the remaining 6 minutes of the run** |

Pauses, measured as customer-topic record timestamps, so the first moment a
customer could see the event:

| event | `poc-dest-1` | `poc-dest-2` (the killed worker's own destination) | `poc-dest-3` |
|---|---|---|---|
| first delivery after the `kill -9` | 45.3 s | **136.0 s** | 0.2 s |
| first delivery after releasing the hold (the rejoin) | **164.4 s** | **163.8 s** | 1.7 s |
| first delivery after the SIGTERM | 52.0 s | 51.4 s | 1.4 s |
| longest pause anywhere in the run | 165.3 s | 165.2 s | 95.8 s |
| deliveries that waited more than 10 s | 698 of 1163 | 590 of 1072 | 1348 of 1611 |
| end-to-end latency p50 / p99 | 11.6 s / 164.8 s | 10.3 s / 160.2 s | 119.2 s / 173.4 s |

**Verdict: 0 gaps, and neither of the other two promises holds.**

1. **The pause is not the session timeout.** `session.timeout.ms` is 45 s and
   that is visible: `poc-dest-1` resumed 45.3 s after the kill. But the
   destination the dead worker actually owned waited **136 s**, because the
   rebalance the kill triggered threw the commit path in both survivors, both
   exited, and the group only reached a stable assignment at 14:33:38, 134 s
   after the kill. So the operational pause is the session timeout **plus a
   full crash-and-restart cycle of the surviving members**.
2. **Adding a member back is worse than losing one.** The longest pause of the
   whole run, 165 s, started the second worker 2 rejoined, and it hit
   `poc-dest-1` and `poc-dest-2` equally. Eager assignment revokes every
   partition from every member, the revoke crashes a member, the crash triggers
   another rebalance, and the group did not settle until **163 s** later. Worse,
   worker 2 then held **no partition at all**: the three partitions were split
   two and one between workers 1 and 3 for the rest of the run.
3. **Per-key ordering breaks.** 52 inversions, all on the three straddle keys,
   with a mechanism that is visible in the record timestamps:

   ```
   customer-topic-3 offset 375  14:30:10.785  c1-b2-strad-0  Put    size=1090
   customer-topic-3 offset 380  14:30:12.793  c1-b2-strad-0  Delete size=1090
   customer-topic-3 offset 388  14:31:00.338  c1-b2-strad-0  Put    size=1018
   customer-topic-3 offset 393  14:31:02.342  c1-b2-strad-0  Delete size=1018
   customer-topic-3 offset 433  14:31:16.348  c1-b2-strad-0  Put    size=1090
   ```

   Operation 1090 was delivered **first**, during the assign/revoke churn of the
   initial join, and operations 1018 to 1072 were delivered 50 s later by the
   member that took the partition when the churn settled. A customer tracking
   that object saw its state jump forward and then go backward by four
   operations. This is the hazard `BackbeatConsumer` documents at `:425-428`
   (consume responses arriving out of order) combined with a revoke abandoning a
   partially delivered batch, and it is the same signature as M2b's single
   unexplained inversion, now reproduced 52 times with timestamps.

   Note the measurement's own limit: with `--straddle-every 6` over three
   buckets every straddle operation landed in bucket 3, so only `poc-dest-3`
   had keys with more than one operation and only it **could** show an
   inversion. The remaining scenarios use `--straddle-every 5`, which rotates
   the straddle keys across all three buckets.

**The cascade, with timestamps** (`evidence/C1/exits-per-rebalance.json`, from
the supervisor's own exit records correlated with each worker's rebalance log):

| time | worker | exit | its own last rebalance | followed |
|---|---|---|---|---|
| 14:30:14 | 2 | **crash** rc=1 | revoke, 2.2 s earlier | worker3 starting, 0.4 s earlier |
| 14:31:24 | 2 | kill rc=137 | assign, 24.9 s earlier | the deliberate `kill -9` |
| 14:32:06 | 3 | **crash** rc=1 | revoke, 22.0 s earlier | that kill, 42.2 s earlier |
| 14:32:13 | 1 | **crash** rc=1 | revoke, 2.1 s earlier | that kill, 49.2 s earlier |
| 14:36:25 | 1 | **crash** rc=1 | revoke, 115.1 s earlier | the SIGTERM, 6.3 s earlier |

One deliberate kill, **both survivors dead 42 s and 49 s later**, each on its
own revoke path. Four crashes to one kill. The three rc=137 exits at 14:48:18
are this agent's own cleanup after the run.

## C5r - workgroup reshard under a stalled member

A workgroups reshard from hashmod modulo 2 to modulo 3, driven by a real
`WorkgroupCutover` against the ft rig, while one generation 1 worker is
SIGSTOPped starting on the completion of the step that produces the barriers
and before it has committed past them. Six destinations, one per class of the
remap, so two keep their owner and four move. P=4 delivery topic, 420 records:
12 priming, a 168 record backlog burst, then 240 at 5 records/s. Each workgroup
runs as its own OS process, which is what makes a single member freezable;
everything else is the path the functional suite takes. Generation 1 is held
slow (pool concurrency 2, destination poll interval 500 ms), the arrangement
design/11's E2 and E4 use, so it is genuinely short of its barriers when the
cutover fires.

Driver: `scripts/c5-reshard-chaos.js` and `scripts/c5-worker.js`, standalone,
no change to `tests/functional/deliverypool/workgroups.js`. Every topic, group
and znode is scoped to a per-run id. Evidence:
`evidence/C5/notes-reshard-under-stalled-member.md` and the four run
directories beside it. Labelled C5r because another scenario of this round
carries the C5 number.

| Observation | control | stall 32 s | stall 313 s | operator error |
|---|---|---|---|---|
| records produced | 420 | 420 | 420 | 420 |
| records below a barrier | 210 | 215 | 230 | 230 |
| cutover wall time | 32.1 s | 25.0 s | 34.4 s | 19.1 s |
| `verify` exit 0 after the cutover returned | 27 ms | 33.6 s | 328.7 s | never |
| `verify` exit 0 after SIGCONT | n/a | 19.6 s | 40.6 s | n/a |
| **gaps** | **0** | **0** | **0** | **43** |
| gaps at or above a barrier | 0 | 0 | 0 | 0 |
| **duplicates** | **122** | **132** | **202** | **115** |
| duplicates below a barrier | 0 | 0 | 12 | 0 |
| duplicates the old offsets predicted | 118 | 129 | 190 | 77 |
| delivered more than twice | 0 | 0 | 0 | 25 |
| **series needing a third ordered run** | **0** | **0** | **0** | **2** |

Duplicates against what each generation 1 workgroup owned above its barriers.
`c5a` is the member that gets frozen:

| run | c5a owned | c5a duplicated | c5b owned | c5b duplicated |
|---|---|---|---|---|
| control | 105 | 58 (55%) | 105 | 64 (61%) |
| stall 32 s | 102 | 29 (28%) | 103 | **103 (100%)** |
| stall 313 s | 95 | **95 (100%)** | 95 | **95 (100%)** |

**A stalled member does not delay the duplicate window, it saturates it, and
the saturation lands on the healthy members.** The drain gate is the AND over
every previous group, so one frozen member keeps every healthy member running
past its barriers for as long as the freeze lasts. At two workgroups the two
effects nearly cancel and the total moves only 122 to 132, but the shape is
what matters: with W workgroups and one member frozen, W-1 slices go to 100%
duplication. The cost of a stall therefore grows with the width of the
generation and is close to invisible at W=2.

**Once the healthy members reach the head, a longer stall stops costing
duplicates above the barrier and starts costing them below it.** The healthy
group's overshoot sat at 194, the head of the topic, for 60 consecutive
one-second polls of the 313 s stall, with above-barrier duplication already at
95 of 95 for both slices. The long stall's extra cost was 12 records the
evicted member re-delivered from *below* its barriers, which the drain report
cannot see: below a barrier they count in `remaining`, never in `overshoot`.

**The 32 s stall crossed neither threshold; the 313 s stall crossed both, and
the eviction is the expensive half.** At 32 s the client's last activity is
461 ms before the SIGSTOP and its next is a heartbeat 9 ms after the SIGCONT on
the same generation id, assignment of 4 partitions intact, no rebalance. At
313 s, within 13 ms of the SIGCONT: `SESSTMOUT` after 315020 ms without a
coordinator response, `LOST` on all 4 partitions, `MAXPOLL` "Application
maximum poll interval (300000ms) exceeded by 13017ms", `LEAVEGROUP` and then
`OffsetCommit` both answered `Broker: Unknown member`, then a rejoin from
scratch. The offsets stored while frozen (partition 1 at 90 against 77
committed) were refused, so those records were delivered twice. `verify`
reaching exit 0 then took 40.6 s against 19.6 s for the short stall, on
comparable remaining work.

`max.poll.interval.ms` cannot be configured for a delivery worker.
`BackbeatConsumer` accepts `kafka.maxPollIntervalMs` (BackbeatConsumer.js:93)
but `DeliveryWorker.start` passes only `hosts`, `site`, `compressionType` and
`requiredAcks` (DeliveryWorker.js:246-252), so the pool always runs at the
300 s default.

**The drain report names the stuck member exactly, and `remaining` is not a
loss count.** Through all three stalls `remaining` was entirely on the frozen
member's group and exactly 0 on the healthy one, unchanged poll after poll
(140 for 5 polls, 119 for 60, 92 for 2). Stopping generation 1 at that first
exit 2 lost 43 records against a `remaining` of 92: the column counts offsets
the group has not committed, and a group consumes every record while owning
only its slice, so the records at risk are roughly `remaining/W`. Over-warning,
in the safe direction, by a factor rather than by design/11's 5 records. All 43
were below a barrier, all 43 were records the report had already counted, and
none was lost above one.

**The `G * (duplicates + P)` overshoot identity E1 asserts holds only when
every old group has finished the topic.** Printed overshoot against the
prediction: 148 vs 252 at the exit 0 poll and 247 vs 252 at the frozen offsets
in the control, 356 vs 412 under the long stall. Under a stall the old groups
are at different points by construction. design/11's conclusion about the
column stands; the exact formula is narrower than stated.

**No cross generation inversion under any stall.** 12 per key series per run;
in the control and both stalls every series decomposed into at most two
non-decreasing runs, with the one decreasing step being the handover. Waiting
for exit 0 before starting generation 2 is what buys it, and a stall only
lengthens that wait.

**New: restarting a wedged workgroup can duplicate everything it delivered,
because the throw that kills it is on the offset store path.** design/09 states
the wedge recovery as "a fresh worker on the same group resumes from that
group's committed offset, so a workgroup that had already delivered does not
deliver anything twice". In the operator error run `gen2-c5b` looped
assign/revoke with "processing queue idle, un-assigning" every one to two
seconds for 2.7 minutes, delivering nothing. Then 29 ms after the SIGTERM the
restart sent it, it got an assignment that held, consumed its four barriers,
opened its producers, and delivered all 63 records of its slice in one burst
3579 ms after that SIGTERM, before exiting on the commit path throw
(`offsetsStore`, BackbeatConsumer.js:851) with nothing committed. The
replacement re-delivered the same 63. That accounts for the whole of that run's
38 unpredicted duplicates, 25 identities delivered more than twice, and two
series needing a third ordered run.

**New, and not about resharding: a join whose effective subscription flaps to
zero topics waits out `session.timeout.ms`.** Three of four generation 1 joins
showed the design/06 flap ("no topics in metadata matched subscription",
effective subscription 1 to 0 to 1) for about 4 s, ended with an empty
subscription, then went completely silent for 40981 to 40992 ms before an
outdated JoinGroup response was discarded and the join succeeded. 4 s plus 41 s
is `session.timeout.ms` (45000 ms), and no 5 s metadata refresh rescues it in
between. It is why first delivery landed at 60947 ms and 60953 ms in two
different runs, and it is a plausible reading of E4's 49169 ms in design/11.
The per-join metadata reconfirm, applied here as the suite applies it, did not
prevent it.

**The commit path throw is fatal to a real worker process.** Each workgroup here
is its own process with no uncaught filter, which is what a deployment looks
like, and both documented sites killed one: `isPaused` through
`KafkaConsumer.subscription` (BackbeatConsumer.js:850) and `offsetsStore`
(BackbeatConsumer.js:851). It is the source of the 3 to 12 duplicates the
committed offsets do not predict in every run.

E4 of design/11 (a crashed old-generation worker cannot resume) was not
re-derived here. The stalled worker always finished its own drain: at 32 s it
never lost its assignment, and at 313 s it was evicted but rejoined the same
group at the same generation and drained to exit 0, because nothing had rewritten
the znode under it beyond the cutover it was already living through.

## C2 - adding processors to the same workgroup under load

One worker owning all three partitions, driver at 5 ops/s, then worker 2,
worker 3 and worker 4 added 60 s apart. Evidence: `evidence/C2/`.

| measurement | value |
|---|---|
| driver operations delivered as events | 1797 (600 / 599 / 598) |
| **gaps** | **0** |
| **duplicate extras** | **13** (6 / 3 / 4) |
| **per-key inversions** | **0** |
| unexpected events | 0 |
| process exits | **4**, all uncaught `Local: Erroneous state` (worker1 twice, worker2 twice) |
| rebalances (assign/revoke) | worker1 3/2, worker2 3/2, worker3 2/1, worker4 1/0 |
| RSS range | 71.8 to 164.3 MB across the four workers |
| end-to-end latency p50 / p99 | 63.0 / 185.1 s (dest-1), 38.6 / 186.0 (dest-2), 118.7 / 294.8 (dest-3) |

Pause per destination for each addition, and the assignment each addition left:

| addition | `poc-dest-1` (p2) | `poc-dest-2` (p0) | `poc-dest-3` (p1) | assignment after it settled |
|---|---|---|---|---|
| worker 1 alone | - | - | - | worker1 = **p0, p1, p2** |
| + worker 2 | 1.3 s | 1.3 s | 1.3 s | worker1 crashed and restarted; worker2 = p0, p1 |
| + worker 3 | 14.8 s | 13.8 s | **211.0 s** | worker1 = p0, worker2 = p2, **worker3 = nothing, p1 unassigned** |
| + worker 4 | 0.8 s | 1.8 s | **146.9 s** | worker1 = p0, worker2 = p2, worker3 = nothing, worker4 = nothing |
| end of run | - | - | - | worker1 = p0, worker2 = p1, worker4 = p2, **worker3 = nothing** |
| longest pause in the run | 52.9 s | 91.3 s | **211.8 s** | |

**Verdict: PASS on the design's two promises, 0 gaps and 0 inversions, and the
scaling behaviour is worse than "a pause".** Three findings.

1. **One worker does serve every destination.** With one member the pool
   delivered all three destinations from all three partitions, 256 events in the
   first minute split 86/85/85. Scaling out is optional, which is the useful
   half of one-destination-one-partition.
2. **The fourth worker gets nothing, and so did a third worker for most of the
   run.** That part is expected: three partitions cannot feed four members. What
   is not expected is that **a partition sat unassigned for over three minutes**.
   After worker 3 joined, the group settled on worker1 = p0 and worker2 = p2 with
   p1 owned by nobody, so `poc-dest-3` was not served for **211 s** while a
   third worker sat idle next to it. The same thing recurred when worker 4
   joined: 147 s more for `poc-dest-3`, with two idle workers. Nothing in the
   group's state said so: `--describe` showed the group Stable, and the idle
   workers answered `/_/live` with 200.
3. **The cause is the same crash cascade as C1, and here every crash is
   attributable.** C2 issued **no kills at all**, yet there were **4 process
   exits, all rc=1**, and every one of them followed that worker's own
   `rdkafka.revoke` (`evidence/C2/exits-per-rebalance.json`):

   | time | worker | its own last rebalance | followed |
   |---|---|---|---|
   | 14:54:32 | 1 | revoke, 23.1 s earlier | worker2 starting, 26.2 s earlier |
   | 14:55:09 | 2 | revoke, 32.1 s earlier | `ADD worker3`, 2.2 s earlier |
   | 14:57:09 | 1 | revoke, 64.0 s earlier | worker4 starting, 55.2 s earlier |
   | 14:57:52 | 2 | revoke, 106.1 s earlier | worker4 settling, 8.1 s earlier |

   So **three worker additions cost four crashes of already-running members**.
   The pause an operator sees when scaling the pool out is not the eager
   rebalance, it is the eager rebalance plus a crash and restart of a member
   that was working.
4. **Two independent measurements agree on the unserved window.** The
   assignment sampler (`evidence/C2/imbalance.json`) shows a live worker
   holding **no partition while another held two or more** for 2 windows
   totalling **39.6 s**, and **at least one partition served by nobody** for 2
   windows totalling **213.0 s**, the larger being 14:55:24 to 14:58:43,
   **199.6 s** with p1 unowned and the assignment reading `worker1:p0,
   worker2:p2, worker3:(nothing)`. The pause analysis, computed independently
   from customer-topic record timestamps, puts `poc-dest-3`'s first delivery
   after `ADD worker3` at **211.0 s**. Two different sources, the same window.

## C3 - increasing the delivery topic's partitions under load

Three workers on a P=3 topic, driver at 5 ops/s, then
`kafka-topics --alter --partitions 6`. Half **a** does it the unsafe way, with
the populator producing throughout. Half **b** does it the recommended way:
stop the populator, wait for the delivery group to reach lag 0, alter, restart.
Each half on its own pre-created topic. Evidence: `evidence/C3/`.

The two halves differ only in the procedure, and the destination map is what
makes the effect visible: at P=6 the delivery keys move
`poc-dest-1` p2 -> **p5**, `poc-dest-2` p0 -> **p3**, and `poc-dest-3`
p1 -> **p1**. Two destinations change partition and one does not, which gives
the run its own control.

### C3a, the unsafe way

| measurement | value |
|---|---|
| driver operations delivered as events | 2635 (878 / 879 / 878) |
| **gaps** | **0** |
| **duplicate extras** | **7** (1 / 2 / 4) |
| **per-key inversions** | **6480**: 3240 on `poc-dest-1`, 3240 on `poc-dest-2`, **0 on `poc-dest-3`** |
| keys carrying them | exactly **one key per destination**, the multi-operation straddle key |
| **time from the alter until the populator's producer used a new partition** | **257.0 s** |
| consumers noticing the new partitions | within the metadata refresh, all three rebalanced to 2 partitions each |
| process exits during the growth rebalance | **3**, one per worker, all uncaught `Local: Erroneous state` |
| rebalances (assign/revoke) | worker1 4/3, worker2 3/2, worker3 2/1 |
| longest pause per destination | 88.9 s / 93.2 s / 93.2 s |
| end-to-end latency p50 / p99 | 11.8 / 168.9 s, 11.3 / 170.4 s, 11.4 / 209.5 s |

**The 257 s is the number to plan around.** The consumers see a partition
count change almost immediately, because `BackbeatConsumer` sets
`metadata.max.age.ms` to 5000 and the rig's shim adds a 2000 ms topic refresh.
The **producer** does not: `BackbeatProducer.producerConfig` sets neither, so
the populator keeps librdkafka's default `topic.metadata.refresh.interval.ms` of
**300000**. So for over four minutes after the alter, new records for
`poc-dest-1` kept going to p2 while p5 stayed empty.

**The reorder window is real, and the inversions land exactly where the theory
says.** The two destinations that changed partition took 3240 inversions each;
the one that did not took zero. The mechanism is visible in the delivered
sequence of a single key, which decomposes into two interleaved streams:

```
expected  : ... Delete:2375 Put:2390 Delete:2390 Put:2405 Delete:2405 Put:2420 ...
delivered : ... Put:2690    Put:2090 Delete:2690 Delete:2090 Put:2705 Put:2105 ...
                 ^new partition        ^old partition backlog
```

Sizes near 2690 are records the producer had already moved to the new partition;
sizes near 2090 are the backlog still sitting on the old one. Two different
workers held those two partitions and delivered the same destination
concurrently, so a consumer tracking that object saw its state oscillate
between two points about 600 operations apart. `0` gaps, `7` duplicates, and the
whole cost of the mistake is ordering, which matches M2c's shape exactly.

### C3b, the recommended way

Same topology, same load. Stop the populator (SIGINT, it finishes its batch),
wait for the delivery group to reach lag 0, alter, restart the populator.

| measurement | value |
|---|---|
| driver operations delivered as events | 2626 (876 / 876 / 874) |
| **gaps** | **0** |
| **duplicate extras** | **0** |
| **per-key inversions** | **0** |
| populator SIGINT to delivery lag 0 | **30 s** |
| **time from the populator restart until it used a new partition** | **13.3 s** |
| old partitions after the alter | p0 and p2 frozen at 102 records, both fully delivered |
| process exits | **0** |
| rebalances during this half (assign/revoke) | worker1 12/11, worker2 9/8, worker3 7/6 |
| longest pause per destination | 37.8 s / 38.0 s / 33.3 s |
| end-to-end latency p50 / p99 | 10.0 / 147.5 s, 10.0 / 150.2 s, 10.0 / 147.3 s |

**Verdict: the safe procedure gives 0 / 0 / 0, and it is cheap.** The whole
extra cost over the unsafe alter is a 30 s wait for lag 0 and a 38 s pause in
notification delivery, against 6480 per-key inversions and a four-minute window
in which two workers serve the same destination. The 13.3 s versus 257 s is the
reason the order matters: a **freshly started producer fetches metadata at
connect**, so restarting the populator after the alter skips the 300 s refresh
interval entirely.

Two things worth carrying into a runbook.

- **Gate on the delivery group's lag, not on the populator being down.** It is
  the pre-alter records still sitting on the old partitions that create the
  overlap, and lag 0 is the only statement that there are none. Read that gate
  together with M4b's warning: gate on lag 0 **and** on a delivery counter
  moving, because a wedged consumer's lag simply stops falling.
- **Growing partitions is not the way to add delivery capacity for one
  destination.** `poc-dest-3`'s key hashed to p1 under both P=3 and P=6, so it
  did not move at all, and no destination gained parallelism: one destination is
  still one partition and one worker. Partition growth only spreads
  destinations differently across workers. Capacity for a single destination
  still goes through `spreadFactor`, as M2 found.

## C4 - chaos loop, ten minutes

Three workers, driver at 5 ops/s for 600 s. Every 25 to 35 s (random) one
random worker took `kill -9` and was held dead 5 s. Every 2 minutes a random
other worker took `SIGSTOP`, for 20 s as briefed and once for 60 s (see below).
Evidence: `evidence/C4/`.

| measurement | value |
|---|---|
| deliberate kills / stalls | **14** kills, **3** stalls (two of 20 s, one of 60 s) |
| driver operations delivered as events | 3290 (1097 / 1097 / 1096) |
| **gaps** | **0** |
| **duplicate extras** | **1100** = 213 + 158 + 729, so **one delivery in four was a repeat** |
| **per-key inversions** | **0**, on all three destinations, 0 keys affected |
| unexpected events | 0 |
| process exits | **20**: 14 rc=137 (the kills) and **6 rc=1**, two per worker, all the uncaught `Local: Erroneous state` |
| rebalances (assign/revoke) | worker1 17/15, worker2 15/11, worker3 8/7, **40 assigns and 33 revokes** in 10 minutes |
| wedge windows (own partitions lagging, liveness 200, delivered flat 60 s+) | **6**: 145 s, 82 s, 96 s, 145 s, 69 s, 347 s |
| RSS range | 69.8 to 164.8 MB |
| longest pause | `poc-dest-1` 49.5 s, `poc-dest-3` 300.4 s, **`poc-dest-2` 876.7 s** |
| end-to-end latency p50 / p99 | 165 / 374 s, **810 / 1151 s**, 420 / 821 s |
| final consumer group state | **Stable, 3 members**, lag 0, after a 7 minute recovery |

**Verdict: the design's two promises hold. 0 gaps and 0 inversions through 14
kills and 3 stalls.** Everything else about the run is bad.

1. **The pool stopped delivering for the last seven minutes and needed no
   further chaos to do it.** The loop ended at 15:50:25 with the group in
   `PreparingRebalance`, three live members, **zero partitions assigned between
   them**, and a lag of 2740 that did not move. All three answered `/_/live`
   with 200 throughout. The group returned to `Stable` at about 15:57:20, only
   after each member had crashed once more on the commit path and its
   replacement rejoined. The full backlog then drained cleanly to lag 0 by
   16:09:42. So the recovery works, unattended, but it costs about **7 minutes
   of total delivery outage after the chaos stops**, and `poc-dest-2` went
   **876 s** between two consecutive deliveries. Evidence:
   `evidence/C4/end-state-preparing-rebalance.txt`.
2. **The duplicate rate is 33 per cent, and it is not bounded by the 5 s commit
   interval.** M12 measured 89 duplicates for one kill and attributed the window
   to `auto.commit.interval.ms`. Here 14 kills produced 1100, which is far more
   than 14 commit intervals of traffic. The reason is
   `design/07-preexisting-findings.md` item 5: when the commit path throws, the
   worker **keeps delivering while its offsets stop advancing**, so the window a
   later kill replays is however far the process got since its last successful
   commit, not five seconds. `poc-dest-3` alone was delivered 1825 times for
   1096 distinct events.
3. **A 20 s stall is invisible, which is worth knowing rather than a fault.**
   `session.timeout.ms` is 45000, so a member `SIGSTOP`ped for 20 s resumes
   inside its own session and the group never notices. The brief's premise that
   20 s "blows max.poll.interval" does not hold on this configuration, where
   `max.poll.interval.ms` is 300000; a stall would have to exceed 45 s to be
   seen at all and 300 s to blow the poll interval. One 60 s stall was therefore
   added at the halfway mark, and that one did evict the member and rebalance
   the group.
4. **Six wedge windows, one of 347 s.** Each is a worker holding partitions
   with lag on them, answering liveness 200, and delivering nothing for over a
   minute. Five of the six have `delivered_total` stuck at exactly 0, meaning
   the worker never delivered anything in that process lifetime. This is the
   design/06 signature and it is invisible to the probe, exactly as design/06
   predicts.
5. **All six crashes followed a rebalance, and the last three are what ended
   the stall.** `evidence/C4/exits-per-rebalance.json` separates the 14 rc=137
   kills from the 6 rc=1 crashes and attributes every one:

   | time | worker | its own last rebalance | followed |
   |---|---|---|---|
   | 15:40:34 | 2 | revoke, 7.1 s earlier | worker1's hold released, 12.6 s earlier |
   | 15:41:50 | 1 | revoke, 42.1 s earlier | worker2's hold released, 13.8 s earlier |
   | 15:49:07 | 3 | revoke, 244.1 s earlier | the `SIGSTOP` of worker2, 9.2 s earlier |
   | 15:54:55 | 1 | revoke, 299.2 s earlier | the chaos loop being over, 117.8 s earlier |
   | 15:54:56 | 2 | assign, 1.1 s earlier | the chaos loop being over, 118.8 s earlier |
   | 15:56:36 | 3 | revoke, 98.1 s earlier | the chaos loop being over, 218.8 s earlier |

   The first three are survivors dying on a rebalance somebody else caused: two
   on a killed worker rejoining, one on a stalled worker being frozen. The last
   three are the group getting itself out of `PreparingRebalance`: all three
   members crashed between 15:54:55 and 15:56:36, and the group reached
   `Stable` at about 15:57:20. **The recovery mechanism was three more
   crashes**, which is why it took seven minutes and why nothing an operator
   could do would have shortened it.
6. **The assignment sampler puts at least one partition unserved for 773 s**
   (`evidence/C4/imbalance.json`, 15:42:08 to 15:55:01), and no window at all in
   which a live worker held nothing while another held two or more. Read that
   figure as a **lower bound with an unreliable attribution**: the sampler's
   partition column is each worker's most recent `rdkafka.assign` line, so it
   cannot tell "still holds" from "was revoked and never reassigned". During
   the `PreparingRebalance` window the broker's own view, captured directly in
   `end-state-preparing-rebalance.txt`, is that **all three** partitions were
   unowned, not one. The fix for a future run is to sample the group's
   `--members` output rather than the workers' logs.

## Reshard under an unhealthy member, through the shipped CLI on the live pipeline

> **Scope note.** This ran as "C5" before the director's scope change reached
> this agent. The briefed C5 belongs to another agent and is written up as
> **C5r**, on RUN_ID-scoped topics, groups and ZooKeeper node; its evidence is
> in `evidence/C5/` (`notes-reshard-under-stalled-member.md`,
> `comparison-table-*.txt`, `reshard-*.out`, `control-*`, `stall-*`,
> `operr-*`). **C5r is the authoritative reshard result.** This run is kept
> because it is not the same experiment: it drives the shipped
> `bin/notificationWorkgroupCutover.js` and the shipped `deliveryWorker/task.js`
> over real S3 traffic through CloudServer and the populator, on the live rig,
> whereas C5r drives `WorkgroupCutover` directly with synthetic records. Its
> evidence has been moved out of the shared directory to
> **`evidence/C5-cli-pipeline/`**. It is excluded from the chaos round's
> summary table, which now covers C1 to C4 as briefed.

Two auto workgroups (hashmod modulo 2, `wg-a`=[0], `wg-b`=[1]) resharded to
modulo 3 (`wg-a`=[1], `wg-b`=[0], `wg-c`=[2]) through the barrier cutover, with
one old-generation worker `SIGSTOP`ped for 30 s in the middle of the drain.
Driven end to end: real S3 PUTs and DELETEs through CloudServer, the real
populator, the shipped `bin/notificationWorkgroupCutover.js`, the shipped
`deliveryWorker/task.js` with its real `WorkgroupConfigLoader`, on a P=3
delivery topic. Evidence: `evidence/C5-cli-pipeline/`.

All three destinations hash to remainder **1** under both moduli, so `wg-b`
owns every destination in generation 1 and `wg-a` owns every destination in
generation 2: a **full handover**, which puts every record's ownership on the
wrong side of the barrier. That is deliberately not what design/11 E1
measured, and the stalled member is the one that owns everything.

Evidence: `evidence/C5-cli-pipeline/`. The other agent's C5r work was fully
namespaced (`poc-c5-*` topics, its own ZooKeeper node, no populator, no
CloudServer) and never touched this round's topics, buckets or destinations, so
the two runs did not interfere.

| measurement | value |
|---|---|
| driver operations delivered as events | 3268 (1089 / 1090 / 1089) |
| **gaps** | **0** |
| **duplicate extras** | **0** |
| **per-key inversions** | **0** |
| generation 1 cutover | rc=0, barriers `{0:0, 1:0, 2:0}`, both gen-1 groups pre-seeded |
| generation 2 cutover | rc=0, barriers `{0:187, 1:187, 2:187}`, all three gen-2 groups pre-seeded, `previousGroups` = the two gen-1 group ids |
| **`verify` exit 0** | **never**, 60 polls over 600 s, every one rc=2 |
| drain report at the stall | `wg-a-gen1` remaining 187/187/187, `wg-b-gen1` remaining 46/49/44, overshoot 0 |
| stall to first delivery again | 55.3 s on every destination |
| **longest pause** | **641.4 s / 639.3 s / 635.3 s** |
| end-to-end latency p50 / p99 | 527.8 / 829.0 s and the same on the other two |
| generation 2 ownership | `wg-a` delivered 903 / 904 / 903, `wg-b` and `wg-c` each skipped 2710 `not_in_slice` and delivered 0 |
| barriers seen | 3 `match="current"` per generation 2 workgroup |
| process exits | worker1 (wg-a gen1) 1, worker2 (wg-b gen1) 2, generation 2 workers **0** |
| rebalances | worker1 **432/431**, worker2 309/308, generation 2 workers 1/0 each |

**Verdict: 0 / 0 / 0 on the data, and the procedure could not be completed.**
Four findings, and the first two are new.

1. **`verify` never cleared, and the reason was a wedged workgroup that owned
   nothing.** `wg-a` in generation 1 held remainder 0, which no destination
   hashed to, so its only job was to consume and commit. Its worker wedged
   instead, in the design/06 pattern: **432 assigns and 431 revokes**, committed
   offset stuck at exactly the pre-seed of 0 on all three partitions,
   `/_/live` answering 200, and not one `skipped_total` sample. Because `verify`
   requires **every** previous group past **every** barrier, a workgroup that
   could not lose a single record blocked the cutover for the full 600 s of
   polling. Evidence: `evidence/C5-cli-pipeline/wedge-wg-a.txt`. A drain report that
   separated "this group still owes deliveries" from "this group still owes
   commits" would not have blocked here.
2. **The 30 s stall took the loaded old-generation worker out permanently, by
   way of the crash and the E4 recovery hole.** 55 s after the `SIGCONT`,
   `wg-b`'s worker died on the uncaught commit-path throw. The supervisor
   restarted it 2 s later, its loader read the now generation-2 document,
   derived `chaos-c5-pool-wg-b-gen2`, and it **defected**. Its own log carries
   both group ids, and the broker agrees: `chaos-c5-pool-wg-b-gen1` ended with
   no active members, frozen at 141/138/143 against barriers of 187. The other
   restart route was tested directly and also fails: pinned to generation 1 the
   loader refuses with `the workgroups document is at generation 2, this worker
   is pinned to generation 1` and the process exits. So design/11's E4 hole is
   **not only reachable by an operator crashing a worker**: a 30 s stall is
   enough, and under any supervisor the defection is automatic and silent.
   Evidence: `evidence/C5-cli-pipeline/pinned-restart-refused.txt`.
3. **Nothing was lost, and that is the drain report over-warning by exactly the
   amount design/11 E2 predicts.** The report said 139 records were still owed
   below the barriers by a group that then lost its last member, which is the
   E2 loss shape. The checker says **0 gaps**: `wg-b`'s worker had in fact
   delivered those records before it crashed, and only its *commits* were
   behind, which is the other half of design/07 item 5. The report reads
   committed offsets, so it cannot tell the two apart, and it errs safe. The
   consequence for an operator is not lost data, it is that **the tool's refusal
   is not actionable**: it says "do not proceed" without saying whether the
   remaining records are undelivered or merely uncommitted.
4. **The cost of the guard was a ten-minute delivery outage, and the guard was
   right to be there.** Once `wg-b`'s worker had defected, no member of either
   generation-1 group existed and every generation-2 group was seeded above the
   barrier, so nothing delivered anything from 16:16:21 until generation 2
   started at 16:27:15: **641 s** on every destination, and a p50 end-to-end
   latency of 528 s for the whole run. Generation 2 then behaved perfectly:
   one assign each, no revokes, no exits, `wg-a` delivered all three
   destinations, `wg-b` and `wg-c` skipped 2710 records each as
   `not_in_slice`, every workgroup saw its own 3 barriers, and the whole
   backlog drained to lag 0 with 0 duplicates and 0 inversions. The barrier
   pre-seed did exactly its job across a full handover.

## Chaos round summary

C1 to C4 as briefed. The reshard scenario is the other agent's C5r; the run
this agent completed before the scope change arrived is reported separately
above and is not in this table.

| scenario | gaps | duplicates | inversions | longest pause | rebalances (assign/revoke) | wedges | process exits | crashes per membership change |
|---|---|---|---|---|---|---|---|---|
| **C1** one consumer dies in a three-worker group | **0** of 3724 | 122 | **52** | **165.3 s** | 13 / 10 | 5 windows (see note) | **5** (4 crashes + 1 kill) | 1 kill -> **2 survivors dead** in 42 s and 49 s |
| **C2** adding processors under load | **0** of 1797 | 13 | **0** | **211.8 s** | 9 / 5 | 1 window | **4**, all crashes, **no kills issued** | 3 additions -> **4 crashes** of running members |
| **C3a** partition growth, unsafe | **0** of 2635 | 7 | **6480** | 93.2 s | 9 / 6 | 3 windows | **3** crashes | 1 partition alter -> **3 crashes**, one per worker |
| **C3b** partition growth, recommended | **0** of 2626 | **0** | **0** | 38.0 s | 28 / 25 | 0 | **0** | populator restart -> **0** |
| **C4** chaos loop, 10 minutes | **0** of 3290 | **1100** | **0** | **876.7 s** | 40 / 33 | 6 windows | **20** (6 crashes + 14 kills) | 14 kills + 3 stalls -> **6 crashes**, 3 of them the only thing that ended the final stall |

Every crash in every row is the same uncaught `Local: Erroneous state` out of
`lib/BackbeatConsumer.js:851`, and every one of them followed that worker's own
`rdkafka.revoke` or `rdkafka.assign`. The per-scenario attribution is in
`evidence/<Cx>/exits-per-rebalance.json`.

Unserved partitions, from `evidence/<Cx>/imbalance.json`:

| scenario | a live worker held nothing while another held two or more | at least one partition served by nobody |
|---|---|---|
| C1 | 1 window, 36.7 s | 1 window, 157.9 s |
| C2 | 2 windows, **39.6 s** | 2 windows, **213.0 s**, the larger 199.6 s with p1 unowned |
| C4 | none | 2 windows, **786.9 s**, the larger 773.4 s |

C2's 199.6 s window and the 211.0 s pause the checker measured for
`poc-dest-3` from customer-topic timestamps are two independent measurements of
the same outage. C1's and C4's figures are lower bounds with unreliable
attribution: the sampler reads each worker's most recent `rdkafka.assign` line,
so it cannot tell "still holds" from "was revoked and never reassigned". Where
the broker's own view was captured directly (C4's
`end-state-preparing-rebalance.txt`) it is worse than the sampler's: all three
partitions unowned, not one.

C1's wedge count is the weaker of the two measurements: its sampler recorded
only the group's total lag, so a worker with an idle partition next to somebody
else's backlog can be counted. From C2 onwards the sampler records
per-partition lag and the detector uses only the worker's **own** partitions.
Two of C1's five windows are the initial join churn and one is the
SIGTERM-hung process; the other two are genuine.

Totals over C1 to C4: **0 gaps across 14 072 expected events** (15 314 actual
deliveries, the difference being the duplicates), 1242 duplicate extras, 6532
per-key inversions confined to two causes, 15 wedge windows, and **17 uncaught
`Local: Erroneous state` process crashes** that nobody asked for (4 in C1, 4 in
C2, 3 in C3a, 0 in C3b, 6 in C4), plus one more in the abandoned first attempt
at C1. Only 15 of the 32 process exits were deliberate.

## What this means for operations

**The pause an operator sees is not the session timeout.** `session.timeout.ms`
is 45 s and that figure does show up: in C1 one destination resumed 45.3 s after
its worker was killed. But the destination the dead worker actually owned waited
136 s, and the longest pause of that run, 165 s, came from putting the worker
**back**. In C2 a partition sat unowned for 211 s while an idle worker stood
next to it; in C4 a destination went 877 s between deliveries. The multiplier
is always the same: an eager rebalance revokes every
partition from every member, the revoke crashes a member on the unguarded
`offsetsStore` at `lib/BackbeatConsumer.js:851`, the crash starts another
rebalance, and the group cycles. **Plan pauses in minutes, not in the 45 s the
timeout suggests, and alert on a destination's delivery rate rather than on
group membership.** The liveness probe is worthless for this: every wedged and
every idle worker in this round answered `/_/live` with 200.

**Eager assignment is the wrong strategy for this design, and it is currently
the default.** `partition.assignment.strategy` is never set, so librdkafka uses
`range,roundrobin`. The pool puts many members in one consumer group and
expects membership to change routinely, which is exactly the workload
`cooperative-sticky` exists for: it would leave untouched partitions assigned
across a membership change instead of revoking all of them, which removes both
the pause and the revoke that triggers the crash. Switching it is a one-line
config change and it cannot be mixed with eager members, so it has to be done
in one step. **Fixing the uncaught throw comes first**, though: with the throw
in place, even a cooperative rebalance that touches one member's partitions
would still kill that member.

**Growing the delivery topic's partitions is a two-step procedure, not a
one-liner, and the order is the whole thing.** Altering the topic while the
populator is producing cost 6480 per-key inversions, all of them on the two
destinations whose delivery key moved to a new partition and none on the one
that stayed. The window is 257 s wide because the consumer sets
`metadata.max.age.ms` to 5 s while `BackbeatProducer` leaves
`topic.metadata.refresh.interval.ms` at librdkafka's 300 s default, so for over
four minutes the producer wrote to the old partition while a different worker
was already serving the new one. Stopping the populator, waiting for lag 0,
altering, and restarting gave **0 / 0 / 0** and cost 30 s of waiting plus a 38 s
pause, because a fresh producer fetches metadata at connect and picked up the
new partitions in 13 s. Gate on the delivery group's **lag being 0 and a
delivery counter still moving**, per M4b. And do not expect capacity from it:
`poc-dest-3`'s key hashed to the same partition under P=3 and P=6, no
destination gained a second worker, and single-destination capacity still goes
through `spreadFactor`.

**Worker count versus partitions: more workers than partitions buys nothing,
and it is not free.** One worker served all three destinations happily. The
fourth worker in C2 took no partition, which is the expected consequence of
one-destination-one-partition, but a third worker also sat idle for most of the
run while a partition it could have taken went unserved. Size the pool at one
worker per partition of the delivery topic and no more, and treat "a worker
with no partitions" as an alert rather than as spare capacity.

**The duplicate bound is not the commit interval.** M12 attributed its 89
duplicates to `auto.commit.interval.ms` of 5 s. C4's 14 kills produced 1100,
a third of all deliveries, because a worker whose commit path has thrown keeps
delivering with its offsets frozen, so the window a later kill replays is
however far it got since its last successful commit. Any consumer of these
notifications must be idempotent, and the "roughly five seconds of traffic"
figure should not be quoted as a bound.

**SIGTERM does not stop a delivery worker.** In C1 the process logged
`received SIGTERM, exiting`, timed out its `LeaveGroupRequest`, and was still
alive and still answering liveness 200 four minutes later. Any rolling deploy or
drain script needs a `SIGKILL` deadline, and `BackbeatConsumer.close()` needs a
deadline of its own.

**For a reshard, the drain report is safe but not actionable, and a stalled
worker is a one-way door.** In the reshard run above, `verify` never exited 0 in
ten minutes of polling, and the blocker was a workgroup that owned no
destination and could not lose a record: its worker had wedged, so its
committed offset never moved past
the barrier. Meanwhile the workgroup that owned everything was stalled for 30 s,
crashed on the resulting rebalance, and its supervisor restarted it straight
into the new generation's consumer group, abandoning the old group's drain for
good. Neither restart route works after a cutover, which is design/11's E4 hole
reached without any operator error at all. Three things follow: **fix the
uncaught throw before workgroups ship**, adopt the generation-addressed znodes
of design/11 so a pinned worker can finish its own generation, and split the
drain report's `remaining` into deliveries still owed and commits still owed so
that a no-op workgroup cannot block a cutover.

## New pre-existing defects found in this round

1. **The `offsetsStore` throw at `lib/BackbeatConsumer.js:851` is fatal to the
   process.** `design/07-preexisting-findings.md` items 3 and 5 record the throw
   and its data cost; what is new is that in a standalone worker there is no
   handler, so node-rdkafka rethrows and the process exits rc=1. It fired **18
   times** across the six measured runs, and once more in the abandoned first
   attempt at C1, on entirely ordinary events: a member joining, a member
   leaving, a partition count change, a stalled member resuming. In a
   three-member group one deliberate `kill -9` took both survivors down with it.
   This is the single defect that turns every membership change in the pool into
   a multi-minute outage, and it is in `lib/`, so it affects flag-on and
   flag-off alike. The one-line guard the code's own comment already
   contemplates (BB-758) would close it.
2. **`SIGTERM` can hang a delivery worker indefinitely.** Observed at 4 minutes
   and still running, with liveness answering 200 the whole time.
   `BackbeatConsumer.close()` waits on a revoke callback with no deadline
   (design/06 records the same shape at `:1122`), and `task.js` has no timer of
   its own.
3. **`BackbeatProducer` sets no metadata refresh interval**, so every producer
   in the tree takes up to 300 s to notice a new partition while every consumer
   notices in 5 s. That asymmetry is the whole width of C3a's reorder window.
4. **`verify` cannot distinguish deliveries owed from commits owed**, so a
   workgroup that owns no destination, or any workgroup whose commits lag its
   deliveries, blocks a cutover it cannot affect. The reshard run spent 600 s
   refused over 139 records that had already been delivered. Cross-check this
   against C5r, which is the authoritative reshard result.
5. **The delivery worker exposes no metric for "assigned nothing".** A member
   holding zero partitions is indistinguishable from a healthy one on
   `/metrics` and on `/_/live`, and C2 had two such members for minutes while a
   partition went unserved.
