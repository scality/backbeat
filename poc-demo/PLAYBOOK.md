<!-- markdownlint-disable MD013 -->

# BNaaS demo: the recording playbook

For whoever records the BNaaS (bucket notifications as a service) POC, and for
Taylor picking it up cold. Run it top to bottom: the ten commands that take a
fresh clone to a finished demo-pace run; then, act by act, what the terminal
prints, what to point at in Grafana, Kafka UI and ZooNavigator, the numbers to
expect, the caveats to say out loud, and how long each act takes; then
troubleshooting and teardown.

How Federation actually runs this against a platform, as one `run.yml` with
nothing between its stop and its start, is `poc-demo/OPERATOR.md`.

**The model, decided 2026-09-11.** The delivery workers read today's topic,
`backbeat-bucket-notification`, the one the populator already writes one record
per event to, and match each event against the bucket's rules per destination
themselves, as a processor does. Production applies every change through
Ansible, which stops the old containers and starts the new ones, so the
migration and every workgroup layout change are plain container swaps: the
new worker groups are seeded from the committed offsets of what they replace,
lowest per partition plus a watermark per destination, and since 2026-09-11
the workers do that themselves at start, so the run is stop, write, start
with no command in between. `bin/notificationDeliverySeed.js` still exposes
the same seeding for an operator who would rather run it ahead. No second
topic, no populator change, no drain, no barrier, no overlap, no return to
per-destination processors. The
previous model (a destination-keyed delivery topic, a populator switch, a
barrier cutover) and its 2026-09-10 numbers are kept under `poc-demo/results/`
for reference and are not what this playbook records.

Every number below comes from runs of this branch on the build machine on
2026-09-11 (a 12-core Apple-silicon Mac, Docker VM raised to 23.4 GiB that morning, no other Kafka stack on the machine, load average about 3; the earlier same-day numbers on a pressured VM with three brokers are kept as history in the timing table). Where a step in this file was executed, its
timestamp is given. Nothing else is stated as fact.

## The ten commands

From nothing to the demo. Each was executed on the build machine on
2026-09-11 (times CEST). The clone was `/tmp/bnaas-demo-clone`.

```bash
# 1. clone the demo branch and enter it                             (11:05:45, 20 s)
git clone --branch poc/S3C-11127-demo git@github.com:scality/backbeat.git && cd backbeat

# 2. node 22 on PATH; 24 breaks the native modules and the install. With nvm:
nvm install 22 && nvm use 22

# 3. install, with the one macOS/node-gyp workaround folded in      (11:06:05, 60 s)
yarn demo:install

# 4. check the machine: docker up, node 22, ports free, python OK   (11:07:05, passed)
yarn demo:preflight

# 5. bring the stack up, with the Kerberos profile for act 07
#    (builds the local-only images on a first run)                   (11:07:06, 51 s, stack already built)
yarn demo:up:krb

# 6. block until broker, mongo PRIMARY, CloudServer and Grafana answer   (11:07:57, ready in 1 s)
yarn demo:wait

# 7. the take; about 38 min (DEMO_PACE=demo is the default)      (11:57:57 to 12:36:14, 38 min 17 s, 12 mocha cases passing)
DEMO_ACTS=02,03,04,05,06,08 yarn ft_test:demo
```

Variants, not extra steps:

```bash
yarn ft_test:demo                          # the reference run, all eight acts (48 min 18 s, 15 mocha cases passing, executed 11:07:58 to 11:57:16)
DEMO_ACTS=04 yarn ft_test:demo             # one act, to reshoot it
DEMO_ACT04_WITHOUT_WATERMARK=1 DEMO_ACTS=04 yarn ft_test:demo   # the swap without the watermark: what it saves
DEMO_PACE=slow yarn ft_test:demo           # every wait doubled, for a careful take
yarn demo:down                             # teardown (see the last section)
```

What can go wrong at each step is in Troubleshooting. `yarn demo:preflight` is
the one to trust on a new machine: it names anything that will block a run.

## Before the take

- **Branch tip.** Run from `poc/S3C-11127-demo` at `25822ac1` or later. CI is
  green on it (workflow run 34582649670): the four delivery pool suites run on
  every push, deliverypool and internal green, workgroups and kerberos marked
  experimental until the `BackbeatConsumer` fixes land upstream.
- **Give the demo the machine.** Stop every other Kafka stack and rig first.
  On the build machine that means the compose projects `ft` (the older
  notification rig: `ft-kafka-1`, `ft-zookeeper-1`, `ft-redis-1` and its
  `bnaas-mongo`) and `bnaaskrb` (the Kerberos spike), and the parked rig
  workers in the tmux session `bnaas-rig`. Why: the Docker VM has 7.75 GiB,
  and during the reference run two Kafka brokers, two ZooKeepers and the
  emulated CloudServer shared it with three rig workers on the host, at a
  load average near 6. A consumer's request to the group coordinator timed out
  at 36 s under that pressure, and what followed (below) cost one workgroup its
  whole act. With the machine to itself the stack can also move to
  `PORT_OFFSET=0`.
- **Disable system sleep**: `caffeinate -dimsu` in a spare terminal for the
  length of the recording. The containers survive a sleep, the consumer groups
  do not: they rebalance and a long act never recovers its narration.
- **Three browser tabs**, beside the terminal:
  - Grafana <http://localhost:4000>, dashboard **BNaaS delivery pool**. Time
    range **Last 15 minutes**, refresh **5s**. Open it with
    `?from=now-15m&to=now&refresh=5s&kiosk` on the dashboard URL for a clean
    frame.
  - Kafka UI <http://localhost:9085>, cluster **bnaas-demo**: the **Topics**
    page for acts 02 to 04, the **Consumers** page for act 06.
  - ZooNavigator <http://localhost:10000>. Connect with the string
    `zookeeper:2181`, no auth, then open `/bnaas-demo/delivery-workgroups` (the
    URL `editor/data?path=/bnaas-demo/delivery-workgroups` goes straight there).
  - At `PORT_OFFSET=0` subtract 1000 from each port, except ZooNavigator, which
    is 9000 there.
- **Layout**: terminal on the left half, it narrates every step in plain
  language with a UTC timestamp and ends each act with a table. Grafana
  top-right, Kafka UI and ZooNavigator stacked bottom-right. The terminal is the
  script, the browsers are the evidence.
- **The through-line**, to open with and close on: a destination is a
  deployment artifact today, and this makes it a resource. Nothing here loses an
  event. What changes is what a failure looks like.

## The consumer wedge: root-caused and fixed during the POC

Every consumer start on this branch used to stall: a fresh consumer held all
its partitions, cycled `assign -> revoke -> "processing queue idle,
un-assigning"` about once a second, delivered nothing, and answered its
liveness probe with 200, until the group coordinator timed a stale generation
out at 45 s or a restart cleared it. It fired on 5 of 21 consumer starts in the
July rig round, on every pool worker start in the 2026-09-10 reference run, and
up to 13 times in one act on the morning of 2026-09-11. Every earlier pause and
most of the earlier duplicate counts carried its cost.

The cause was found on 2026-09-11 and fixed on this branch (`c27b0f28`, with
consumer guards in `63d79adf`): while a fresh consumer has no partitions,
`BackbeatConsumer._tryConsume` asks node-rdkafka for the topic's metadata, and
node-rdkafka requests metadata for **all** topics unless `allTopics: false` is
passed; librdkafka re-evaluates the subscription on every full metadata
response, flips it, rejoins, and discards the JoinGroup in flight. One flag on
two metadata calls. Measured after the fix: first assignment holds, records are
consumed about 2 s after start, and the reference run below had **zero** wedge
cures in eight acts where the same morning's runs had 1 to 13 per act.

The suite keeps its cure machinery (stall gates that watch progress, restart
the one stalled consumer, wait for its first assignment) as a safety net, and
every act's table carries a "wedge cures" count so a run can be compared with
the pre-fix history. If `WEDGE SUSPECTED` appears on camera, say what it is: a
pre-existing `BackbeatConsumer` behaviour, root-caused and fixed during this
POC, and the cure is what an operator did before the fix. If an uncaught
`Local: Erroneous state` ever appears in a worker log, that is new: the guards
of `63d79adf` made every read of the consumer's client state safe.

## Act by act

Each act ends with a table: the rig's measurement or the expectation, this
run's, and a verdict word (`same`, `close`, `context`, `DIFFERS`). The `gaps`
rows are loss and must be zero. Duplicates and inversions move with timing.
Durations are ACT START to ACT END from each act's `timeline.txt`.

### Act 01: here is the code (185 s)

Reference run: 11:08:31 to 11:11:37 CEST. Unit suite 1757 passing, 1 pending,
5 failing; lint exit 0. The five failures are `patchConfiguration` (two hooks)
and `OplogPopulator` ("should fail when mongo connection fails", "can't get
metadata db", "can't get metastore collection"), all 30 s timeouts on a
MongoDB connection attempt: the same suite passed 1759 / 1 / 0 at 09:00 the
same day, before the Docker restart removed the older stacks' MongoDB from the
port those tests dial. Pre-existing test environment coupling, not the POC
code; say so if the row is on screen.

Terminal only: the commits of the segments on top of development/9.3, the
file map of the delivery pool, where the tests live. Say: the delivery side is
a backbeat delivery worker in **Node**, on the existing consumer, reading
**today's** topic; not a new service, not a new language, not a new topic. Name
the two pieces added on 2026-09-11: the internal-source path of the worker
(match per destination, like a processor) and `bin/notificationDeliverySeed.js`,
the one step an Ansible run does between deleting the old containers and
starting the new ones. The three functional suites (pool, workgroups, kerberos)
are out of CI on purpose and behind `DEMO_ACT01_FULL=1`.

### Act 02: what today does (95 s, rig M1)

Reference run: 11:11:37 to 11:13:12 CEST. Every row as expected. 0 start-up wedge cures.

A real S3 PUT through CloudServer, the mongo oplog, the populator, one
per-destination processor, the customer's topic. **Kafka UI, Topics**: the
internal topic `backbeat-bucket-notification` with four partitions and the
customer topic growing by 25. Point at the record key `<bucket>/<objectKey>`,
the `size` field as a **string**, and the nulls (`eTag`, `versionId`,
`sequencer`, `principalId`) the mongo log source gives. Nothing about this
changes: the workers read the very same records.

Say the cost: about 12 processes at about 78 MB per destination, roughly 936 MB
whether or not traffic flows, 11 of them idle standbys; about 56 destinations
fit on six nodes; the requirement is 10,000.

The terminal prints "seeded ... at the head of backbeat-bucket-notification on
partitions 0, 1, 2, 3" before the processor starts. Say what it is: a group that
has run in production for years has a committed offset on every partition; a
brand-new one starts at `latest` and would skip what lands during its first
rebalance, so the suite puts the group in the production state first.

### Act 03: the failure that is invisible (264 s, rig M10)

Reference run: 11:13:12 to 11:17:37 CEST. Every row as expected. 0 start-up wedge cures.

The act that earns the redesign. Three dead destinations: a refused connection,
an unroutable address, and a reachable broker with an unwritable target.

Legacy half: the processor for the refused destination **cannot start**; under
a supervisor that is a crash loop consuming nothing. The third class depends on
the cluster and the act says which it built. On this single-broker stack it is
a topic with `max.message.bytes` below one event: the produce is **rejected
immediately**, the processor's send callback fires with an error, it calls
`done()`, and the consumer **commits and advances past events it never
delivered** ("moved, 33 to 53"). Same silent failure, one step worse: no
counter, and the failed topic the config names stays at 0 because no production
code reads it.

Pool half, on today's topic: the worker starts **healthy** with every dead
destination configured (producers are created lazily, per endpoint).
**Grafana, "Drops per second, by reason"**: `producer_error` at once for the
refused and blackholed endpoints, `delivery_error` after about 30 s for the
unwritable one (the configured deadline). The healthy destination delivers
throughout: 5 of 5 delivered.

Then the row that is new with today's topic, **"pool commit progress on shared
partitions"**: lag 0 at t+31s, peak lag 65, healthy 5 of 5 at t+13s. Say what it means: the dead destinations'
records sit on the same partitions as everybody else's, so the partitions'
committed offsets can only move once the dead records ahead of them have been
dropped; the lag holds for about one delivery deadline and then falls to 0.
Isolation on today's topic rests on the deadline and the drop, not on
partition ownership, and it is bounded by the deadline.

Say: today a dead destination fails silently with no counter, by stalling that
destination forever or by committing past dropped events. The pool makes it a
bounded, counted, per-destination drop within 30 s. Keep the caveat: the drop
reason cannot yet tell a timeout from a rejection.

### Act 04: the migration, one Ansible run (430 s)

Reference run: 11:17:37 to 11:24:47 CEST. Every row as expected. 0 start-up wedge cures.

Production applies every change by replacing containers, so this act is the
playbook run itself and nothing else: no drain, no topic switch, no rollback
step. The populator is untouched throughout; it keeps writing one record per
event to `backbeat-bucket-notification`, today's topic, and does not know which
path consumes it.

1. Today's path, three destinations, one processor each, under load
   (`demo-mig-1..3`, one bucket per destination). The terminal seeds each
   processor group at the head first, the way a production group already
   stands, and warms it.
2. `SIGSTOP` on the processor of `poc-dest-3`: a stalled destination whose
   backlog grows for the rest of the load. That is what a destination stuck
   behind a dead endpoint looks like from the broker. A little later, stop the
   processor of `poc-dest-2`: a small backlog. `poc-dest-1` stays caught up.
   **Grafana, "Migration: processor groups versus pool groups"**: three
   processor groups, three different lags.
3. **The run, part one: delete every processor.** The terminal prints each
   group's committed offset per partition first. That table is the whole
   argument for the next step: three groups, three different places in the
   same topic.
4. **Part two: write the layout.** The act writes the generation 1 document
   (one auto workgroup, since the migration is about offsets, not slicing)
   and stops. That is the whole of part two: **no seeding command runs**.
   Say it out loud, because it is the point of the act: an Ansible run can
   express stop, template, start, and nothing else.
5. **Part three: start the worker container.** The worker finds its consumer
   group empty, takes an ephemeral lock at
   `/bnaas-demo/delivery-workgroups/seed-locks/gen1`, seeds every group of
   the generation from the processor groups and writes the watermarks,
   releases the lock, and only then subscribes. The act reads that evidence
   back out of ZooKeeper and out of the worker's log, which says `seeded
   itself`. Read the numbers aloud: per partition the lowest processor
   offset, so nothing is skipped, and a watermark per destination at its own
   processor's offset, so nothing already delivered is sent again.
   **ZooNavigator**: the document at `/bnaas-demo/delivery-workgroups`, its
   `watermarks/gen1` child and its `history/gen1` archive. The act then
   prints the delivery pause when the first pool delivery lands: 21s, processors stopped to first pool delivery. Say what it is: the
   container swap, about a second of seeding, and the group join, 45 s of
   which is the consumer session by default.
6. The check, per destination: gaps 0, inversions 0, duplicates the processors' uncommitted windows only: 2 (poc-dest-1 2, poc-dest-2 0, poc-dest-3 0). Point at **Grafana, "Records
   skipped under a watermark"**: those are the caught-up destination's
   already-delivered records being committed without a second delivery,
   163, already delivered by a processor of them. And say the last row aloud: the stalled
   destination's backlog, 280 records after the swap, of 280 operations in the whole load, delivered by the pool. Today that
   backlog sits behind a frozen offset until somebody notices.

The variant `DEMO_ACT04_WITHOUT_WATERMARK=1 DEMO_ACTS=04 yarn ft_test:demo`
runs the same swap with `seedOnStart` off, the seeding CLI run ahead of the
start, and the watermarks deleted before the worker reads them. It measures
what the watermark saves, the spread between the processors' offsets
delivered twice, and it is also the pass that keeps
`bin/notificationDeliverySeed.js` covered. Measured once on 2026-09-11 (10:39, before the consumer fix): 258 records delivered twice against 30 with the watermark on the same swap, 0 lost, 0 inversions, pause 21 s; the stalled destination's 280 backlog records arrived either way.

### Act 06: workgroups, and ZooKeeper (540 s, rig W gates and C5r)

Reference run: 11:31:05 to 11:40:05 CEST. Every row as expected. 0 start-up wedge cures.

The act to slow down on. Say the topology is decided: the workers read
**today's** topic; a workgroup is a consumer group over it with a slice filter,
so a worker commits records outside its slice without delivering them. No
second topic, no populator change, no per-workgroup topics. And say how a
change is applied: a layout change is a new generation, and a new generation
is an Ansible run, containers stopped and started, with nothing between them.
The seeding is not a step. The first worker of the new generation to come up
finds its group empty, takes a lock in ZooKeeper, seeds every group of the
generation from the groups it inherits from, and the others wait for their
own offsets before they join.

With the ZooKeeper browser open, in this order:

- **The document is the contract.** A generation, a hashmod rule and optional
  static pins. Show it in ZooNavigator at `/bnaas-demo/delivery-workgroups`
  and in the terminal (`poc-demo/bin/zk-show.sh`). Account-scoped
  destinations never touch it: they hash into a workgroup by name and start
  delivering with no change and no restart. Only a layout change (the number
  of auto workgroups, a pin) is a new generation.
- **How a destination picks its workgroup.** md5 over the encoded destination
  token, modulo the hashmod modulo, remainders covering the whole modulo.
  Total coverage makes ownership a total function: every record has exactly
  one owner, which is why a worker can commit what it does not own without
  anything being lost. The act prints the mapping with the repository's own
  function.
- **One consumer group per workgroup**, named `<base>-<workgroup>-gen<G>`.
  **Kafka UI, Consumers**; **Grafana, row "Workgroups"**.
- **The cost of one shared topic**: a hash and a commit per record a workgroup
  does not own, no I/O. Stated, not hidden.

Then what happens to it:

- **A worker dies** (`kill -9` on wg-b's worker). Only its destinations
  pause; wg-a keeps delivering. Point at the per-workgroup **delivered**
  panel, not the lag one: both groups read the whole topic and skip what they
  do not own, so their raw lag is about the same number. Then say what a wedge
  looks like instead, because it is worse: the worker is up, holds its
  partitions, answers liveness 200, and its delivered counter does not move.
  Check the counter, not the lag. The cure is a restart. Measured: only the dead workgroup's destinations pause.
- **A noisy destination is pinned**, generation 2. A static rule beats the
  hashmod one, so `poc-dest-1` is carved out of the hash space into its own
  workgroup with its own blast radius: the lever for a hostile tenant. The
  change runs in Ansible's order: stop generation 1, write the document,
  start generation 2, and generation 2 seeds itself from generation 1's
  groups as it comes up. Pause measured:
  19s: seed 5s, start 14s (0 wedge cures), first delivery 0s after start. Measured: served by the pinned workgroup only.
- **A reshard, two auto workgroups to three**, generation 3, under traffic.
  Same order, and again no command between the stop and the start. The
  seeding takes, per partition, the lowest committed offset across the
  generation 2 groups a new group inherits from, and a watermark per
  destination at its previous owner's offset, so the destination that
  changes owner keeps its place in the stream. Pause measured:
  19s: seed 5s, start 14s (0 wedge cures), first delivery 0s after start. In the reshard's own window: gaps 0, inversions 0, duplicates the stopped generation's window 573, cure re-deliveries 0, in the window 162; whole act: 952: 356 across the generation 1 stop, 573 across the generation 2 stop, 23 across the wg-b kill; 0 within a generation.
  - **Why loss is impossible**: every record either was delivered by the old
    generation or is read by the new one from the lowest offset.
  - **Why duplicates still appear, and how many**: the watermark stands at
    the previous owner's *committed* offset, and on today's topic the consumer
    commits a partition contiguously, up to the oldest record still in flight.
    Every destination shares every partition, so one slow lane (a hot object
    key delivers one record per producer poll, 2 s) holds the whole
    partition's committed offset back while hundreds of later records are
    delivered. Stop that generation and the next one, seeded at the committed
    offset, delivers them again. In the reference run that was 356 records
    across the generation 1 stop and 573 across the generation 2 stop, 0 from
    cure restarts (there were none), 0 attributable to the seed. On the
    previous model a slow destination held back only its own partition. Two
    mitigations, named and not built: release a lane on the producer's
    delivery report instead of its 2 s poll, and a graceful stop that writes a
    per-destination "delivered up to" mark so the next generation's watermark
    is the delivered offset rather than the committed one. At-least-once
    holds; the size of the window is the finding.
  - **Why nothing is reordered**: one generation at a time. Two generations
    delivering the same keys together reordered 4197 pairs in one rehearsal of
    the previous model (2026-09-11, before the swap order); never doing that
    costs exactly the pause above.
  - **What still stands**: a destination's per-object lanes deliver one record
    per producer poll, 2000 ms; and a kill or a cure restart re-delivers the
    worker's uncommitted window, at-least-once.

Captures to have ready: `poc-demo/evidence/screenshots/` (bnaas-grafana-act04-migration-topic-a.png, bnaas-zoonavigator-act04-watermarks-gen1.png, bnaas-kafkaui-act06-consumer-groups-topic-a.png and bnaas-grafana-act06-workgroup-lag-by-generation-topic-a.png, all captured live during the 11:07 reference run).

### Act 08: the semantics that change (570 s, rig M9, M8, M6b)

Reference run: 11:47:23 to 11:56:54 CEST. Every row as expected. 0 start-up wedge cures.

Three short cases, each ending in a decision. The mixed-window case of the
previous model is gone with the second topic: there is one path, and every
change is a container swap, so no two consumers are ever alive together.

- **Detach**: today a detached destination's queued events are dropped
  silently, which reads like a revocation. The pool reads the same topic and
  matches at delivery time too, so the act measures what it does:
  legacy 0 of 20, pool 0 of 20. What the pool can do that the processor cannot is count the
  drop. Product question 3.
- **Overlapping rules**: a catch-all plus a prefix rule delivers to both
  destinations, on both paths (legacy yes, pool yes). First-match-wins is what
  neither implementation does, so shipping it literally would be a silent
  behaviour change. Product question 6.
- **Name collision** (slow down here): an account-scoped ARN naming a
  destination that does not exist is refused; the same shape naming an
  existing **global** destination is accepted with HTTP 200, read back
  verbatim, and every event lands on the global destination carrying a
  configuration id it never set up (12 of 12). Every component
  matches on the last ARN segment and discards the account field. Any tenant
  who can put a bucket notification configuration can do this today. Product
  question 5, and a threat-model row.

### Act 05: crashes (377 s, rig M11 and M12)

Reference run: 11:24:47 to 11:31:05 CEST. Every row as expected. 0 start-up wedge cures.

Two `kill -9`s on the populator under load. The first lands during read and
filter and costs nothing. The second is timed to land right after a publish,
between the kafka acknowledgement and the ZooKeeper offset write: the restart
republishes the whole batch, republished checkpoint window, 135 records. Low probability, high amplitude,
at-least-once, zero loss. **ZooNavigator**: the populator's `logOffset` node is
the checkpoint.

Then `kill -9` on the worker. The supervisor restarts it two seconds later, the
way systemd would. The replacement holds nothing until the dead member's
session expires (45s measured). It re-delivers the uncommitted window
and nothing else: the uncommitted window: 19 of 42. **Grafana, "Delivery workers up"**: 1 to
0 to 1. Say the caveat this model adds (act 06 has the numbers): on a shared
topic the uncommitted window is everything since the oldest in-flight record on
the partition, not "5 s of traffic".

### Act 07: Kerberos (437 s, rig GATE 2)

Reference run: 11:40:05 to 11:47:23 CEST. Every row as expected. 0 start-up wedge cures.

One process, two identities. librdkafka gives one Kerberos identity per OS
process, measured, with every documented workaround tried and failed; the
per-connection producer authenticated two and then fifty principals in one
process. The rule to state on camera: only the broker's own `authenticationID`
line counts, because a client stack can report the identity it was configured
with rather than the one it authenticated as. Unchanged by the topic decision.

Needs `yarn demo:up:krb` run from the checkout the suite runs from (the KDC
writes the keytabs into that checkout's `poc-demo/krb/keytabs/`); it also
builds the test image and seeds its node_modules volume
(`bin/krb-test-image.sh`, setup time, not demo time). Without the profile the
act skips with a message that names that script.

## Close

Three decisions, not more demo: the failure contract in the requirements' own
words (retry for a window, then abandon and count), destination naming across
account-scoped and global, and whether per-object ordering is written in as a
target with the known rebalance exception. They are in
`~/capsule-corp/bnaas-poc/gw2cto/08-asks-from-product.md`. And one finding to
carry into the design from this model: the size of the uncommitted window on a
shared topic (act 06), with its two mitigations.

## Reference run totals

| act | duration | result | wedge cures |
|---|---|---|---|
| 01-code-and-tests | 185 s | rows to read: unit suite: expected 1720 passing, 1 pending, measured 1757 passing, 1 pending, 5 failing | 0 |
| 02-legacy-baseline | 95 s | every row as expected | 0 |
| 03-dead-destination | 264 s | every row as expected | 0 |
| 04-switch-and-drain | 430 s | every row as expected | 0 |
| 05-crashes | 377 s | every row as expected | 0 |
| 06-workgroups | 540 s | every row as expected | 0 |
| 07-kerberos | 437 s | every row as expected | 0 |
| 08-semantics | 570 s | every row as expected | 0 |
| total | 48 min 18 s | | |

## Rehearsal and reruns

**The take, as it will be recorded**: `DEMO_ACTS=02,03,04,05,06,08 yarn
ft_test:demo`, fresh clone at 75a60958, 2026-09-11 11:57:57 to 12:36:14 CEST,
**38 min 17 s** wall, 12 of 12 mocha cases passing, every verdict row as
expected, zero wedge cures. Acts 01 (the code, 3 min) and 07 (Kerberos, 7 min,
needs the krb profile) are the two left out to fit the window; add either with
`DEMO_ACTS=01,02,...` when there is time.

| act | duration | result |
|---|---|---|
| 02 legacy-baseline | 82 s | 25 of 25, 0 lost, 0 duplicated, 0 inverted |
| 03 dead-destination | 264 s | 20 drops each with its reason, healthy 5 of 5, lag 0 once the deadline passed |
| 04 switch-and-drain (the migration run) | 431 s | 0 lost, 0 inverted, the processors' uncommitted windows as the only duplicates, pause about 20 s, the stalled backlog delivered |
| 05 crashes | 379 s | 0 lost on two populator kills and a worker kill |
| 06 workgroups | 531 s | mapping, isolation, pin, two seeded layout changes, 0 lost, 0 inverted, pauses about 20 s |
| 08 semantics | 569 s | detach 0 of 20 on both paths, fan-out yes on both, collision 12 of 12 |

**Reruns and samples the same day, before the consumer fix** (branch at
4ab210fb, quiet machine after the Docker restart unless noted): act 04 429 s
with 30 duplicates (10 for the running processor, 20 for the frozen one, 0 for
the one stopped 47 s earlier); act 04 without the watermark 498 s, 258
duplicates against 30, 0 lost, pause 21 s; act 05 424 s, 0 lost, worker window
25 of 47, first delivery 85 s after the load began with 1 wedge cure; act 06
1178 s on the pressured VM (12 wedge cures, pauses 191 s and 206 s) and 941 s
on the quiet one (13 cures, pauses 96 s each, of which 91 s were the cures);
act 08 1368 s with its overlap case silent on both consumers (the wedge). The
same acts after the fix are in the table above and in the reference run.

## Timing at a glance

Measured, ACT START to ACT END, at `DEMO_PACE=demo`, 2026-09-11. "Post-fix" is
the consumer fix `c27b0f28` (zero wedge cures in every run since); "pre-fix" is
the same morning before it, with the wedge cures that stretched every consumer
start.

| act | what | post-fix, reference run | post-fix, the take | pre-fix samples |
|---|---|---|---|---|
| 01 | code + unit + lint | 185 s | not in the take | 62 s on 2026-09-10 (unit suite alone was 60 s) |
| 02 | legacy baseline | 95 s | 82 s | 128 s |
| 03 | dead destination | 264 s | 264 s | 338 s (2 cures) |
| 04 | the migration run | 430 s | 431 s | 429 s, 498 s without the watermark |
| 05 | crashes | 377 s | 379 s | 424 s (1 cure), 628 s (2 cures) |
| 06 | workgroups | 540 s | 531 s | 941 s (13 cures), 1178 s (12 cures) |
| 07 | kerberos | 437 s | not in the take | 434 s |
| 08 | semantics | 570 s | 569 s | 1368 s (overlap case silent, 2 cures) |

The whole suite measured **48 min 18 s** in the reference run (15 mocha cases
passing). The take, `DEMO_ACTS=02,03,04,05,06,08`, measured **38 min 17 s**,
inside the 30 to 40 minute window; with act 07 added it is about 45 min, with
act 01 as well about 48 min.

## Troubleshooting

- **"another demo run is already driving this stack"**: the single-run lock.
  Another `ft_test:demo` is live against this stack; wait for it. A lock left
  by a dead run is taken over automatically. Runs at different `PORT_OFFSET`
  values do not collide. The lock lives in `poc-demo/run/` of the checkout that
  runs, so two different checkouts against one broker are not protected from
  each other: run from one.
- **`WEDGE SUSPECTED`**: the pre-existing consumer defect above. The suite
  cures it. If you drive by hand, restart that one consumer and expect up to
  two minutes before it delivers again.
- **A Grafana panel is empty while the log shows deliveries**: a restarted
  worker can lose the race for its own probe port; the suite waits for the
  probe and restarts once if it never answers. Delivery is unaffected, the
  worker is just unscrapeable.
- **Leftover processes after a Ctrl-C**: the suite stops its children on
  teardown, but a hard interrupt can orphan one. Find them by the shim path,
  never by the task name (the older rig runs the same task names):
  `pgrep -fl 'poc-demo/conf/shims'`, then stop those pids. Do not touch the
  `bnaaskrb-*`, `ft-*` or `f9-mongo` containers, or processes whose command
  line carries `bnaas-poc/rig/conf`.
- **Stale consumer groups**: the suite recreates its topics and deletes its
  groups before act 1; a fresh legacy group is then seeded at the head before
  its processor starts. If you drive by hand, give any ad-hoc console consumer
  its own throwaway group, and expect a brand-new group to skip what lands
  during its first rebalance unless you seed it.
- **A half-created topic in ZooKeeper**: earlier act 03 runs asked a
  single-broker cluster for a replica on broker 99; the request never
  completes, the CLI exits 0, and `/brokers/topics/customer-topic-leaderless`
  is left behind with no leader. The act no longer asks (cf046aae). To remove
  one left by an earlier run:
  `docker exec bnaasdemo-zookeeper-1 zkCli.sh -server localhost:2181 deleteall /brokers/topics/customer-topic-leaderless`.
- **Port collision on `yarn demo:up`**: it refuses and names the owner. Raise
  `PORT_OFFSET` in `poc-demo/.env` and re-run; every derived port follows.
  `yarn demo:preflight` flags this before you start.
- **The mongo replica-set member**: mongod runs on its own offset port with a
  matching member host, because the `ci-mongodb` image pins the member to
  `127.0.0.1:27018`, which belongs to another workstream on the build machine.
  Do not remap it by hand.
- **macOS sleep** rebalances the groups mid-act. `caffeinate -dimsu`.
- **`yarn install` fails on `fcntl`**: a transitive native module the demo
  never uses; node-gyp wants a python with distutils (removed in 3.12).
  `yarn demo:install` makes a setuptools venv and points node-gyp at it
  (verified 22:46 CEST on python 3.14: install completed, node-rdkafka loads).
- **`yarn demo:install` fails with `tsc: command not found`**: the install ran
  under a node that is not 22 (this happened under node 24 on 2026-09-11 at
  07:30). `scubaclient`'s build step needs the `tsc` that node 22's global bin
  carries here. Put node 22 first on PATH (`nvm use 22`) and run it again;
  `yarn demo:preflight` names the node on PATH.
- **The broker vanished mid-run** (`cannot find the kafka CLI inside
  bnaasdemo-kafka-1`, or every consumer disconnects at once): check
  `docker inspect bnaasdemo-kafka-1 --format '{{.State.OOMKilled}}'`. On
  2026-09-11 at 06:52:24 UTC the Docker VM's kernel OOM-killed the broker
  (exit 137) while three Kafka brokers shared its 7.75 GiB. Bring it back with
  `docker start bnaasdemo-kafka-1` (healthy in about 15 s, topics and offsets
  intact, the runs that failed at setup are simply rerun), and give the demo
  the machine as the first pre-take step says. Raising the Docker engine's
  memory is the durable fix.
- **`yarn demo:up` right after a Docker engine restart fails with "dependency
  kafka failed to start"**: ZooKeeper still holds the broker's ephemeral
  `/brokers/ids` node from before the restart (NodeExists). Wait about 20 s and
  run `yarn demo:up` again (seen 2026-09-11 09:56 after the engine's memory
  was raised).
- **Host load spikes while a run writes its evidence**: Spotlight indexes the
  customer-topic dumps. `poc-demo/evidence/` and `poc-demo/logs/` carry a
  `.metadata_never_index` file since 2026-09-11; if the load average climbs on
  `mdworker` processes, check the file is there.

## Teardown

```bash
yarn demo:down       # stop and remove the stack's containers; named volumes stay
# to wipe the volumes too (kafka, mongo, grafana, zookeeper data):
docker compose --project-name bnaasdemo --file poc-demo/docker-compose.yaml --profile krb down -v
```

Not executed in the 2026-09-10 verification: the stack was left up for the
recording. Evidence from a run is under `poc-demo/evidence/<act>/`
(gitignored): `verdict.txt`, `timeline.txt`, the driver and process logs, and
the customer-topic dumps. The UI captures are under
`poc-demo/evidence/screenshots/`.
