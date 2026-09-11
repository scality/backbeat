<!-- markdownlint-disable MD013 -->
# GW2CTO document: Bucket Notifications as a Service (BNaaS)

> Draft 4, 2026-09-11, Anurag Mittal (with AI assistance), for review with Taylor McKinnon before the extended reviewer list. Follows the Standard Gateway to CTO template: it links to requirements and design, does not restate them, and every number points to a measurement. Reading rule for the design PR: where it says "Go V2 worker", read "the same queue processor, in Node, handling multiple destinations"; the wording fixes are listed in `01-design-critique.md` §5.

- Requirements: [Citadel PR 373](https://github.com/scality/citadel/pull/373) (David Tencer)
- Design: [Citadel PR 383](https://github.com/scality/citadel/pull/383)
- Jira: epic [S3C-9705](https://scality.atlassian.net/browse/S3C-9705) (fix version 10.X), PoC [OS-1147](https://scality.atlassian.net/browse/OS-1147), this document [OS-1148](https://scality.atlassian.net/browse/OS-1148)

## People involved

- Engineers and Architects: Anurag Mittal, Taylor McKinnon
- Product Owner: David Tencer
- Engineering Lead: Nicolas Humbert
- Design reviewer so far: François Ferrand (predecessor PR 364)
- Senior VP Customer Success and Senior VP Engineering: Pierre Derome
- Chief Product Officer: Erwan Girard
- Engineering Team: Object Squad

# Context and background

Service providers (Core42, Orange NAOS, Allstate at ~250 destinations) run RING + S3C as a multitenant platform. A bucket notification needs a destination, a customer Kafka endpoint plus credentials. Today a destination is a Federation inventory entry realized as one OS process per destination: only the platform administrator can create one, every change restarts the notification containers cluster-wide, and every destination is usable by every account. Three limitations, one cause: a destination is a deployment artifact, not a resource.

What "today" actually does, measured (July 2026 scale test on a 6-node lab; 2026-09-09 local rig on `development/9.3` code):

| Fact | Measurement |
|---|---|
| One destination | 12 processes x ~78 MB = ~936 MB RAM, traffic or not; 11 of 12 are idle standbys |
| Ceiling on 6 nodes of 30 GB | ~56 destinations safe, 64 edge, 80 hard failure; 10,000 would need ~9.4 TB |
| Adding one destination | 8 to 22 min of playbook, ~121 s delivery stall for every destination, S3 API restart; two static lists to edit; a populator that predates the destination skips its events silently |
| Dead destination | the processor cannot start if the destination is down; a running one stalls that destination indefinitely, offsets never advance, no counter, nothing written to the configured failed topic |
| Detaching a destination | queued events for it are dropped silently (20 of 20) |
| Tenant isolation | the ARN's account field is never read; `arn:scality:bucketnotif::123456789012:poc-dest-1` was accepted with HTTP 200 and its 12 events landed on the global `poc-dest-1` |
| Kerberos | accepted by configuration, broken in the shipped image since 2024 (BB-828 fixes it) |

## Requirements and capabilities

Detailed requirements: Citadel PR 373. The ones that shape the design:

| ID | Requirement | Target |
|---|---|---|
| F1 | Self-service destination CRUD, AuthV4 and IAM, account-scoped | no administrator, no restart, effective within 5 min |
| F2 | Tenant isolation | no account can use, list or see another's destination |
| F3 | Validation | destination tested before create and update are accepted |
| F4 | Global (legacy) destinations keep working | coexistence on one bucket |
| F5 | Same events, message shape, rules, Kafka only, same auth (Kerberos for global destinations only in v1) | byte-compatible event JSON |
| NF1 | Scale | 10,000 destinations system-wide; adding one is never manual |
| NF2 | Delivery isolation | a faulty destination must not affect others |
| NF3 | Delivery SLA | at-least-once best-effort; retry then abandon with a logged cause; duplicates allowed |
| NF4 | Security | credentials encrypted at rest; key readable only by the decrypting process |
| NF5 | Operations | Federation flags, seamless upgrade, health checks, KPIs, L3 dashboard, alerts, audit logs |
| NF6 | ARTESCA | backbeat stays backward compatible |

# Proposal and general architecture

### Solution overview

- Control plane (API side, lower risk): CloudServer routes for `Create/Get/Update/Delete/List` destination, `scality:` IAM actions, ARN `arn:scality:bucketnotif::<accountId>:<name>` (the live convention for utapi, metadata and sur; accepted by today's parser). Records in a dedicated MetaData (bucketd) cluster owned by CloudServer; credentials encrypted per record (AES-256-GCM envelope), master key held by CloudServer only. Workers fetch a record through an internal CloudServer route on cache miss (30 to 60 s TTL, stale-while-revalidate): a control-plane outage costs freshness, never deliveries.
- Data plane (the risky half): the existing backbeat populators keep writing one record per event to today's internal topic, unchanged. A fixed pool of backbeat delivery workers replaces the per-destination processors: each worker reads that topic, matches every event against the bucket's rules the way a processor does today, keeps the destinations in its workgroup's slice, and delivers through pooled producers (one per endpoint and credential, idle-reaped). Destination count no longer drives process count. Decided 2026-09-11: workers read today's topic; the destination-keyed delivery topic the POC built is dropped (see decision 2 and History).
- Distribution: workers placed by the deployment layer (2 per node by default), Kafka assigns partitions. Workgroups are consumer groups over today's topic, each delivering its slice of the destinations (hash of the destination name over the auto workgroups, plus pins for global or heavy destinations) and skipping the rest, to cap producer connections per worker and bound blast radius. Account-scoped destinations added at runtime hash into a workgroup with no configuration change and no restart; only a layout change (number of auto workgroups, a pin) is an Ansible run.
- Configuration: one Federation flag (`env_bucket_notifications.delivery_pool`); flag off renders byte-identical configuration to today.

### Key design decisions

1. Stay in backbeat, in Node (2026-07-24). The wall is the process-per-destination model, not the runtime: one Node process holds destinations at ~0.75 MB each, measured. The Go rewrite is recorded as rejected.
2. Workers read today's topic and match per destination themselves (2026-09-11, supersedes "address every record at publish time"). The POC measured the destination-keyed delivery topic (one addressed record per matching destination, one destination per partition); it works, but it forces a populator change, a second topic and a drain at the switch, none of which fit a deployment that replaces every container in one Ansible run. Fan-out to every matching destination is preserved either way (measured on both paths). Cost of the change: isolation of a dead destination rests on the delivery deadline and drop instead of partition ownership (to be re-measured, one run), and the spread factor becomes unnecessary.
3. Kafka-owned assignment, zero coordination code of our own. Per-record commit on terminal resolution, contiguous-offset ledger, per-object ordering by construction of the record key.
4. Visible failure. Every drop is counted, reason-labelled and alertable; a configurable send deadline (30 s default) replaces today's silent indefinite stall.
5. Control plane on CloudServer and a dedicated bucketd cluster, not a new service (ARR parked), not Vault (single raft session), not SQL.
6. Migration is one Ansible run and one way (2026-09-11, supersedes the drain-then-switch procedure): the populator is untouched; Ansible deletes the processor containers and starts the worker containers; each worker group is seeded before its first fetch from the committed offsets of the processors it replaces (lowest per partition), with each destination's own processor offset kept as a watermark so nothing is delivered twice and a stalled destination finally receives its backlog. Delivery pauses for the container swap plus the group join (45 to 60 s measured). There is no return to per-destination processors; manual workgroups can reproduce today's isolation if ever needed. A workgroup layout change later is the same run with the groups seeded from the previous generation.

### POC evidence

Branch `poc/S3C-11127-workgroups` (scality/backbeat, +16.7k lines on `development/9.3`), Federation roles on `improvement/S3C-11127-delivery-pool-poc`, working notes `~/capsule-corp/bnaas-poc/`. Every row below ran against real Kafka, ZooKeeper and backbeat code; the migration rows against real S3 PUTs through CloudServer.

| What | Result |
|---|---|
| Density and cost (July) | ~0.75 MB per destination pooled versus ~936 MB today; 10,000 destinations in ~10 GB cluster-wide |
| Slice enforcement, 3 workgroups | every record delivered by exactly one workgroup, 5 runs |
| Isolation | a blackholed destination held only its own workgroup's offsets (31.5 s) while the others drained at p99 0.25 s; 60 counted drops across three dead destinations while a healthy one delivered 5 of 5 |
| Workgroup cutover and reshard | 0 gaps; duplicates = the old generation's post-barrier consumption (21 of 72; 30 of 108); loss only when the old generation is stopped before the drain report reaches zero (393 of 600 when done deliberately, 0 unwarned) |
| Legacy-to-pool cutover under load, parked backlog (M2) | 1168 of 1168, 0 gaps, 0 duplicates, 0 per-key inversions; legacy offsets untouched |
| Rollback on the drainer path (M4) | 106 re-delivered (exactly the drained records), 161 stranded until roll-forward, which recovers them out of order; no reverse drainer exists |
| The design's drain-then-switch path and its mirror rollback (M2b, M4b) | cutover 0 of 601 lost, 69 duplicates from the worker's first-join replay (avoided by starting the worker before the switch), rollback 0 lost, 0 duplicates, 0 inversions; no tool, 5 operator steps |
| Drain-then-switch done wrong, pool started before legacy drained (M2c) | 0 lost, 284 same-key inversions on 3 keys: the reorder the barrier exists to prevent; ordering, not data, is what the barrier protects |
| Legacy and pool running together (M5) | 657 of 657, 0 duplicates: the two paths partition the stream |
| Crashes (M11, M12) | populator `kill -9` twice: 0 gaps, 0 duplicates; worker `kill -9`: 0 gaps, 89 duplicates from a 125-record uncommitted window |
| Chaos round (C1 to C5r) | 17,340 events, 0 lost, across member death, scale-out, partition growth, a 10-minute random-kill loop and reshards with an unhealthy member. Costs, all from the shared consumer library: pauses of 165 to 877 s (an uncaught throw on the offset-store path crashed a process 18 times, each crash restarting the rebalance), 1,100 duplicates of 3,290 in the loop, 52 inversions on member death and 6,480 when partitions were grown under load (0 with the safe procedure) |
| Downgrade (M6, M7) | unknown ARN skipped silently; name collision misroutes through the API (above); an unparseable configuration bricks the bucket (500) and crash-loops the populator for every tenant; legacy processor tolerates addressed records |
| Semantics evidence (M8, M9) | overlapping rules fan out on both paths (FR10 as drafted would be a silent change); detach drops queued events on legacy, delivers them on the pool |
| GATE 2, two Kerberos principals in one process | node-rdkafka: one identity per process; `DIR:`, `sasl.username` and `kswitch` all fail (losing producer 0 of 9). KafkaJS with a GSSAPI mechanism over the `kerberos` binding: 2 principals 9/9 and 9/9, 50 principals 250/250, three broker restarts and 2-minute ticket expiry survived, 82 MB RSS and 11 threads at 50 identities standalone, 140 to 146 MB through the delivery pool; landed in backbeat on `poc/S3C-11127-kerberos-producer` (nine commits, unit and functional suites, node-rdkafka stays the default) |
| Demo suite on the decided model (today's topic, container swaps), fresh clone, fixed consumer, 2026-09-11 | eight acts green in 48.4 min, recording cut (02, 03, 04, 05, 06, 08) green in 37.7 min, zero wedge cures. Migration swap: 0 lost, 0 inversions, 2 duplicates, 21 s pause, 163 skipped by the watermark, stalled backlog of 280 delivered (258 duplicates without the watermark). Layout changes: 19 s each (5 s seed, 14 s start and join), 0 lost, 0 inversions. Isolation on shared partitions: 20 drops per dead destination with the reason, healthy 5 of 5, commit lag back to 0 at 31 s. Crashes: 0 lost, checkpoint window 135, worker window 19 of 42, 45 s failover. Semantics: detach drops on both paths (0 of 20), fan-out on both, collision 12 of 12 |
| Previous model (destination-keyed delivery topic), 2026-09-10 | 82.5 min reference run; legacy baseline 25/25 0/0/0; dead destinations 20 drops each, healthy 5/5; cutover 0/0/0 and rollback 0/0/0; worker `kill -9` 0 lost, 49 duplicates in a 67-record window; two Kerberos identities in one process, 8 arms |
| Generation change started on a lagging old generation (demo act 06) | 0 lost, 4,197 same-key inversion pairs and 877 duplicates on the one destination with several operations per key: the new generation delivered post-barrier records while the old one was 916 records behind its barrier. The worker has no hold; the ordering rule ("do not start the new generation until verify exits 0") is printed by the CLI and enforced by nobody. 0 inversions in every run that respected it (M2c inside the workgroup protocol) |
| Ordered-lane throughput | one record per 2.00 s per active object key (`BackbeatProducer` poll interval 2000 ms releases the next record of a lane); a six-key destination drains at 3 records/s regardless of `concurrency: 1000` |
| Populator checkpoint window | a populator killed mid-batch republishes the window (105 to 133 duplicates, 0 lost); the same window at the legacy-to-pool switch (129 duplicates, populators never overlapped) |
| Contiguous-commit duplicate window on today's topic (demo act 06, fixed consumer) | 952 duplicates across two generation stops, none from the seed: one slow object lane (one record per 2 s poll) holds a shared partition's committed offset back; any stop re-delivers everything since the oldest in-flight record. Mitigations: delivery-report-driven lane release; graceful-stop per-destination watermarks |
| Paused partitions not resumed after a coordinator disconnect (demo act 06, loaded host, pre-fix) | 36 s coordinator request timeout, `Local: Erroneous state` from `_resumePausedPartitions`, two of three partitions paused for 37 min with the member alive; the state-read guards (63d79adf) make this a warning and duplicates rather than a silent stall |

Full write-ups: `rig/RESULTS.md` (including a side-by-side of the two migration paths), `krb-spike/RESULTS.md`.

### Delivery flow

1. An S3 request mutates an object; the metadata log entry reaches a populator (one tail loop per raft session, spread over the populator processes).
2. The populator evaluates the bucket's rules and publishes one record `{event, destinationId, configurationId}` per matching destination, keyed by destination (optionally spread over m sub-keys for a hot destination).
3. A worker in the owning workgroup consumes, resolves the destination record (cache, else the internal route), sends through the pooled producer with a deadline.
4. Delivery report: terminal. Drop after the deadline or on a permanent error: terminal and counted with a reason. The ledger commits the contiguous prefix.
5. A destination created through the API delivers within one TTL; deleted, it drops with `destination_missing` within one TTL.

### Storage

- Destination record `accountId::bucket-notification-destination::name` in an internal bucket of the dedicated cluster; `Auth{}` as `{cryptoScheme, KeyId, wrappedDEK, IV, ciphertext, tag}`; additive evolution only. Precedent for credentials at rest: Arsenal's RSA-OAEP `decryptSecret` (Zenko overlay); symmetric here because records are small and rotation re-wraps the DEK.
- Delivery topic: RF 3, `acks=all`, `min.insync.replicas=2`, hours of retention plus a size cap.
- Workgroup document in ZooKeeper (generation, hashmod slices, pins, barrier offsets); amendment proposed: one node per generation plus a current pointer.

## Research and experimentation

Five delivery architectures were developed to one template and each attacked by an adversarial review (19 defects fixed on paper; leads report 2026-08-05, Confluence 4223533061).

- Shared stream with in-memory dispatch: rejected, any worker may need every destination, adding workers makes it worse.
- Own assignment map in ZooKeeper: rejected, 12x to 40x read tax and three silent-loss sequences in a hand-written protocol; kept as fallback.
- One topic per destination: rejected as default, 30,000 partition replicas on 6 brokers shared with CRR and lifecycle; the escape hatch for an extreme tenant.
- Fixed shards with a move protocol: rejected, five protocol defects.
- Go rewrite: rejected 2026-07-24, two engines forever and a proven consumer to reproduce.
- Per-workgroup topics with routing in the populator (design PR §7a) versus one shared topic with a consumer-side slice filter: decided for the one topic (2026-09-10). The former buys 1x reads at the cost of G topics, populator routing and the one-event-one-destination rule; the latter pays G-fold reads (a hash and a commit per skipped record, no I/O) for one topic, an untouched populator and today's fan-out kept. The POC built and measured the latter. The assume-destination submode that per-workgroup topics required was explored and set aside: global destinations already flow through the pool by stamp and static configuration.

## Operations: what the operator does, in one place

Everything an operator does is a Federation run (`run.yml`) that renders the configuration and replaces the containers. No manual drain, no live cutover, no second procedure.

| Situation | What the operator does | What happens, measured | Cost the operator sees |
|---|---|---|---|
| Migration from per-destination processors to the pool | One `run.yml` with the pool enabled. The run stops the processors (and restarts the populator, unchanged) and starts the workers; the first worker of each workgroup to start seeds the group itself from the processors' committed offsets, with a per-destination watermark, under a ZooKeeper lock (`deliveryPool.seedOnStart`, default on; the CLI remains for seeding ahead) | 0 lost, 0 reordered, 13 duplicates (the processors' uncommitted windows), the stalled destination's 280-record backlog delivered, 151 already-delivered records skipped, seeding 3 s after the switch (demo act 04 with self-seeding, 2026-09-11) | delivery pauses about 20 s; the populator republishes its last checkpoint batch once |
| Workgroup layout change (number of auto workgroups, a pin for a global or heavy destination) | Edit the configuration group, `run.yml`. The run stops the old generation and starts the new one; one worker of the new generation seeds it from the previous generation | 0 lost, 0 reordered, 21 s pause per change, seeding 4 s after the layout was written (demo act 06 with self-seeding) | duplicates equal to the stopped generation's uncommitted window, hundreds on a shared partition with a slow lane (see limitations) |
| New account-scoped destination | nothing: it hashes into a workgroup by name | delivered by its slice within the delivery deadline | none |
| Global destination change (endpoint, credentials, principal) | configuration group, `run.yml` | same as a layout change | same |
| Populator crash | nothing: the supervisor restarts it | 0 lost; the last checkpoint batch is republished (105 to 137 records measured) | duplicates once |
| Worker crash | nothing: the container restarts and rejoins | 0 lost; its uncommitted window is re-delivered (19 of 42, 25 of 47 measured); its partitions wait for the 45 s session timeout, then move | 45 s pause on that worker's partitions, bounded duplicates |
| Dead or refusing destination | nothing to keep the others healthy; fix the destination | drops counted with a reason after the 30 s deadline; the other destinations unaffected (5 of 5); commit lag returns to 0 within the deadline | drop counter and alert for that destination |
| Broker restart, coordinator disconnect | nothing | reconnect; guarded state reads turn a disconnected consumer into warnings and duplicates, not a crash | brief pause, duplicates |
| Bad worker release | forward-fix: a new image and `run.yml`; there is no return to per-destination processors | canary: pin one or two destinations to a workgroup on the new image first | the canary's blast radius only |
| Unparseable bucket configuration | fix the bucket's configuration | today: bucket returns 500 and the populator crash-loops for every tenant (M6c); schema changes must stay additive | outage for every tenant until fixed |

What changes for the operator compared with today: one configuration group for the pool instead of one process definition per destination; adding a destination is no longer a playbook; the two dashboards (delivery pool, workgroups) and the drop counter per destination replace watching a processor that silently stalls; the seeding happens inside the worker at first start, so the run is a plain replace; there is no rollback procedure, by decision. The full procedure as Federation runs it is `poc-demo/OPERATOR.md` on the demo branch.

## Known limitations

- Duplicates, bounded and measured: the populator's batch between Kafka ack and checkpoint (rare, up to a whole batch), the worker's 5 s auto-commit window, and at a workgroup cutover the old generation's post-barrier consumption. Consumers de-duplicate, as the requirements say; `s3.object.sequencer` is the right key and is null today.
- Isolation is bounded, not preserved: a worker crash takes every destination on it for the restart time; workgroups bound it to one slice.
- One destination is one delivery partition unless `spreadFactor` is set; a hot destination is sized through `spreadFactor` (CRC32 makes m sub-keys give fewer than m partitions; a guaranteed-m table is in the runbook). Partitions grow, never shrink.
- Detach is not a revocation: on today's topic the worker matches at delivery time like the processors, so a detached destination's queued events are dropped on both paths (measured 0 of 20); delete stops it within one TTL. Same as today; release-note material only if the addressed-topic behaviour had been promised.
- Kerberos: one identity per OS process with librdkafka (measured). Decided 2026-09-11: v1 ships node-rdkafka only, and every Kerberos destination is pinned to its own workgroup, so it runs in its own process (Kerberos is global-only in v1, so this is a handful of processes). For account-scoped Kerberos later, the client comparison of 2026-09-11 (`bnaas-poc/krb-libs/RESULTS.md`, same rig and arms for every candidate) settles the choice: `@platformatic/kafka` 2.11.0 (pure JavaScript, Apache-2.0, forty releases in 2026) with our GSSAPI mechanism in its `sasl.authenticate` hook passes every arm: two principals 9/9 and 9/9 under ACLs, keytab with no kinit, three broker restarts, two-minute tickets reacquired per principal, 50 principals in one process (250/250, 116 MB, 11 threads), gzip, snappy, lz4 and zstd; the producer is implemented through `DeliveryProducerPool` on branch `poc/S3C-11127-kerberos-platformatic` (functional suite 8 passing). The KafkaJS producer (branch `poc/S3C-11127-kerberos-producer`) is the same mechanism on an unmaintained client (2.2.4, February 2023) and stays as reference. A 41-line librdkafka patch adding a per-client `sasl.kerberos.ccache` also works (9/9 and 9/9, the control without it collides) and is worth proposing upstream; Confluent's JavaScript client ships no GSSAPI provider at all. Caveats: platformatic is ESM-only (Node 22.12+), pins ajv 8 against backbeat's ajv 6, and about 40 MB heavier than KafkaJS; one first-acquisition race in ten runs when twenty producers start into one credential collection at once (serialize or retry the first acquisition). The one-identity limit was re-verified side by side on ubuntu 24.04 and Debian bookworm (same libsasl2 2.1.28 and krb5 1.20.1 upstream versions as the shipped bookworm-slim image): librdkafka passes GSS_C_NO_CREDENTIAL, so each handshake uses the last kinit's principal; a CI run that saw two identities was a race at the first handshake. Not tested on any path: Active Directory, cross-realm, SASL over TLS, Kafka 4.x brokers.
- Records are installation-scoped; DR-paired rings need a dual-create runbook and the same key file.
- The event JSON is preserved byte for byte, including a known `eventTime: null` defect; fixing it is a decision.
- Per-key ordering is a property of `BackbeatConsumer`, shared by legacy and pool. It holds in steady state and across planned cutovers (0 inversions in every such run) and breaks under rebalance churn: 52 inversions when a member of a three-worker group was killed, 6,480 when partitions were grown while the populator wrote (0 with the safe procedure). The mechanism is a revoke abandoning a half-delivered batch; it is present today and is fixed by the same groundwork as the crashes. Until then the claim is "per-object ordering, except during rebalances".
- Every layout change and the migration itself are container swaps: delivery pauses for the swap plus the group join (45 to 60 s measured); a bounded window may be delivered twice unless the per-destination watermark is implemented.
- A destination with few hot object keys delivers at one record per 2 s per key (producer poll interval), whatever the concurrency setting.

## Risks

| Risk | Impact | Mitigation |
|---|---|---|
| Account-scoped ARNs whose name equals a global destination's are delivered to the global destination by every legacy component (account field ignored); reproduced end to end | silent cross-tenant misrouting in any mixed-version window | naming rule keeping the namespaces disjoint, or a legacy patch requiring an empty account field; threat-model row |
| The design PR describes per-workgroup topics with routing in the populator, a sibling of the mechanism that was built and measured | reviewers argue about the wrong design | converge on the built mechanism (one destination-keyed topic, slice filter, barrier cutover, fan-out kept) or state the trade explicitly (`01-design-critique.md` §5) |
| Reliability posture wording (drop) versus requirements (retry then abandon) | requirement mismatch at the soutenance | state the configurable deadline in the requirements' words; DLQ later |
| Validation semantics (opt-in test vs mandatory, no test API) | same | decide with product; make the infrastructure-as-code argument explicitly |
| Synchronous test dial on the S3 request path (CloudServer makes no outbound call today) | PUT latency tied to a customer broker; SSRF surface | 10 s deadline, parallel dials, dial-time validation against the deployment's own addresses, skip flag |
| Service-root query routes have no precedent (`GET /?x` returns a bucket list, `POST /` returns HTTP 200 NotImplemented on an old node) | confusing rolling-upgrade failures | Arsenal route change; auth modelled on `routeMetadata`; never the "any authenticated user" shortcut |
| Rollback on the drainer path strands the pool's uncommitted records and re-delivers the drained ones (161 and 106 measured) | loss until roll-forward, then reorder | drain-then-switch as the default (rollback measured at 0/0/0); the drainer only for a processor that cannot drain; write the reverse drainer before the first customer cutover |
| Crashed old-generation worker cannot resume its drain (E4) | stuck drain | one ZooKeeper node per generation plus a current pointer |
| Pre-existing shared-consumer defects, measured in the chaos round: (1) an uncaught `offsetsStore` throw on ordinary rebalances (`BackbeatConsumer.js:851`) crashed a process 18 times, and every crash restarts the rebalance, so a member change pauses delivery for minutes (165 to 877 s measured) instead of the 45 s session timeout; (2) the lost-callback wedge leaves a consumer assigned, healthy to the probe and deaf (6 windows in the loop, up to 347 s); (3) the 2023 `metadata.max.age.ms` misconfiguration rebalances a joining consumer every 1 to 2 s; (4) `BackbeatProducer` never refreshes topic metadata, so growing partitions under load reorders for 257 s | multi-minute pauses, one delivery in four repeated under churn, ordering breaks; all of it affects CRR today | fix the throw first, then switch to cooperative-sticky assignment in one step, then a stall detector that fails readiness; the one-line refresh fix; a producer metadata refresh interval; alert on delivery rate, never on membership; all as groundwork tickets Two of them (the start-up wedge and the uncaught state-read throws) were root-caused and fixed on the POC branch on 2026-09-11; the chaos numbers are pre-fix. |
| Unparseable bucket configuration | bucket bricked at the S3 API, populator crash loop for all tenants | additive fields only, never an optional `queueArn` |
| Message version field `schemaVersion` collides with a live field | ambiguous versioning | different name |
| Producer fan-out per worker at 10,000 fully distinct endpoints | thread and connection budgets (estimate ~40 workers x ~250 destinations) | workgroups, producer pooling, density gate on a lab |
| First platform-wide key rotation | operational novelty | tiny enumerable dataset; canary decrypt in DR rehearsals; external KMS additive later |
| Per-connection Kerberos needs a client that holds several identities in one process | the only mature pure-JavaScript client (KafkaJS) is unmaintained since 2023 | not in v1 (Kerberos destinations pinned one per workgroup on node-rdkafka); for account-scoped Kerberos ship the `@platformatic/kafka` producer already measured and on its branch (maintained, all arms pass), and propose the 41-line librdkafka patch upstream in parallel |
| Workgroup generation change: the built cutover pre-seeds the new generation at the barrier, which is safe only if the old generation drained to the barrier before stopping; production applies layout changes through Ansible, which restarts every worker without waiting (live overlap, measured at 4,197 same-key inversion pairs, cannot happen there) | an undrained backlog before the barrier would be skipped by the new generation (loss) | seed the new generation from the previous groups' committed offsets (the worker treats "behind the barrier" as duplicates), drop the barrier record, accept the restart pause (50 to 176 s measured); rollback = the same procedure with the previous layout as the next generation |
| Per-object ordering lanes advance one record per producer poll (2 s) | hot objects cap at 0.5 events/s each; slow drains widen every cutover window | shorter poll or delivery-report driven release in `BackbeatProducer`; measure before sizing |

### Open questions

- G default; grown online or static.
- Per-account quota: out of scope in the requirements; an ops-configurable cap is wanted as a support lever.
- Ordering is absent from the requirements; per-object ordering must be written in.
- Zenko shape and timing; when to turn on the deferred-delivery buffer.
- Account-scoped Kerberos: offered only as pinned, operator-provisioned workgroups (one process per principal), or not at all? Product decision.

## Delivery in phases

1. Groundwork: two shared-consumer fixes are done on the POC branch and should ship to product on their own tickets: the start-up wedge (application metadata requests asked for all topics, flapping the subscription until the 45 s session timeout; fixed with `allTopics: false`, c27b0f28) and the unguarded consumer state reads that threw out of task callbacks (`subscription()`, `assignments()`, `offsetsStore()`; guarded, 63d79adf). Still to do: cooperative-sticky assignment, the 2023 metadata-refresh misconfiguration, the producer's topic metadata refresh interval, node-rdkafka 3.2+ repo-wide with regression across CRR, lifecycle and ingestion, Kafka message-format unpin (S3C-11389), delivery-report-driven lane release in the producer.
2. v1 data plane: worker pool on today's topic with per-destination matching, seeded group start with per-destination watermarks, workgroups (auto slices plus pins, applied by Ansible), metrics and alerts, Federation roles; processors replaced per deployment in the same run.
3. v1 control plane: CloudServer routes, dedicated bucketd cluster, encryption, IAM actions, attach-time account check and test.
4. Post-v1: deferred-delivery buffer, tenant delivery-health API, record replication for DR pairs, account-scoped Kerberos (the `@platformatic/kafka` producer, measured and on its branch; the librdkafka patch proposed upstream in parallel).

## Impact on users and customers

Positive: tenants create and rotate destinations themselves within one TTL; no cluster-wide restart per destination; ~0.75 MB instead of ~936 MB per destination; every failure visible within 30 s instead of a silent stall; adding a node adds ~500 to 600 destinations of capacity (estimate).

Operational: new public API and IAM actions; a new internal topic and a dedicated bucketd cluster to deploy and monitor; per-worker Prometheus targets; L3 dashboard and alerts; runbooks for partition growth, workgroup cutover, rollback, hot-destination spread, Kerberos pinning; a drain step in the upgrade procedure.

## History

- 2026-07 [S3C, 10.X]: destination-keyed delivery topic with addressed records, drain-then-switch migration, because the pool was designed as a live cutover beside the running processors.
- 2026-09-11 [S3C, 10.X]: workers read today's internal topic and match per destination; the destination-keyed delivery topic, the populator flag, the drainer and the drain-then-switch migration are dropped because deployment replaces every container in one Ansible run, so no two generations or paths ever run together. Migration and layout changes become seeded container swaps; rollback to processors is out.

## Links and references

- Design PR 383; requirements PR 373; predecessor PR 364 (closed).
- Leads report 2026-08-05: Confluence 4223533061; local `~/capsule-corp/bucket-notification-redesign/leads-report.md`.
- Scale test: `~/capsule-corp/bnaas-scale-test/report/checkpoints/2026-07-21_FINAL/`.
- POC branches: backbeat `poc/S3C-11127-workgroups`, `improvement/S3C-11127-notification-delivery-pool-poc`; federation `improvement/S3C-11127-delivery-pool-poc`.
- Evidence: `~/capsule-corp/bnaas-poc/design/06,07,09,11`; `rig/RESULTS.md` and `rig/evidence/`; `krb-spike/RESULTS.md`; code verification reports `gw2cto/reports/`.
- Critique, question bank and paste-ready PR comments: `gw2cto/01-design-critique.md`, `gw2cto/04-pr383-review-comments.md`.

# Actions post Gateway to CTO soutenance

- (to be filled during the meeting)
