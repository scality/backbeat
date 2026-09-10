<!-- markdownlint-disable MD013 -->

# BNaaS demo: the recording playbook

For whoever records the BNaaS (bucket notifications as a service) POC, and for
Taylor picking it up cold. Run it top to bottom: the ten commands that take a
fresh clone to a finished demo-pace run; then, act by act, what the terminal
prints, what to point at in Grafana, Kafka UI and ZooNavigator, the numbers to
expect, the caveats to say out loud, and how long each act takes; then
troubleshooting and teardown.

Every number below comes from one uninterrupted `yarn ft_test:demo` run from a
fresh clone of this branch on the build machine, started 2026-09-10 23:09:40
CEST (21:09:40 UTC; the suite's own log lines are UTC). Where a step in this
file was executed, its timestamp is given. Nothing else is stated as fact.

## The ten commands

From nothing to the demo. Each was executed on the build machine on
2026-09-10 (times CEST). The clone was `/tmp/bnaas-demo-clone`.

```bash
# 1. clone the demo branch and enter it                             (22:44:55)
git clone --branch poc/S3C-11127-demo git@github.com:scality/backbeat.git && cd backbeat

# 2. node 22 on PATH; 24 breaks the native modules. With nvm:      (node 22 was already installed here)
nvm install 22 && nvm use 22

# 3. install, with the one macOS/node-gyp workaround folded in      (22:46:19, 58 s)
yarn demo:install

# 4. check the machine: docker up, node 22, ports free, python OK   (22:47)
yarn demo:preflight

# 5. bring the stack up (builds the two local-only images on a first run).
#    Add :krb for act 07.                                           (see note below)
yarn demo:up          # or: yarn demo:up:krb

# 6. block until broker, mongo PRIMARY, CloudServer and Grafana answer   (22:49)
yarn demo:wait

# 7. the whole demo at recording pace (DEMO_PACE=demo is the default)    (23:09:40)
yarn ft_test:demo
```

Variants, not extra steps:

```bash
DEMO_ACTS=02,03,04,06 yarn ft_test:demo    # the four acts that carry the argument
DEMO_ACTS=04 yarn ft_test:demo             # one act, to reshoot it
DEMO_PACE=slow yarn ft_test:demo           # every wait doubled, for a careful take
yarn demo:down                             # teardown (see the last section)
```

Step 5 on the build machine: the stack was already up from earlier in the day,
so `yarn demo:up` was run against a running stack (it is idempotent: it reports
each image as present and each port as held by its own container). The
image-build path it carries was validated by `docker compose config` and by the
Dockerfiles it points at, not by a build from nothing, because both images
already existed here.

What can go wrong at each step is in Troubleshooting. `yarn demo:preflight` is
the one to trust on a new machine: it names anything that will block a run.

## Before the take

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

## The thing that will happen live: the consumer wedge

A consumer can become a live group member that holds all its partitions, cycles
`assign -> revoke -> "processing queue idle, un-assigning"` about once a second,
delivers nothing, and still answers its liveness probe with 200. It is a
measured, pre-existing `BackbeatConsumer` defect, it is not caused by the pool,
and in the reference run it fired on **every** pool worker start (acts 03, 04,
05 and 06) and on none of the legacy processors. The suite handles it: the
drain gates watch progress, not lag, print `WEDGE SUSPECTED`, restart that one
consumer, and wait for its first assignment. In the reference run a cure cost
between 90 s and 150 s each time: about 60 s to call the stall, a restart, and
the replacement's first join, which is revoked and re-assigned some 40 s later.

Say it on camera when the line appears. It is the reliability ceiling the POC
reports, the cure is what an operator would do, and the numbers that follow it
are still zero loss.

A second shape of the same defect showed once in the reference run, in act 06,
and is worth a line of its own: after a group-coordinator request timed out
(36 s, under host pressure) the consumer logged `KafkaConsumer is disconnected`
and `Local: Erroneous state` out of `_resumePausedPartitions`, and the
partitions it had paused for the rebalance were never resumed. The member
stayed alive, answered its probe, kept committing on the one partition it was
still reading (where it only filtered the other workgroup's records), and never
moved on the two partitions its own destinations hash to, for the rest of the
act. Total lag kept falling, so nothing that watches lag or start-up cycling saw
it. The suite now samples committed offsets per partition before calling a
workgroup healthy and restarts a worker whose member is alive while one
partition moves and the others do not. If it fires on camera, say what it is:
the paused-partitions variant of the pre-existing consumer defect.

## Act by act

Each act ends with a table: the rig's measurement, this run's, and a verdict
word (`same`, `close`, `context`, `DIFFERS`). The `gaps` rows are loss and must
be zero. Duplicates and inversions move with timing. Durations below are ACT
START to ACT END from each act's `timeline.txt` in the reference run; the
suite's own "took" line at the bottom of a table reported cumulative time in
that run (fixed in a47c63f6).

### Act 01: here is the code (62 s)

Reference run: 23:10:00 to 23:11:02. Unit suite 1720 passing, 1 pending, 0
failing in 60 s; lint exit 0. Every row `same`.

Terminal only: the commits of the two segments on top of development/9.3, the
file map of the delivery pool, where the tests live. Say: the delivery side is
a backbeat delivery worker in **Node**, on the existing consumer; not a new
service, not a new language. The three functional suites (pool, workgroups,
kerberos) are out of CI on purpose and behind `DEMO_ACT01_FULL=1`.

### Act 02: what today does (95 s, rig M1)

Reference run: 23:11:02 to 23:12:38. 25 of 25 events (20 PUTs, 5 DELETEs), 0
gaps, 0 duplicates, 0 inversions; one assign and no revoke on the processor.
Every row `same`.

A real S3 PUT through CloudServer, the mongo oplog, the populator, one
per-destination processor, the customer's topic. **Kafka UI, Topics**: the
internal topic `backbeat-bucket-notification` with four partitions and the
customer topic growing by 25. Point at the record key `<bucket>/<objectKey>`,
the `size` field as a **string**, and the nulls (`eTag`, `versionId`,
`sequencer`, `principalId`) the mongo log source gives. The migration
preserves this byte for byte.

Say the cost: about 12 processes at about 78 MB per destination, roughly 936 MB
whether or not traffic flows, 11 of them idle standbys; about 56 destinations
fit on six nodes; the requirement is 10,000.

The terminal prints "seeded ... at the head of backbeat-bucket-notification on
partitions 0, 1, 2, 3" before the processor starts. Say what it is: a group that
has run in production for years has a committed offset on every partition; a
brand-new one starts at `latest` and would skip what lands during its first
rebalance, so the suite puts the group in the production state first.

### Act 03: the failure that is invisible (423 s, rig M10)

Reference run: 23:12:38 to 23:19:41. Pool: dropped 20 poc-dest-3
`producer_error`, 20 krb-dest-a `producer_error`, 20 krb-dest-b
`delivery_error`; healthy destination 5 of 5; delivery group lag 0. Legacy:
the refused destination's processor cannot start; the unwritable destination's
processor committed from 33 to 53 past events it never delivered; failed topic
0; no counter. The wedge fired on the pool worker (61 assigns, 60 revokes,
nothing delivered), the cure restarted it at 23:18:01, the first drop landed
52 s later.

The act that earns the redesign. Three dead destinations: a refused connection,
an unroutable address, and a reachable broker with an unwritable target.

Legacy half: the processor for the refused destination **cannot start**; under
a supervisor that is a crash loop consuming nothing. The third class depends on
the cluster and the act says which it built. On the rig, a multi-broker
cluster, it was a **leaderless partition**: the produce hangs, the offsets never
move, and at about t+300s the member is evicted. On this single-broker stack it
is a topic with `max.message.bytes` below one event: the produce is **rejected
immediately**, the processor's send callback fires with an error, it calls
`done()`, and the consumer **commits and advances past events it never
delivered**. The act's offset row says "moved, 33 to 53". Same silent failure,
one step worse: no counter, and the failed topic the config names stays at 0
because no production code reads it.

Pool half: the worker starts **healthy** with every dead destination configured
(producers are created lazily, per endpoint). **Grafana, "Drops per second, by
reason"**: `producer_error` at once for the refused and blackholed endpoints,
`delivery_error` after about 30 s for the unwritable one (the configured
deadline). The delivery group's lag goes to 0: it commits past the drops. The
healthy destination delivers throughout.

Say: today a dead destination fails silently with no counter, by stalling that
destination forever or by committing past dropped events. The pool makes it a
bounded, counted, per-destination drop within 30 s. Keep the caveat: the drop
reason cannot yet tell a timeout from a rejection, because the delivery report
carries a different error code than the timeout branch tests for. Fix it before
the label is promised to operators.

### Act 04: the migration, and its free rollback (458 s, rig M2b and M4b)

Reference run: 23:19:41 to 23:27:19. Cutover: 218 of 218, 0 gaps, 0
duplicates, 0 inversions; legacy lag at the instant of the switch 22; internal
topic frozen at 143 with the legacy group committed at 143. Rollback: 151 of
151, 0/0/0; nothing from the cutover window re-delivered; delivery topic
frozen; the legacy group resumed with a lag of 126, exactly the records
published since the switch. Every row `same` or `close`. One wedge: the pool
worker, caught by the drain gate at step 5 (lag 136, nothing delivered),
restarted 23:23:34, drained 85 s later.

Five operator steps, no new tooling, because today's per-destination processor
**is** the design's single-destination worker of generation v0:

1. Start the delivery worker **first**, on the quiet delivery topic. Started
   after the switch it would replay whatever accumulated during the switch,
   which is where the rig's 69 duplicates came from.
2. Switch the populator. **Kafka UI, Topics**: `backbeat-bucket-notification`
   freezes (143 in the reference run), `bucket-notification-delivery` starts
   moving. Point at the delivery topic's partitions: one destination occupies
   exactly one, because the key is the bare destination name. Capacity for one
   destination goes through `spreadFactor`, never the partition count.
3. Drain the legacy side, gating on lag zero **and** progress. Say why: a
   wedged consumer's lag stops falling while its probe answers 200.
4. Stop the legacy processor. The act shows the legacy group committed exactly
   at the frozen internal-topic head, which is what makes the rollback free.
5. Mirror rollback: populator back, pool drains to 0, legacy resumes at its
   own offset, stop the worker. Nothing lost, duplicated, reordered or
   re-delivered.

**Grafana, "Cutover: legacy groups versus the delivery pool group"**: the two
lag series, and the lag table with the legacy group and the pool group both
at 0 afterwards. Capture from the reference run:
`poc-demo/evidence/screenshots/bnaas-grafana-act04-cutover.png` and
`bnaas-kafkaui-act04-topics.png` (23:21 CEST, internal topic at 143, delivery
topic at 169 and moving).

Contrast with the drainer path's rollback on the rig: 106 re-delivered, 161
stranded, no reverse drainer. That is the argument for this path.

### Act 05: crashes (635 s in the reference run, rig M11 and M12)

Reference run: 23:27:19 to 23:37:54. Populator kills: 2, gaps 0, duplicate
extras 105. Worker kill: **red in the reference run**, 173 gaps, for harness
reasons explained below; rerun on the fix recorded in the section "Act 05
rerun" at the end of this file.

Two `kill -9`s on the populator under load. The first lands during read and
filter and costs nothing. The second is timed to land right after a publish,
between the kafka acknowledgement and the ZooKeeper offset write, and in the
reference run it did what the rig's M11 says it can: the restart republished
the whole batch, 105 duplicate deliveries, 0 lost. Low probability, high
amplitude, and the rig's two kills never hit it, which is why its table row
expects 0 and prints `DIFFERS` here. Say so.

Then `kill -9` on the worker. The supervisor restarts it two seconds later,
the way systemd would. The replacement holds nothing until the dead member's
session expires and its first join is revoked and re-assigned, and the act
now measures that pause. It re-delivers the uncommitted window and nothing
else, bounded by the consumer's five second auto-commit, which is not exposed
in the schema (product question 8). **Grafana, "Delivery workers up"**: 1 to 0
to 1.

Why the reference run's worker-kill case went red: the wedge cure earlier in
the act had gone through `Proc.stop()`, which drops the supervision and was
never restored, so the SIGKILL killed a worker nothing would bring back; the
act read the zombie pid as running; and the drain after the cure started its
60 s stall clock a second after the process was ready while the first
assignment took 67 s, so it gave up with 173 records still on the delivery
topic. Nothing was lost in the pipeline. Fixed in cf046aae: supervision is
carried across a restart, the act waits for the replacement's first
assignment, and the post-cure drain allows 120 s.

### Act 06: workgroups, and ZooKeeper (rig W gates and C5r)

Reference run: filled in from the run's verdict, see "Reference run totals".

The act to slow down on, and the longest. **One** internal delivery topic; a
workgroup is a consumer group over it with a slice filter, so a worker commits
records outside its slice without delivering them. Per-workgroup topics are
out. Every consumer start in it can wedge; the suite cures each.

With the browsers open:

- **The document is the contract.** ZooNavigator,
  `/bnaas-demo/delivery-workgroups`: a generation, a hashmod rule whose
  remainders cover the whole modulo, optional static pins, and at a change the
  barrier offsets and the previous groups. The terminal prints the same.
  Captured from an earlier run today, generation 3:
  `poc-demo/evidence/screenshots/bnaas-zoonavigator-act06-workgroups-doc.png`
  (22:29 CEST): wg-a remainders 0 and 1, wg-b 2, wg-c 3, wg-pin static
  poc-dest-1, previousGroups the three gen-2 groups, barriers per partition.
- **Ownership is a total function.** md5 over the destination token modulo
  the hashmod modulo; the act prints the mapping with the repository's own
  function. Every record has exactly one owner, which is why a worker can
  commit what it does not own.
- **One consumer group per workgroup**, named `<base>-<workgroup>-gen<G>`.
  **Kafka UI, Consumers**: after the reshard, gen1, gen2 and gen3 groups sit
  side by side. Captured 22:28 CEST:
  `bnaas-kafkaui-act06-consumer-groups.png`. **Grafana, Workgroups row** and
  the lag table: `bnaas-grafana-act06-workgroups.png` (22:27 CEST).
- **The cost of one shared topic**: a hash and a commit per record a workgroup
  does not own, no I/O. Stated, not hidden.

Then the three things that happen to it:

- **A worker dies** (`kill -9`). Only its workgroup's destinations pause. Point
  at the per-workgroup **delivered** panel, not the lag one: both groups read
  the whole topic and skip what they do not own, so their raw lag is about the
  same; the isolation is in the delivered counters.
- **A noisy destination is pinned.** A static rule beats the hashmod one; the
  destination gets its own workgroup and its own blast radius. The step drives
  its own burst of traffic to the pinned destination so the claim is provable
  after the long load has ended.
- **A live reshard, two workgroups to three**, over the same modulo, so exactly
  one destination changes owner. The tool writes a barrier record on every
  partition, then the document with those offsets, then pre-seeds the new
  generation's groups while they are empty; the ZooKeeper write is the commit
  point. The old generation owns everything before its barrier, the new one
  starts at it. **Wait for `verify` to exit 0** before stopping the old
  generation: that is the whole discipline. `DEMO_WORKGROUPS_STOP_EARLY=1`
  shows the loss when you do not.

Duplicates are the old generation consuming past its barrier while it drains:
the longer it runs, the more there are. Gaps are impossible once `verify` has
exited 0. Known gap: a crashed old-generation worker cannot restart to finish
its drain once the document is overwritten; the proposed amendment is one
ZooKeeper node per generation plus a current pointer.

### Act 07: Kerberos (rig GATE 2)

Reference run: filled in from the run's verdict, see "Reference run totals".

One process, two identities. librdkafka gives one Kerberos identity per OS
process, measured, with every documented workaround tried and failed; the
per-connection producer authenticated two and then fifty principals in one
process. The rule to state on camera: only the broker's own `authenticationID`
line counts, because a client stack can report the identity it was configured
with rather than the one it authenticated as.

Needs `yarn demo:up:krb`, which also builds the test image and seeds its
node_modules volume (`bin/krb-test-image.sh`, setup time, not demo time).
Without the profile the act skips with a message that names that script.

### Act 08: the semantics that change (rig M5, M9, M8, M6b)

Reference run: filled in from the run's verdict, see "Reference run totals".

Four short cases, each ending in a decision:

- **Mixed window**: both consumers alive partition the stream, they do not
  duplicate it, because the populator's routing is an if/else.
- **Detach**: today a detached destination's queued events are dropped
  silently, which reads like a revocation; on the pool they still arrive.
  Release note, and product question 3.
- **Overlapping rules**: a catch-all plus a prefix rule delivers to both
  destinations, on both paths. First-match-wins is what neither implementation
  does. Product question 6.
- **Name collision** (slow down here): an account-scoped ARN naming a
  destination that does not exist is refused; the same shape naming an existing
  **global** destination is accepted with HTTP 200, read back verbatim, and
  every event lands on the global destination carrying a configuration id it
  never set up. Every component matches on the last ARN segment and discards
  the account field. Any tenant who can put a bucket notification configuration
  can do this today. Product question 5, and a threat-model row.

## Close

Three decisions, not more demo: the failure contract in the requirements' own
words (retry for a window, then abandon and count), destination naming across
account-scoped and global, and whether per-object ordering is written in as a
target with the known rebalance exception. They are in
`~/capsule-corp/bnaas-poc/gw2cto/08-asks-from-product.md`.

## Reference run totals

REFERENCE_RUN_TOTALS

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
