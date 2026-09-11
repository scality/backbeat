---
name: bnaas-demo-walkthrough
description: The recording storyline for the BNaaS (bucket notifications as a service) demo: what each act of the test suite shows, what to point at in Grafana, Kafka UI and the ZooKeeper browser, and what to say about each number. Use when recording or presenting the POC.
---
<!-- markdownlint-disable MD013 -->

# The BNaaS demo, as a recording

Two commands and three browser tabs.

```bash
yarn demo:up:krb                          # infrastructure, CloudServer and the KDC
yarn demo:wait                            # blocks until it all answers
DEMO_PACE=slow yarn ft_test:demo          # the take
```

Tabs: Grafana http://localhost:4000 (dashboard "BNaaS delivery pool"), Kafka
UI http://localhost:9085, ZooNavigator http://localhost:10000 for the
ZooKeeper browser, Prometheus http://localhost:10090. Subtract 1000 at
`PORT_OFFSET=0`, except ZooNavigator, which is 9000 there.

The suite narrates itself: every step prints a timestamped plain-language
line, and each act ends with the rig's measurement next to this run's. Your
job on camera is the why, not the what.

The through-line to open with and return to at the end: **a destination is a
deployment artifact today, and this makes it a resource. Nothing here loses
an event. What changes is what a failure looks like.**

Eight acts, about 60 minutes at normal pace. To cut it short, run
`DEMO_ACTS=02,03,04,06` and read the rest from
`poc-demo/results/RESULTS.md`.

## Act 01: here is the code

The commit lists of the two branches, the file map of the delivery pool, and
where the tests live. Then the repository's own suites: the unit suite (about
1720 passing), lint, and the pool functional suite.

Say: the delivery side is a backbeat delivery worker in Node on the existing
consumer, not a new service and not a new language. The workgroups functional
suite is out of CI on purpose, because it needs a broker and ZooKeeper.

The suites hardcode localhost:9092, so run this act with the stack at
`PORT_OFFSET=0`, or it prints what it would have run and why it did not. One
unit test binds port 8080, so two unit runs cannot overlap on one machine.

## Act 02: what today does

Watch the events arrive on the customer topic. Point at the record key
`<bucket>/<objectKey>`, the `size` field as a **string**, and the nulls:
`eTag`, `versionId`, `sequencer`, `principalId`. That is what the mongo log
source gives, and the migration preserves it byte for byte.

Kafka UI, the internal topic: four partitions, one record per object,
consumed by one process per destination. Say the cost: 12 processes at about
78 MB per destination, roughly 936 MB whether or not traffic flows, 11 of
them idle standbys. About 56 destinations fit on six nodes. The requirement
is 10,000.

## Act 03: the failure that is invisible

The act that earns the redesign.

Legacy half: the processor for a refused destination **cannot start**, and
under a supervisor that is a crash loop consuming nothing. Then a reachable
broker with an unwritable target, the third failure class. What it does
depends on the cluster, and the act says which it built:

- On a multi-broker cluster (the rig) the class is a **leaderless partition**:
  the broker is reachable, the producer is ready, but the produce **hangs**.
  The processor attempts only its concurrency worth of records, the group's
  committed offset does not move on any partition for five minutes, and at
  about t+300s the log shows the sends timing out and the commit failing with
  the group assignment lost: the stall blew the poll interval and the broker
  evicted the member.
- On the **single-broker demo** that partition cannot be created, so the class
  is a topic with `max.message.bytes` far below one event: the producer
  connects and reports ready, and every produce is **rejected immediately**.
  The send callback fires with an error, the processor calls `done()`, and the
  consumer **commits and advances past events it never delivered**. The act's
  offset row says "advances past N undelivered events" rather than "none".

Say the point out loud: same silent failure, one step worse. Either the
offsets freeze and the destination stalls forever, or they advance past
records that were dropped on the floor. No counter moves either way, and the
failed topic the config names stays at zero, because no production code reads
it.

Pool half: the worker starts **healthy** with four unreachable destinations
configured. Grafana, the drops panel: 20 of 20 per destination, labelled
`producer_error` immediately for a refused and a blackholed endpoint, and
`delivery_error` after about 30 seconds for the leaderless one, which is the
configured deadline. The delivery group's lag goes to zero: it commits past
the drops. A healthy destination delivers throughout.

The line to say: today a dead destination fails silently with no counter at
all, either by stalling that destination indefinitely or by committing past
events it never delivered. The pool turns the same failure into a bounded,
counted, per-destination drop visible in 30 seconds. Neither side has a
dead-letter path.

Keep the honest caveat on camera: the drop reason cannot yet tell a timeout
from a rejection, because the delivery report carries a different error code
than the timeout branch tests for. Fix it before the label is promised to
operators.

## Act 04: the migration, one Ansible run

Say the model first: production applies every change by replacing
containers, so there is no drain, no switch and no rollback step. The
populator is untouched; it keeps writing one record per event to today's
topic. The processors are deleted, the workers start on the same topic and
match per destination themselves.

1. Today's path, three destinations, one processor each, under load.
2. Freeze one processor (SIGSTOP): a stalled destination whose backlog
   grows. Stop another a little later: a small backlog. Leave one caught up.
   Grafana, row "Migration": three processor groups, three different lags.
3. **The run, part one: delete every processor.** The act prints each
   group's committed offset per partition first; that table is the whole
   argument for the next step.
4. **Part two: write the layout, seed.** `bin/notificationDeliverySeed.js
   seed-from-processors --generation 1`: the lowest processor offset per
   partition, so nothing is skipped, and a watermark per destination at its
   own processor's offset, so nothing already delivered is sent again.
   ZooNavigator: the document and its `watermarks/gen1` child.
5. **Part three: start the worker.** Say the pause aloud when the act prints
   it: the container swap plus the group join, 45 s of it the consumer
   session.
6. The check: nothing lost, nothing doubled, order kept, and the stalled
   destination's whole backlog delivered by the pool. Point at the
   "records skipped under a watermark" panel: that is the caught-up
   destination's already-delivered records being committed without a second
   delivery.

The variant `DEMO_ACT04_WITHOUT_WATERMARK=1` runs the same swap with the
watermarks deleted after seeding: it measures what the watermark saves, the
spread between the processors' offsets delivered twice.

## Act 05: crashes

Two populator kills cost nothing. Explain the two windows: during read and
filter a kill costs nothing, and between the kafka acknowledgement and the
offset write it republishes the whole batch. Low probability, high amplitude.

A worker kill re-delivers exactly the uncommitted window. The bound is the
consumer's five second auto-commit interval, not the configured concurrency,
and that interval is not exposed in the schema. Product question 8.

## Act 06: workgroups, and ZooKeeper

The act to slow down on. Say the topology is decided: the workers read
**today's** topic. A workgroup is a consumer group over it with a slice
filter, so a worker commits records outside its slice without delivering
them. No second topic, no populator change, no per-workgroup topics.

What to explain, in this order, with the ZooKeeper browser open:

- **The document is the contract.** A generation, a hashmod rule and optional
  static pins. Show it in ZooNavigator at `/bnaas-demo/delivery-workgroups`
  and in the terminal (`bin/zk-show.sh`). Account-scoped destinations never
  touch it: they hash into a workgroup by name and start delivering with no
  change and no restart. Only a layout change (the number of auto workgroups,
  a pin) is a new generation, and a new generation is an Ansible run.
- **How a destination picks its workgroup.** md5 over the encoded destination
  token, modulo the hashmod modulo, and the remainders cover the whole
  modulo. That total coverage is what makes ownership a total function: every
  record has exactly one owner, which is why a worker can commit what it does
  not own without anything being lost. The act prints the mapping using the
  repository's own function.
- **One consumer group per workgroup**, named `<base>-<workgroup>-gen<G>`.
  Show them in Kafka UI, and the per-workgroup panels in Grafana.
- **The cost of one shared topic**: a hash and a commit per record a
  workgroup does not own, no I/O. That is the trade against per-workgroup
  topics, and it is stated, not hidden.

Then the three things that happen to it:

- **A worker dies.** Only its workgroup's destinations pause; the other
  workgroup keeps delivering. Point at the per-workgroup **delivered** panel,
  not the lag one: every workgroup reads the whole topic and skips what it
  does not own, so the raw lag of two workgroups is about the same number and
  the isolation does not show there. Then say what a wedge looks like
  instead, because it is worse: the worker is up, holds its partitions,
  answers liveness 200, and its delivered counter does not move. Check the
  counter, not the lag. The cure is a restart.
- **A noisy destination is pinned**, generation 2. A static rule beats the
  hashmod one, so the destination is carved out of the hash space and gets
  its own workgroup and its own blast radius. That is the lever for a hostile
  tenant. The change is the Ansible order: stop generation 1, write the
  document, `seed-from-generation --from 1 --to 2`, start generation 2. Say
  the pause when the act prints it.
- **A reshard, two auto workgroups to three**, generation 3, under traffic.
  Same order. The seed tool takes, per partition, the lowest committed offset
  across the generation 2 groups a new group inherits from, and a watermark
  per destination at its previous owner's offset, so the one destination that
  changes owner keeps its place in the stream.
  - **Why loss is impossible**: every record either was delivered by the old
    generation or is read by the new one from the lowest offset.
  - **Why nothing is doubled**: the watermark. Without it the new generation
    would re-deliver the spread between the old groups' offsets.
  - **Why nothing is reordered**: one generation at a time. Two generations
    delivering the same keys together reordered 4197 pairs in one rehearsal
    of the previous model; the price of never doing that is the pause.
  - **What still stands**: a destination's per-object lanes deliver one record
    per producer poll, 2000 ms; and a kill or a cure restart re-delivers the
    worker's uncommitted window, at-least-once.

## Act 07: Kerberos

One process, two identities. The claim it settles: librdkafka gives one
Kerberos identity per OS process, measured, with every documented workaround
tried and failed; the per-connection producer authenticated 2 and then 50
principals in one process. The evidence rule to state on camera: only the
broker's own `authenticationID` line counts, because a client stack can
report the identity it was configured with rather than the one it
authenticated as.

Needs the krb profile (`bin/stack-up.sh --krb`); the act skips cleanly and
says why if it is not there.

## Act 08: the semantics that change

Three short cases, each ending in a decision. The mixed-window case is gone
with the second topic: one path, and every change is a container swap, so no
two consumers are ever alive together.

- **Detach**: today a detached destination's queued events are dropped
  silently, which reads like a revocation. The pool matches at delivery time
  on the same topic, so the act measures what it does; the previous model,
  which resolved the destination at publish time, delivered them. What the
  pool can do that the processor cannot is count the drop. Product question 3.
- **Overlapping rules**: a catch-all plus a prefix rule delivers to both
  destinations, on both paths. First-match-wins is what neither
  implementation does, so shipping it literally would be a silent behaviour
  change. Product question 6.
- **Name collision**: the one to slow down on. An account-scoped ARN naming a
  destination that does not exist is refused. The same shape naming an
  existing **global** destination is accepted with HTTP 200, read back
  verbatim, and every event lands on the global destination carrying a
  configuration id that destination never set up. Every component matches on
  the last ARN segment and discards the account field. Any tenant who can put
  a bucket notification configuration can do this today. Product question 5,
  and a threat-model row.

## Close

Three decisions, not more demo: the failure contract in the requirements' own
words (retry for a window, then abandon and count), destination naming across
account-scoped and global, and whether per-object ordering is written in as a
target with the known rebalance exception. They are in
`~/capsule-corp/bnaas-poc/gw2cto/08-asks-from-product.md`.

## If something wedges mid-take

It will, and the suite handles it: every consumer it starts is watched for
the churn signature for a few seconds, and a churning one is restarted with a
line that says so. When that line appears, say what it is on camera. It is a
measured pre-existing defect with an evidence file, in code that replication
and lifecycle share, and it is the reliability ceiling the POC reports.
Showing the cure reads better than hiding the symptom.

The same cure runs inside the workgroups act's drain gate, because a wedged
previous-generation worker is what makes `verify` never exit 0.

## Recording the demo: the exact commands

The minute-by-minute script, with the measured times from the reference run,
is `poc-demo/PLAYBOOK.md`; this section is the short form. `DEMO_PACE=demo` is
the default and is tuned for recording. `slow` doubles every deliberate wait
for a careful take; `fast` cuts them to about a third and is for iterating.
Before a take: stop every other Kafka stack and rig on the machine (the
PLAYBOOK says which and why), and disable system sleep with
`caffeinate -dimsu`, because the consumer groups rebalance during a sleep and a
long act never recovers its narration.

```bash
# once, before the take, from the checkout you run the suite from
yarn demo:up:krb
yarn demo:wait

# the recording: the four acts that carry the argument, at the default pace
DEMO_ACTS=02,03,04,06 yarn ft_test:demo

# the whole demo, every act, for a reference run (about 80 minutes measured)
yarn ft_test:demo

# one act, to reshoot it
DEMO_ACTS=04 yarn ft_test:demo

# the four-pane terminal layout, if you want the processes on screen
poc-demo/bin/demo-layout.sh
```

Have Grafana, Kafka UI and ZooNavigator open before you start, and leave the
Grafana dashboard on a five-minute window. Keep the honest parts in: the
consumer wedge and the worker exits are measured pre-existing defects with
evidence files, the suite says WEDGE SUSPECTED and restarts that one consumer
the way an operator would, and showing the cure reads better than hiding the
symptom.

## Related

- Infrastructure and URLs: `bnaas-demo-stack`
- Running acts and reading the numbers: `bnaas-run-scenario`
- Branches, evidence, open decisions: `bnaas-poc-state`
