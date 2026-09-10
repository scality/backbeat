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
under a supervisor that is a crash loop consuming nothing. Then the
leaderless destination: it starts, attempts only its concurrency worth of
records, and the group's committed offset does not move, on any partition, for
five minutes. No counter moves. The failed topic the config names stays at
zero, because no production code reads it. At about t+300s the log shows the
sends timing out and the commit failing with the group assignment lost: the
stall blew the poll interval and the broker had already evicted the member.

Pool half: the worker starts **healthy** with four unreachable destinations
configured. Grafana, the drops panel: 20 of 20 per destination, labelled
`producer_error` immediately for a refused and a blackholed endpoint, and
`delivery_error` after about 30 seconds for the leaderless one, which is the
configured deadline. The delivery group's lag goes to zero: it commits past
the drops. A healthy destination delivers throughout.

The line to say: today a dead destination does not drop with the offsets
advancing, it stalls that destination indefinitely with no counter at all.
The pool turns the same failure into a bounded, counted, per-destination drop
visible in 30 seconds. Neither side has a dead-letter path.

Keep the honest caveat on camera: the drop reason cannot yet tell a timeout
from a rejection, because the delivery report carries a different error code
than the timeout branch tests for. Fix it before the label is promised to
operators.

## Act 04: the migration, and its free rollback

Five steps, no new tooling, because today's per-destination processor **is**
the design's single-destination worker of generation v0.

1. Start the worker first, on the quiet delivery topic. Say why: started
   after the switch it replays whatever accumulated during the switch, which
   is where the rig's 69 duplicates came from.
2. Switch the populator. Kafka UI: the internal topic freezes, the delivery
   topic starts moving. Point at the delivery topic's partitions: one
   destination occupies exactly **one**, because the key is the bare
   destination name. Capacity for one destination goes through `spreadFactor`,
   never through the partition count.
3. Drain the legacy side, gating on lag zero **and** progress. Say the reason
   out loud: a wedged consumer holds its partitions with a lag that stops
   falling while its liveness probe answers 200, and that wedge landed on this
   exact step during the measured rollback.
4. Stop the legacy processor. The act shows the legacy group committed exactly
   at the frozen internal-topic head, which is what makes the rollback free.
5. Then the mirror rollback: populator back, pool drains to zero, legacy
   resumes at its own offset, stop the worker. Nothing lost, nothing
   duplicated, nothing reordered, and nothing from the cutover window
   re-delivered.

Contrast with the other rollback, the one after the drainer path: 106
re-delivered and 161 stranded on the delivery topic, with no reverse drainer
to recover them. That is the argument for this path as the default.

## Act 05: crashes

Two populator kills cost nothing. Explain the two windows: during read and
filter a kill costs nothing, and between the kafka acknowledgement and the
offset write it republishes the whole batch. Low probability, high amplitude.

A worker kill re-delivers exactly the uncommitted window. The bound is the
consumer's five second auto-commit interval, not the configured concurrency,
and that interval is not exposed in the schema. Product question 8.

## Act 06: workgroups, and ZooKeeper

The act to slow down on. Say the topology is decided: **one** internal
delivery topic. The populator writes to that one topic; a workgroup is a
consumer group over it with a slice filter, so a worker commits records
outside its slice without delivering them. Per-workgroup topics are out.

What to explain, in this order, with the ZooKeeper browser open:

- **The document is the contract.** A generation, a hashmod rule, optional
  static pins, and at a change the barrier offsets. Show it in ZooNavigator
  at `/bnaas-demo/delivery-workgroups` and in the terminal (`bin/zk-show.sh`).
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
  the isolation does not show there. Then say
  what a wedge looks like instead, because it is worse: the worker is up,
  holds its partitions, answers liveness 200, and its delivered counter does
  not move. Check the counter, not the lag. The cure is a restart.
- **A noisy destination is pinned.** A static rule beats the hashmod one, so
  the destination is carved out of the hash space and gets its own workgroup
  and its own blast radius. That is the lever for a hostile tenant.
- **A live reshard, two workgroups to three**, over the same hashmod modulo,
  so exactly one destination changes owner and the others stay put. Say why the barrier exists:
  the tool writes a barrier record on every partition, then the document with
  those offsets, then pre-seeds the new generation's groups while they are
  still empty; the ZooKeeper write is the commit point. The old generation
  owns everything before its barrier, the new one starts at it, so no record
  is reordered across the change and none is missed.
  - **Why duplicates appear**: the old generation keeps consuming past its
    barrier until you stop it, and every record it consumes there is
    delivered twice. The longer it runs, the more there are. On the rig that
    was 18 to 30 per 100 while it drained.
  - **Why gaps are impossible**: only if you wait for `verify` to exit 0.
    That is the whole discipline. Stopping the old generation early loses
    exactly the records the drain report was still counting, which the
    `DEMO_WORKGROUPS_STOP_EARLY=1` variant shows deliberately.
  - **The known gap**: a crashed old-generation worker cannot restart to
    finish its drain once the document has been overwritten. The proposed
    amendment is one ZooKeeper node per generation plus a current pointer.

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

Four short cases, each ending in a decision:

- **Mixed window**: both consumers alive partition the stream, they do not
  duplicate it, because the populator's routing is an if/else.
- **Detach**: today a detached destination's queued events are dropped
  silently, which reads like a revocation. On the pool they still arrive.
  Release note, and product question 3.
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

`DEMO_PACE=slow` doubles every deliberate wait, which is what gives you room
to talk over a step. `fast` cuts them to about a third and is for iterating,
not for recording. Disable system sleep first: the containers survive it, but
the consumer groups rebalance and a long act never recovers its narration.

```bash
# once, before the take
yarn demo:up:krb
yarn demo:wait

# the whole demo at recording pace, about 90 minutes
DEMO_PACE=slow yarn ft_test:demo

# the short version, the four acts that carry the argument, about 45 minutes
DEMO_PACE=slow DEMO_ACTS=02,03,04,06 yarn ft_test:demo

# one act, to reshoot it
DEMO_PACE=slow DEMO_ACTS=04 yarn ft_test:demo

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
