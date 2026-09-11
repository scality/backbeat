---
name: bnaas-run-scenario
description: Run the BNaaS (bucket notifications as a service) demo test suite or one of its acts, read the gaps, duplicates and inversions it reports, and tell a real result from the consumer wedge. Use to reproduce a measured POC result, to rehearse the demo, or when an act's numbers do not match the rig's.
---
<!-- markdownlint-disable MD013 -->

# Run the demo suite

The demo is a mocha functional suite. It brings up nothing itself except the
host processes: the infrastructure comes from compose first (see
`bnaas-demo-stack`).

```bash
yarn ft_test:demo                                   # every act, in order
DEMO_ACTS=02,04 yarn ft_test:demo                   # only these
DEMO_ACTS=06 DEMO_PACE=slow yarn ft_test:demo       # one act, recording pace
poc-demo/scenarios/run.sh 04 06                     # the same, by wrapper
poc-demo/scenarios/run.sh --list                    # what the acts are
```

Environment knobs: `DEMO_ACTS` selects and orders acts, `DEMO_PACE` is
`slow`, `normal` or `fast` and scales every deliberate wait,
`DEMO_WORKGROUPS_STOP_EARLY=1` adds the deliberate loss variant to act 06,
`DEMO_ACT01_FULL=1` adds the three out-of-CI functional suites to act 01,
which otherwise runs only the unit suite and the linter, `DEMO_STALL_WATCH_S` sets how long act 03 watches the legacy stall.

## The acts

| act | rig scenario | what it proves | minutes |
|---|---|---|---|
| 01 code-and-tests | the branches | the commit lists, the file map, and the unit, lint and functional suites | 2, or 10 with the suites |
| 02 legacy-baseline | M1 | today's path end to end: 25 events, in per-key order, in the shape the migration preserves | 2 |
| 03 dead-destination | M10 | the legacy stall with no counter, against 60 counted drops in about 30 seconds | 6, or 12 with the full stall watch |
| 04 switch-and-drain | one run | the migration as one Ansible run: delete the processors, seed the worker groups from their offsets, start the workers; loss, duplicates, order, pause, the stalled backlog | 8 |
| 05 crashes | M11, M12 | two populator kills cost nothing, a worker kill costs the uncommitted window | 7 |
| 06 workgroups | W gates, C5r | two workgroups, one worker's death, a pin and a reshard, each layout change a seeded container swap | 9 |
| 07 kerberos | GATE 2 | two Kerberos principals in one process, skipped cleanly without the krb profile | 8 |
| 08 semantics | M9, M8, M6b | detach, overlapping rules, name collision | 10 |

Each act is self-contained: it starts the processes it needs, stops them
afterwards, and writes evidence under `poc-demo/evidence/<act>/`. Any subset
can run in any order.

## Reading the numbers

Every act ends with a table putting the rig's measurement next to this run's,
and the last column says `same`, `close`, `context` or `DIFFERS`. Four numbers
carry the argument, and they are not equally important:

- **gaps**: an operation the driver completed that never arrived. This is
  **loss**, it is the row that must match, and every act asserts it is zero
  except where an act deliberately shows loss.
- **duplicate extras**: extra copies. Expected, bounded, allowed by the
  requirements. They scale with backlog and rebalance timing, so a number
  that differs from the rig's is usually timing, not a defect.
- **inversions**: two operations on ONE key delivered out of order. Only the
  straddle keys can show one, because only they have several operations.
- **unexpected**: delivered events no driver log accounts for, which usually
  means evidence from an earlier run leaked in.

An event is identified by (object key, Put or Delete, size), because the
driver carries a monotonic sequence in the object size.

Every act that measures through a legacy processor warms its consumer group
first, with a few operations and a wait for committed offsets on every
partition. The legacy processor starts at `latest`, so a group that has never
committed can skip what is already on the topic and show loss the pipeline
did not cause. The warm-up is the rig's own method note, and the act says so
as it runs.

## The wedge, and its cure

When a drain gate prints `WEDGE SUSPECTED`, or a consumer's lag stops
falling, ask whether it is **delivering**, not whether it is assigned:

```bash
poc-demo/bin/worker.sh status
grep -c 'rdkafka.assign' poc-demo/evidence/<act>/worker1.log
grep -c 'rdkafka.revoke' poc-demo/evidence/<act>/worker1.log
grep 'un-assigning' poc-demo/evidence/<act>/worker1.log | tail
```

Signature: a live group member holding all its partitions, `CURRENT-OFFSET`
possibly `-`, cycling `assign -> revoke -> processing queue idle,
un-assigning` about once a second, no delivery counters, liveness 200. Cure:
restart that one consumer, and only that one. The suite does this itself
where a procedure cannot skip the step.

The suite cures it for you: every consumer it starts is watched for the churn
signature for a few seconds and restarted if it shows it, and the workgroups
act does the same inside the drain gate, because a wedged
previous-generation worker is what makes `verify` never exit 0.

Three lookalikes that are not the wedge:

- **Nothing published yet.** The populator's batch cadence is several
  seconds, so a topic can still be empty when the driver has finished. The
  gate knows this and says "still empty, nothing to drain".
- **A group with offsets on another topic.** `--describe` prints every topic
  a group ever consumed, so a reused group id has a total lag that never
  reaches zero. Every lag figure in the suite is per topic.
- **A topic that came back with one partition.** `auto.create.topics.enable`
  is on, so a producer touching the name between a delete and a re-create
  wins the race. The suite grows it back with an alter and says so.
- **A worker with no counters but deliveries in its log.** Its probe server
  lost the race for its port, which is non-fatal by design. The suite waits
  for the probe and restarts once; by hand, restart that worker.
- **A worker that keeps exiting.** An uncaught error out of the consumer's
  commit path kills a standalone worker on an ordinary rebalance. The suite
  restarts it, the way systemd does, and counts the exits. Those exits are a
  real pre-existing defect, and they are why a member death costs minutes
  rather than the 45 second session timeout.

## Evidence

```
poc-demo/evidence/<act>/
  timeline.txt         every step, with epoch milliseconds
  verdict.txt, .json   the expected versus measured table
  driver-*.log         one line per completed operation, in order
  events-*.jsonl       the customer topic slices, replayable from an offset
  checker-*.json       the full per-key report
  populator-*.log processor-*.log worker*.log
  backbeat-*.json      the exact configs the act ran with
  cutover-*.log        what the workgroups tool printed, act 06
```

A previous run's directory is moved aside with a timestamp when an act
starts, because the driver appends to its log and a mixed log would make the
checker report the earlier run's operations as gaps.

To look at evidence by hand: `poc-demo/bin/check.sh <label> --events <dump>
--driver <log>`, which is the same checker the suite uses.

## Related

- Bring the infrastructure up: `bnaas-demo-stack`
- What to say while it runs: `bnaas-demo-walkthrough`
- Branches, evidence, open decisions: `bnaas-poc-state`
- The measurements each act compares itself against: `poc-demo/results/RESULTS.md`
