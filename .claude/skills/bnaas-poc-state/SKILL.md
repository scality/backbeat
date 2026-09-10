---
name: bnaas-poc-state
description: Where everything in the BNaaS (bucket notifications as a service) POC is: the branches, the demo stack and suite in this repository, the evidence and results, the design and CTO documents, the published artifacts, and the decisions still open with who owns each. Use to orient before touching the POC, or to find the file that carries a number.
---
<!-- markdownlint-disable MD013 -->

# BNaaS POC: where everything is

The code, the demo stack and the demo suite are on the branch
`poc/S3C-11127-demo` in `scality/backbeat`: `poc-demo/` is the stack,
`tests/functional/demo/` the suite, `poc-demo/results/` the measurements and
`poc-demo/HANDOVER.md` the map. The design and CTO documents, the rig's own
working files and the remaining evidence stayed outside the repository, in
`~/capsule-corp/bnaas-poc/`. The epic, the POC and the CTO document each have a Jira
issue; the ids are in `~/capsule-corp/bnaas-poc/gw2cto/GW2CTO-BNaaS.md`, which is the
one place that carries them.

## Decided, so nobody reopens it

**Topology.** One internal delivery topic. The populator writes to that one
topic, addressing each record to its destination; a workgroup is a consumer
group over the same topic with a slice filter, so a worker commits records
outside its slice without delivering them. Per-workgroup topics are out. The
built mechanism is the one with measurements, and its cost (a hash and a
commit per record a workgroup does not own, no I/O) is stated rather than
hidden.

**The engine** is a backbeat delivery worker in Node, on the existing
consumer. **The migration default** is drain-then-switch with the worker
started before the populator switch; the drainer stays only for a legacy
processor that cannot drain. **Assume-destination is out of scope**: explored
and set aside, reference only on its branch, not in the demo.

## Code: one branch to run, two segments in it

All three sit on top of `development/9.3` in `scality/backbeat`.

| branch | what it carries |
|---|---|---|
| `poc/S3C-11127-demo` | **the one to clone.** Everything below plus `poc-demo/`, `tests/functional/demo/` and the four skills. `yarn demo:up` then `yarn ft_test:demo` runs the whole demo. |
| `poc/S3C-11127-workgroups` | the delivery pool plus workgroups: populator addressing, `DeliveryWorker`, producer pool, workgroup membership and slice filter, the ZooKeeper document and loader, the barrier cutover CLI, the drainer, and the out-of-CI functional suites. Contained in the demo branch. |
| `poc/S3C-11127-kerberos-producer` | nine commits on top of the workgroups tip: a pure-JS kafka client, a GSSAPI binding, per-destination credentials, the per-destination producer-stack choice, and the two-principal functional suite. node-rdkafka stays the default. Contained in the demo branch. |
| `poc/S3C-11127-assume-destination` | four commits: a manual workgroup can declare the destination it assumes, and delivery says so on every series. Its last commit says explored and set aside; reference it, do not build on it. NOT in the demo branch. |

`poc/S3C-11127-demo` is where the demo material is going: `poc-demo/` for
the compose stack, the configs, the scripts and the evidence layout,
`tests/functional/demo/` for the suite, and `.claude/skills/bnaas-*/` for
these skills, so one clone gives somebody everything. Commit messages there
are prefixed `S3C-11127: POC-specific-code:`.

Federation roles: `improvement/S3C-11127-delivery-pool-poc` in the federation
repo. CloudServer runs unmodified from `development/9.3`.

Read the commits with `git log --oneline development/9.3..HEAD`, or run
`DEMO_ACTS=01 yarn ft_test:demo`, which prints both segments' lists and the
file map.

## The demo is a test suite

`docker compose up` for the infrastructure, then `yarn ft_test:demo` for the
demo. Eight acts drive the whole story end to end and assert the design's own
promises. `DEMO_ACTS=02,04,06` selects acts, `DEMO_PACE=slow|normal|fast`
sets the waits. See `bnaas-run-scenario` to run it and `bnaas-demo-walkthrough`
to narrate it.

Everything but the backbeat processes is a container, CloudServer included,
and the suite finds the S3 endpoint itself after running the stack's own
`bin/wait-ready.sh`. The destination names the acts use are the five that
CloudServer validates a notification configuration against: `poc-dest-1`,
`poc-dest-2`, `poc-dest-3`, `krb-dest-a` and `krb-dest-b`. Backbeat's own
list, which the suite generates, decides where a record actually goes, so
those five names carry the healthy destinations, the three failure classes
and the workgroup spread.

## Rigs on this machine

| rig | what | do not disturb |
|---|---|---|
| demo stack | `demo/`, compose project `bnaasdemo`, `PORT_OFFSET=1000`, tmux `bnaas-demo`. The one to use. | |
| original rig | compose project `ft`, container `bnaas-mongo`, tmux `bnaas-rig`, standard ports | belongs to the migration and chaos rounds; the evidence in `rig/evidence/` came from it |
| Kerberos spike rig | compose project `bnaaskrb`, ports 1088, 19095, 19096 | belongs to the Kerberos spike |
| `f9-mongo` | port 27018 | another workstream entirely |

## Documents, and which number lives where

| file | what it holds |
|---|---|
| `~/capsule-corp/bnaas-poc/gw2cto/GW2CTO-BNaaS.md` | the Gateway-to-CTO document, house template, every number pointing at a measurement |
| `poc-demo/results/design-critique.md` | the question bank, the standards review of the design PR, and section 5's ordered list of what to change in it |
| `~/capsule-corp/bnaas-poc/gw2cto/04-pr383-review-comments.md` | paste-ready review comments for the citadel design PR, in co-author voice |
| `~/capsule-corp/bnaas-poc/gw2cto/08-asks-from-product.md` | the thirteen open questions for product, ordered by how much of the design depends on the answer |
| `poc-demo/results/RESULTS.md` | every scenario, its procedure and its numbers: the migration matrix M1 to M12 and the chaos round C1 to C5 |
| `rig/README.md` | the original rig, step by step, its shims and its gotchas |
| `design/06-backbeatconsumer-wedge.md` | the consumer wedge, with line references and fix shapes |
| `design/07-preexisting-findings.md` | the commit-path throws, the deliver-without-committing window, and the recon trio |
| `design/08-workgroups-code-design.md` | the pinned final design of workgroups as built |
| `design/09-workgroups-observations.md`, `design/11-reshard-observations.md` | the workgroup gates and the reshard answers |
| `poc-demo/results/RESULTS-kerberos.md` | librdkafka: one Kerberos identity per process, every workaround measured and failed |
| `poc-demo/results/RESULTS-kerberos-nodejs.md` | the per-connection alternative, proven for 2 and for 50 principals |
| `demo/README.md`, `HANDOVER.md` | how to run the demo stack, and the handover |
| `demo/tests/functional/demo/` | the demo suite: `demo.js`, `acts/01..08`, `lib/` |
| `demo/evidence/<act>/` | what each act leaves behind, including its configs |

Published artifacts: the findings summary
https://claude.ai/code/artifact/902d1e9d-6701-45b4-9f91-992dd02adffd and the
meeting brief
https://claude.ai/code/artifact/61b0d204-73b8-49d5-8efe-e2b3e760844e

## The headline numbers, so nobody re-derives them

- Today, per destination: about 936 MB of processes whether or not traffic
  flows, 12 processes of which 11 are idle standbys. About 56 destinations fit
  on six nodes; the requirement is 10,000. Pooled, a destination costs about
  0.75 MB.
- Adding one destination today: 8 to 22 minutes of playbook, about 121 seconds
  of delivery stall for every destination, an S3 API restart, and two static
  lists to edit.
- Migration, drain-then-switch: 0 of 601 lost on the cutover, 0 lost and 0
  duplicated and 0 reordered on the rollback, five operator steps, no new
  tooling. Done wrong, with the pool started before legacy drained: still 0
  lost, and 284 same-key inversions.
- Migration, the drainer path: 0 of 1168 lost on the cutover, but the rollback
  re-delivered 106 and stranded 161, and there is no reverse drainer.
- Crashes: two populator `kill -9`s cost nothing; a worker `kill -9`
  re-delivered 89 records, the uncommitted window, bounded by the consumer's 5
  second auto-commit interval and not by `concurrency`.
- Dead destination: today the offsets never advance and no counter moves; the
  pool counted 60 of 60 drops with per-destination reasons in about 30 seconds
  while a healthy destination kept delivering.
- Chaos: 0 loss through 14 kills and 3 stalls, and one delivery in four was a
  duplicate, with six wedge windows and a seven-minute total delivery outage
  after the chaos stopped.
- Workgroups: every record delivered by exactly one workgroup, five runs. A
  generation swap through a real barrier cutover delivered 72 of 72 with no
  gaps; duplicates are the old generation's consumption past its barriers, 18
  to 30 per 100 while it drains. A reshard loses nothing as long as the old
  generation is stopped only after `verify` exits 0; stopping early lost 393
  of 600 when done deliberately, none of them unwarned.
- Kerberos: node-rdkafka gives one identity per process, measured, every
  workaround included. KafkaJS with a GSSAPI mechanism over the `kerberos`
  binding did 2 principals at 9 of 9 each and 50 principals at 250 of 250,
  surviving three broker restarts and a two-minute ticket lifetime.

## Open, and who decides

Product (David Tencer), from `~/capsule-corp/bnaas-poc/gw2cto/08-asks-from-product.md`: the failure
contract and its retry window, mandatory or opt-in destination validation,
detach semantics, whether per-object ordering is written in as a target,
whether an account-scoped destination may share a global one's name, whether
overlapping rules keep fanning out, Kerberos scope for v1, acceptable duplicate
windows, a per-account quota, upgrade expectations, the rate and destination
numbers that make sizing real, KPI thresholds, and the SSRF limit.

The citadel design PR (Taylor McKinnon is rewriting it): the engine is backbeat in
Node, not Go; the workgroups mechanism as built rather than per-workgroup
topics; drop FR10 and keep today's fan-out; state reliability as a bounded
retry then a counted drop; close the name-collision hazard; the measured
migration numbers in section 16. The ordered list is
`~/capsule-corp/bnaas-poc/gw2cto/01-design-critique.md` section 5 and the paste-ready comments are in
`~/capsule-corp/bnaas-poc/gw2cto/04-pr383-review-comments.md`.

Engineering, as groundwork before v1: the node-rdkafka bump to 3.2 or later
for cooperative rebalancing, repo-wide because CRR, lifecycle and ingestion
share the consumer; the two `BackbeatConsumer` fixes (the metadata refresh
interval, and guarding the commit path so an ordinary rebalance cannot kill a
worker); the Kerberos image fix, which has its own ticket; and a reverse
drainer before the first customer cutover.

## Related

- Bring the demo up: `bnaas-demo-stack`
- Reproduce a measured result: `bnaas-run-scenario`
- Record the demo: `bnaas-demo-walkthrough`
