<!-- markdownlint-disable MD013 -->

# BNaaS POC handover

The bucket-notifications-as-a-service POC, as it stands on this branch.
Written 2026-09-10 for whoever picks it up next. The stack is
`poc-demo/`, the demo is `tests/functional/demo/`, and the measurements
the acts compare themselves against are `poc-demo/results/`.

## Where this stands

The bucket-notifications-as-a-service POC has done its job: the data plane is
built, run against real Kafka, ZooKeeper, MongoDB and CloudServer, and
measured end to end, and the migration onto it is measured in both
directions. The headline is that nothing loses events, that today's silent
failures become counted ones, and that what is left to settle is wording in
the design document and answers from product, not code. The demo is now a
test suite: bring the infrastructure up with compose, run `yarn ft_test:demo`,
and eight acts drive the whole story end to end while Grafana, Kafka UI and
the ZooKeeper browser show it happening. What remains open in the POC itself
is one pre-existing consumer defect that sets the reliability ceiling, and it
lives in code that replication and lifecycle share, so it is groundwork rather
than pool work.

## Decisions already taken

**Topology.** One internal delivery topic. The populator writes to that one
topic, addressing each record to its destination; a workgroup is a consumer
group over the same topic with a slice filter, so a worker commits records
outside its slice without delivering them. Per-workgroup topics are out. This
is decided, not an option: the built mechanism is the one with measurements,
and the trade it makes (a hash and a commit per record a workgroup does not
own, against G topics and routing logic in the populator) is stated openly.

**The engine.** A backbeat delivery worker in Node, on the existing consumer.
No new service, no second language.

**The migration path.** Drain-then-switch is the default, with the worker
started before the populator switch. The drainer stays for the one case
drain-then-switch cannot handle, a legacy processor that is wedged or whose
destination is dead.

**Assume-destination is out of scope.** It was explored and set aside; its
partial work is pushed as reference only on branch
`poc/S3C-11127-assume-destination` and is not part of the demo or the
consolidated branch.

## The code, and where it is

Everything is on this branch. It sits on top of two segments, both on
`development/9.3` in `scality/backbeat`, and the tree every act runs carries
all of it:

- the delivery pool and workgroups: publish-time addressing in the populator,
  the delivery worker, the producer pool, the probe server and metrics,
  workgroup membership, the slice filter, the ZooKeeper document and its
  loader, the barrier cutover CLI with its drain report, the cutover drainer,
  and the out-of-CI functional suites under `tests/functional/deliverypool/`;
- Kerberos: a pure-JS Kafka client with a GSSAPI binding, per-destination
  credentials, and the per-destination choice of producer stack behind the
  existing destination seam. node-rdkafka stays the default.

```bash
git log --oneline development/9.3..HEAD              # the whole thing
git diff --shortstat development/9.3..HEAD
```

Act 01 prints both segments' commit lists and the file map, and writes them
into `poc-demo/evidence/01-code-and-tests/`.

A third branch holds an assume-destination experiment that was explored and
set aside. It is pushed as reference only, its last commit says so, and it is
not part of the demo or of this branch.

The federation roles for the pool live on their own branch in the federation
repository. CloudServer runs unmodified from `development/9.3`, as a container
of the demo stack.

## The layout

```
poc-demo/
  .env                      PORT_OFFSET, images, topic names, and NODE_BIN.
                            The repository is resolved from the scripts' own
                            location, so no path needs editing.
  docker-compose.yaml       kafka, zookeeper, redis, mongo, prometheus,
                            grafana, kafka-ui, kafka-exporter, cloudserver,
                            zoonavigator, and the krb profile
  prometheus.yml            kafka-exporter, the workers, the populator
  grafana/                  provisioning plus the "BNaaS delivery pool" board
  krb/                      KDC, kerberised broker, ACL setup. The keytabs and
                            krb5.conf.host are generated at run time and are
                            not in git.
  cloudserver/              the config template and the Dockerfile. The
                            rendered config.json is not in git.
  conf/
    templates/              the two configs everything is rendered from
    shims/                  the three preload shims, plus a pidfile one
    generated/              what the renderer writes, and what processes read.
                            Not in git.
  bin/                      stack-up, stack-down, stack-status, wait-ready,
                            topics-create, mongo-init, zk-show, conf-render,
                            and the process scripts: cloudserver, populator,
                            legacy-processor, worker, consumer, driver, check,
                            offsets, bucket, demo-layout
  scenarios/run.sh          thin wrapper: run.sh 04 06, run.sh --list
  results/                  RESULTS.md, the two Kerberos write-ups, and the
                            design critique. What the acts compare against.
  evidence/<act>/           what each act leaves behind. Not in git.
  README.md, HANDOVER.md    the stack, and this file
tests/functional/demo/
  demo.js                   the entry point, `yarn ft_test:demo`
  acts/01..08               one file per act
  lib/                      env, narration, kafka, zookeeper, configs,
                            processes, S3, the checker, the workload driver
.claude/skills/bnaas-*/     the four skills
```

The suite is under `tests/functional/<suite>/`, which is where every other
backbeat functional suite lives, and it is excluded from `ft_test` and from
every CI job. It drives real S3 through `@aws-sdk/client-s3`, which backbeat
already depends on, so it needs nothing the repository does not install.

## The rig, in one picture

```
   aws-sdk, from the suite's own workload driver
              |
              v
    +---------------------+        metadata (oplog)
    |  CloudServer 9.3    |------------------------+
    |  from source        |                        |
    +---------------------+                        v
                                          +------------------+
                                          |  MongoDB rs0     |
                                          |  metadata db     |
                                          +------------------+
                                                   |
                                                   v
                                    +--------------------------------+
                                    |  backbeat queue populator      |
                                    |  reads the oplog, matches each |
                                    |  bucket's rules per destination|
                                    +--------------------------------+
                              legacy path  |            |  pool path
             (no deliveryPool block)       |            |  (deliveryPool block)
                                           v            v
              +--------------------------------+   +---------------------------------+
              | backbeat-bucket-notification   |   | bucket-notification-delivery    |
              | P=4, one record per object     |   | P=3, one ADDRESSED record per   |
              |                                |   | matching destination, keyed by  |
              |                                |   | destination                     |
              +--------------------------------+   +---------------------------------+
                     |            |                     |        |        |
        one process per destination            one consumer group per WORKGROUP,
                     v            v            each with a slice filter over the
              +-----------+ +-----------+      same topic
              | queue     | | queue     |           v        v        v
              | processor | | processor |      +--------+ +--------+ +--------+
              | dest-1    | | dest-2    |      | worker | | worker | | worker |
              +-----------+ +-----------+      | wg-a   | | wg-b   | | wg-pin |
                     |            |            +--------+ +--------+ +--------+
                     |            |              pooled producers, one per
                     |            |              endpoint and credential
                     v            v                  v        v        v
              +----------------------------------------------------------------+
              |  customer-topic-1 .. customer-topic-6                          |
              |  what the tenant's own consumer reads                          |
              +----------------------------------------------------------------+

   Which path runs is decided by ONE thing: whether the config file the
   processes were started with has an extensions.notification.deliveryPool
   block. No rebuild, no image change, no repo change.

   ZooKeeper holds two things the demo shows: the populator's log offset,
   which is its checkpoint, and the workgroups document, which carries the
   generation, the slices, the pins and the barrier offsets.

   Observability: each worker exposes /metrics, /_/live and /_/ready on its
   own port, bound 0.0.0.0; prometheus scrapes those plus kafka-exporter;
   grafana has the dashboard.
```

## What is proven

Full write-ups, with procedures and per-key evidence, are in
`poc-demo/results/RESULTS.md`, which is copied into `poc-demo/`. Two published summaries:

- Findings: https://claude.ai/code/artifact/902d1e9d-6701-45b4-9f91-992dd02adffd
- Meeting brief: https://claude.ai/code/artifact/61b0d204-73b8-49d5-8efe-e2b3e760844e

Cost, and why the redesign exists. One destination today is 12 processes at
about 78 MB, roughly 936 MB, whether or not any traffic flows, and 11 of the
12 are idle standbys. About 56 destinations fit safely on six nodes; the
requirement is 10,000. Pooled, a destination costs about 0.75 MB. Adding one
destination today takes 8 to 22 minutes of playbook, stalls delivery for every
destination for about 121 seconds, needs an S3 API restart, and edits two
static lists.

Migration, the recommended path: 0 of 601 lost on the cutover, and the mirror
rollback lost nothing, duplicated nothing and reordered nothing. The 69
duplicates the rig measured on the cutover come from starting the worker after
the populator switch; starting it first removes them. Done wrong, with the
pool started before the legacy side had drained, nothing was lost and 284
same-key operations arrived out of order.

Migration, the drainer path: 0 of 1168 lost on the cutover with the legacy
offsets untouched, but its rollback re-delivered 106 records and stranded 161
on the delivery topic, and no reverse drainer exists to recover them. Write
one before the first customer cutover.

Running both paths at once does not double-deliver: 657 of 657, zero
duplicates, because the populator's routing is an if/else.

Crashes. Two populator kills mid-stream cost nothing. A worker kill
re-delivered 89 records, exactly the uncommitted window, and the bound is the
consumer's 5 second auto-commit interval, not the configured concurrency,
which is a knob the pool schema does not expose.

Failure visibility, the strongest single result. Today a dead destination does
not drop with the offsets advancing: the processor cannot even start if the
destination is down, and one already running attempts only its concurrency
worth of records, waits out a five minute librdkafka default, never advances
its consumer offset on any partition, writes nothing to the failed topic that
the configuration names, and exposes no counter at all. The pool started
healthy with four dead destinations configured, dropped 60 of 60 with exact
per-destination reasons within about 30 seconds, committed past the drops, and
kept delivering to a healthy destination throughout.

Workgroups. Every record is delivered by exactly one workgroup, five runs. A
generation swap through a real barrier cutover delivered 72 of 72 with no
gaps; the duplicates are the old generation's consumption past its barriers.
A reshard from two workgroups to three moves the destinations whose hash
remainder changes, atomically at the barrier, and loses nothing as long as the
old generation is stopped only after `verify` exits 0. Stopping it early loses
exactly what the drain report was still counting: 393 of 600 when done
deliberately, none of them unwarned. One confirmed gap: a crashed
old-generation worker cannot restart to finish its drain once the document has
been overwritten, and the proposed amendment is one ZooKeeper node per
generation plus a current pointer.

Semantics that change, all measured on both paths: detaching a destination
drops its queued events today and delivers them after the migration; a
catch-all rule plus a prefix rule delivers to both destinations on both paths,
so the design's first-match-wins rule is what neither implementation does; and
an account-scoped ARN whose last segment names a global destination is
accepted with HTTP 200 through the supported API and delivered to the global
destination, because every component matches on the last ARN segment and
discards the account field.

Chaos and rebalance: no loss through 14 kills and 3 stalls, and everything
else about that run was bad. One delivery in four was a duplicate, six windows
where a worker held partitions and delivered nothing while answering liveness
200, and about seven minutes of total delivery outage after the chaos stopped.
One pre-existing defect causes it, below.

Kerberos: with librdkafka it is one identity per OS process, measured, with
every documented workaround tried and failed. The pure-JS alternative
authenticated 2 principals at 9 of 9 each and 50 principals at 250 of 250 in
one process, survived three broker restarts and a two-minute ticket lifetime,
and is landed on the kerberos branch behind the destination seam. Details in
`poc-demo/results/RESULTS-kerberos.md` and `poc-demo/results/RESULTS-kerberos-nodejs.md`.

## What is open, and who decides

**Product** owns thirteen questions, ordered by how much of the design depends
on the answer, in `~/capsule-corp/bnaas-poc/gw2cto/08-asks-from-product.md`. The four that block design
wording: the failure contract and its retry window (retry for a configurable
window then abandon and count, and what the default window is), whether
destination validation on create and update is mandatory or opt-in, whether an
account-scoped destination may carry the same name as a global one, and
whether per-object ordering is written in as a target with the known rebalance
exception. Also detach semantics as a release note, whether overlapping rules
keep fanning out, Kerberos scope for v1, acceptable duplicate windows, a
per-account quota as an operator lever, upgrade expectations, the event rate
and destination counts that make sizing real, and KPI thresholds.

**The design document** needs ten changes before the extended reviewer list,
ordered in `poc-demo/results/design-critique.md` section 5, with the paste-ready
comments in `~/capsule-corp/bnaas-poc/gw2cto/04-pr383-review-comments.md`. The big three: the engine is
a backbeat delivery worker in Node and not a Go binary; the workgroups section
describes a sibling of the mechanism that was built and measured, and the
built one is now the decided topology; and the first-match-wins requirement
should be dropped in favour of today's fan-out, since shipping it literally
would be a silent behaviour change.

**Engineering groundwork**, before v1 rather than inside it: the node-rdkafka
bump to 3.2 or later for cooperative rebalancing, which is repo-wide because
replication, lifecycle and ingestion share the consumer; two `BackbeatConsumer`
fixes, the metadata refresh interval and guarding the commit path so an
ordinary rebalance cannot kill a worker; the Kerberos image fix that the
requirements already assume works, which has its own ticket; and the reverse
drainer.

## How to run it

Three commands, four browser tabs.

```bash
yarn demo:up                # containers, mongo replica set, topics, CloudServer
yarn demo:up:krb            # the same plus the Kerberos KDC and broker, for act 07
yarn demo:wait              # blocks until broker, mongo PRIMARY, S3 and grafana answer
yarn ft_test:demo           # every act, in order

DEMO_ACTS=02,04,06 yarn ft_test:demo        # only these acts, in this order
DEMO_ACTS=06 yarn ft_test:demo:act          # the same thing, one act
DEMO_PACE=fast yarn ft_test:demo            # shorter waits, while iterating
poc-demo/scenarios/run.sh --list            # the acts, by name
yarn demo:status                            # what is up, and the URLs
yarn demo:down                              # stop the containers
```

CloudServer is a container of the stack, published on `CLOUDSERVER_PORT`, and
the suite's before-hook waits for the stack and then finds the endpoint
itself, so nothing needs starting by hand. `poc-demo/bin/cloudserver.sh` still
starts one from a source checkout, as a fallback for a machine where the
container will not run; the suite uses it only when `DEMO_HOST_CLOUDSERVER=1`.
Do not run both: they fight over the same ports.

Grafana <http://localhost:4000> (dashboard "BNaaS delivery pool"), Kafka UI
<http://localhost:9085>, ZooNavigator <http://localhost:10000> for the
ZooKeeper browser, Prometheus <http://localhost:10090>, S3
<http://localhost:9010>. Subtract 1000 from each at `PORT_OFFSET=0`, where the
S3 endpoint is 8010 and ZooNavigator is 9000.

The five destination names the acts use, `poc-dest-1`, `poc-dest-2`,
`poc-dest-3`, `krb-dest-a` and `krb-dest-b`, are the ones the containerised
CloudServer validates a notification configuration against. Backbeat's own
list, which the suite generates, is what decides where a record is actually
delivered, so the same five names carry the healthy destinations, the three
failure classes of act 03 and the six-way spread of act 06.

| act | rig scenario | what it proves | minutes |
|---|---|---|---|
| 01 code-and-tests | the branches | commit lists, the file map, the unit suite and the linter | 5, or 30 with `DEMO_ACT01_FULL=1` |
| 02 legacy-baseline | M1 | today's path end to end, 25 events, the event shape | 2 |
| 03 dead-destination | M10 | the silent stall today, the counted drop on the pool | 6 |
| 04 switch-and-drain | M2b, M4b | the cutover, worker first, and the free rollback | 8 |
| 05 crashes | M11, M12 | two populator kills, then a worker kill | 6 |
| 06 workgroups | W gates, C5r | two workgroups over hashmod modulo 4, a worker death, a pin, a live reshard to three | 15 |
| 07 kerberos | GATE 2 | two Kerberos principals in one process | 10 |
| 08 semantics | M5, M9, M8, M6b | mixed window, detach, overlapping rules, collision | 12 |

Every act is self-contained: it starts the processes it needs as real child
processes, so a scenario can kill one and the audience sees it, stops them
afterwards, writes evidence under `poc-demo/evidence/<act>/`, and ends with
the rig's measurement next to this run's. Its assertions are the design's
promises: no loss anywhere, no reordering except where an act deliberately
shows it, counted drops on the pool, the stall on the legacy path.

Act 01 runs the unit suite and the linter by default. The three functional
suites for the pool, the workgroups and Kerberos are behind
`DEMO_ACT01_FULL=1`, because they take fifteen to thirty minutes between them
and they hardcode `localhost:9092`, so they need a stack at `PORT_OFFSET=0`.
The act says so on screen rather than skipping them silently. Act 07 needs
`yarn demo:up:krb`; without the profile it skips with a clear message rather
than failing.

For ad-hoc work the scripts in `poc-demo/bin/` drive the same processes by
hand: `populator.sh start legacy|pool`, `worker.sh start 1 --workgroup wg-a`,
`legacy-processor.sh start poc-dest-1`, `offsets.sh --watch`, `zk-show.sh`,
`demo-layout.sh` for the four-pane recording layout.

Four Claude Code skills, in `.claude/skills/`: `bnaas-demo-stack` (up, down,
status, URLs), `bnaas-run-scenario` (run acts, read the numbers, tell a real
result from the wedge), `bnaas-demo-walkthrough` (the recording storyline, act
by act, with what to say), `bnaas-poc-state` (this map, in skill form).

## Recording the demo

The pace knob is the whole difference between a run and a take. `slow` doubles
every deliberate wait, which is what gives you time to talk over a step;
`fast` cuts them to about a third, which is for iterating, not for recording.
Disable system sleep first: the containers survive it, but the consumer groups
rebalance and a long act never recovers its narration.

```bash
# once, before the take: infrastructure with the Kerberos profile, then wait
yarn demo:up:krb
yarn demo:wait

# the whole demo, at recording pace. About 90 minutes at DEMO_PACE=slow.
DEMO_PACE=slow yarn ft_test:demo

# the short version, the four acts that carry the argument, about 45 minutes
DEMO_PACE=slow DEMO_ACTS=02,03,04,06 yarn ft_test:demo

# one act, to reshoot it
DEMO_PACE=slow DEMO_ACTS=04 yarn ft_test:demo

# the four-pane terminal layout, if you want the processes on screen
poc-demo/bin/demo-layout.sh
```

Open Grafana, Kafka UI and ZooNavigator before you start, on the URLs above,
and leave the Grafana dashboard on a five-minute window. The suite narrates
itself: every step prints a timestamped plain-language line, and each act ends
with the rig's measurement beside this run's, so your job on camera is the
why, not the what. `bnaas-demo-walkthrough` has the storyline act by act.

Keep the honest parts in. The consumer wedge and the worker exits are measured
pre-existing defects with evidence files, the suite says WEDGE SUSPECTED when
it sees one and restarts that one consumer the way an operator would, and
showing the cure reads better than hiding the symptom.

## A fresh clone

```bash
git clone --branch poc/S3C-11127-demo git@github.com:scality/backbeat.git
cd backbeat
export PATH=$HOME/.nvm/versions/node/v22.22.3/bin:$PATH
yarn install --frozen-lockfile
yarn demo:up          # or demo:up:krb, if you want act 07
yarn demo:wait
yarn ft_test:demo
```

Three things a fresh machine needs that the clone does not carry:

**Two local images.** `poc-ft-kafka:latest` and `ci-mongodb:latest` are
local build artifacts, published nowhere, and the stack cannot come up
without them. That is the one hard prerequisite in the whole handover.
Everything else in `.env` pulls: CloudServer, kafka-ui, kafka-exporter,
ZooNavigator, socat, prometheus, grafana, zookeeper, redis. CloudServer and
ZooNavigator are amd64 only and run under emulation on Apple silicon. The
Kerberos KDC and broker build on first use of the `krb` profile, so that
needs network.

**`yarn install` fails on one native module, and it does not matter.**
`fcntl`, a transitive dependency pinned to an old node-gyp, cannot configure
under Python 3.12 or later, because gyp imports `distutils`, which Python
removed. It is only used by arsenal's file data backend, which the demo does
not touch: the demo runs MongoDB metadata and in-memory data. Everything the
suite needs, `node-rdkafka` included, builds and loads. This is a
pre-existing repository problem, not something this branch introduced. If
you want a clean install, give the build a Python that still has
`distutils`, or `pip install setuptools` into the interpreter node-gyp
picks.

**Kerberos keytabs.** `poc-demo/krb/keytabs/` is empty in git on purpose:
the KDC container writes the keytabs on its first start, so
`yarn demo:up:krb` creates them. Act 07 skips with a clear message if they
are not there, rather than failing.

`PORT_OFFSET` is 1000 in `.env`, which is the machine this was built on.
Nothing about the demo needs that value; set it to 0 on a machine where the
standard ports are free and every derived port follows.


## Known gotchas

**Node 22.** Everything runs on the path in `NODE_BIN`, by default
`$HOME/.nvm/versions/node/v22.22.3/bin`. Node 24 breaks the native modules.

**The three preload shims are not optional on this host.** They live in
`poc-demo/conf/shims/` and every backbeat process loads them with `--require`.
One aliases the oplog field arsenal identifies entries by, which MongoDB 4.2
removed; without it the populator throws on its first batch. One translates
MongoDB 5's update diff format, without which every overwrite and every delete
event is silently lost. One lowers the consumer's topic metadata refresh
interval, without which a joining consumer rebalances every one to two seconds
forever. None of them changes notification semantics. Both paths carry the
identical set, so the comparison is fair, but neither side is stock on this
Mac.

**The consumer wedge, and its cure.** A consumer can become a live group
member holding all its partitions, cycling assign then revoke then "processing
queue idle, un-assigning" about once a second, delivering nothing, with no
delivery counters on `/metrics` and a liveness probe still answering 200. It
fired on 5 of 21 consumer starts in the migration round and 6 times in a
ten-minute chaos loop, and it is not caused by the pool. The cure is to
restart that one consumer, and only that one, which costs up to 45 seconds
while the wedged member's group session expires. Never gate a procedure on lag
alone: gate on lag zero and on progress. The suite does this for you and
prints WEDGE SUSPECTED when progress stops.

**Three things that look like the wedge and are not.** A topic that is still
empty, because the populator's batch cadence is several seconds. A consumer
group with committed offsets on another topic, because `--describe` prints
every topic a group ever consumed, so a reused group id has a total lag that
never reaches zero. And a partition no destination hashes to, which stays
empty and therefore never gets a committed offset: one destination is one
delivery key is one partition, so on a three-partition topic with one
destination two partitions are permanently uncommitted. The suite's lag
figures are per topic, and it only counts an uncommitted partition as
suspicious when that partition actually holds records.

**A worker that exits on its own.** An uncaught error out of the consumer's
commit path kills a standalone worker process on an ordinary rebalance. The
suite restarts it, the way systemd does on a deployment, and counts every
exit. Exits nobody asked for are expected; they are the reason a member death
costs minutes rather than the 45 second session timeout.

**A restarted worker can lose its own probe port.** A probe that cannot bind
is non-fatal by design: the worker keeps delivering, it is just
unscrapeable. That is the right call in production and the wrong one for a
demo, whose numbers come from that endpoint, and a replacement process can
lose the race for the port against the one it replaces. The suite waits for
the probe to answer after every worker start and restarts once if it does
not. If a Grafana panel is empty while the log shows deliveries, this is why.

**Probe servers must bind 0.0.0.0.** Prometheus runs in a container and
scrapes the host, so a probe bound to localhost is unscrapeable and its
Grafana panels stay empty. The renderer does this; a hand-written config is
where it goes wrong.

**Ports.** `.env` holds `PORT_OFFSET`, which is 1000 while the older rig owns
the standard ports on this machine. Two CloudServers on one machine need more
than the S3 port moved: the renderer offsets the metrics port and the daemon
ports too, because with only the S3 port moved CloudServer starts its listener
and then dies on the other one's metrics port.

**The repository's own suites hardcode localhost:9092.** The unit suite also
wants kafka, redis and mongo on the standard ports, and one of its files binds
port 8080, so two full unit runs cannot overlap on one machine. Run act 01's
suites with the stack at `PORT_OFFSET=0`.

**Nobody's leftovers.** The suite recreates every topic it uses and deletes
every consumer group it uses before the first act, because leftover records
and a stale committed offset are exactly what makes a healthy consumer look
wedged and a Grafana panel lie. Give any ad-hoc console consumer its own
throwaway group; never reuse a production group name.

**Do not touch the other rigs.** Compose project `ft` with container
`bnaas-mongo` is the older notification rig; the `bnaaskrb-*` set is the
Kerberos spike; `f9-mongo` on port 27018 belongs to another workstream. The
scripts and the suite exclude all of them by name, and they identify their own
processes by the shim path on the command line.

**The mongo replica-set member host.** The `ci-mongodb` image's own init
script pins the member to `127.0.0.1:27018`, and the address in the
replica-set config is what the driver handshake hands back to clients. Port
27018 belongs to another workstream here, so a naive remap would silently
point CloudServer and backbeat at the wrong database. The demo runs mongod on
its own port with a matching member host.

**Topics before consumers.** A consumer that subscribes to a topic which does
not exist yet, or whose partitions have no leader yet, is one of the wedge
triggers. Both `topics-create.sh` and the suite confirm leadership three times
running before anything consumes.

**A deleted topic can come back with one partition.**
`auto.create.topics.enable` is on, so any producer that touches the name in
the moment between a delete and the re-create wins the race and the topic
reappears with the broker default of one partition. The delivery topic then
has one partition instead of three, no leader count ever satisfies a waiter,
and a setup that polls for leadership hangs. The suite now grows such a topic
back with an alter and says so; if you delete topics by hand, check the
partition count afterwards.

**The wedge is handled, not hoped about.** Every consumer the suite starts is
watched for the churn signature for the first few seconds, and one that shows
it is restarted once or twice, narrated as it happens. That is the operator
cure, and on this machine it fires often enough that a demo without it would
stall on a fresh worker.

**A fresh legacy consumer group starts at `latest`.** The legacy processor
builds its consumer with no `fromOffset`, so a group that has never committed
can skip what is already on the topic, and its first-join revoke can move it
past records published in between. Every act that measures through a legacy
processor warms its group with a few operations first and waits for committed
offsets on every partition, which is the rig's own method note. Without it a
measurement shows loss that the pipeline did not cause.

**Straddle keys.** Only a key with several operations can show an ordering
defect, so the workload driver makes every fifth operation a PUT then DELETE
on one of a few fixed keys. A run without them cannot measure ordering at all,
and a run whose straddle interval lines up with the number of buckets puts
every straddle key in one bucket and can only measure ordering on one
destination.

**macOS sleep** ruins a long act: the containers survive but the consumer
groups rebalance. Disable sleep before a take.

## The demo storyline

Eight acts, in `.claude/skills/bnaas-demo-walkthrough/SKILL.md`, with what to
point at and what to say at each step. In short: here is the code and its
tests; today's path and what it costs; the failure that is invisible today and
counted by the pool; the migration and its free rollback; crashes; workgroups
and ZooKeeper, including a live reshard through the barrier; Kerberos; and the
four semantics that change, each ending in a decision. Close on the decisions
rather than more demo.

## The design document

Taylor is rewriting the design PR. The two files written for that are
`~/capsule-corp/bnaas-poc/gw2cto/04-pr383-review-comments.md`, which is paste-ready comment by comment
with the line numbers of the version it was written against, and
`poc-demo/results/design-critique.md`, whose section 5 is the ordered list of changes
and whose section 6 is the measured migration table. The CTO document itself
is `~/capsule-corp/bnaas-poc/gw2cto/GW2CTO-BNaaS.md`, in the house template, and it is the one place
that carries the ticket ids this handover leaves out.

## Knowledge proposal

The code-verified facts about today's pipeline, in the vault's own format with
source pointers, are proposed at
`~/capsule-corp/knowledge/proposals/bnaas-gw2cto/verified-facts-2026-09-09.md`.
It has not been merged into the vault: only the review flow does that.

## First day, for Taylor

1. Read `~/capsule-corp/bnaas-poc/gw2cto/GW2CTO-BNaaS.md` end to end, then the summary table at the top
   of `poc-demo/results/RESULTS.md`. Half an hour, and it is the whole argument.
2. Bring the infrastructure up and run one act, to have the pipeline under
   your own hands: `yarn demo:up` then `yarn demo:wait`,
   then `DEMO_ACTS=02 yarn ft_test:demo`. Five
   minutes from cold, and it ends with 25 events and a clean checker.
3. Run `DEMO_ACTS=03 yarn ft_test:demo`. It is the result that carries the
   redesign, and the halves are side by side.
4. Run `DEMO_ACTS=06 DEMO_PACE=slow yarn ft_test:demo` with the ZooKeeper tab
   open. That is workgroups, and it is the part of the design that is hardest
   to explain from a document.
5. Read `~/capsule-corp/bnaas-poc/gw2cto/08-asks-from-product.md` and pick the four you want answered
   before the design document goes wider. They are already ordered.
6. Take `~/capsule-corp/bnaas-poc/gw2cto/04-pr383-review-comments.md` into the design rewrite. The
   comments are in co-author voice, so they can go in as they are, but check
   the line numbers against the current revision first.
7. If you record the demo, follow the walkthrough skill and keep the honest
   parts in. The wedge and the worker exits are measured pre-existing defects
   with evidence files, and showing the cure reads better than hiding the
   symptom.

## For the integrator: where the assumptions live

Every value below has a default in code and an override in `poc-demo/.env` or the
environment. These are the files to read before the first run on another
machine.

| file | what to check |
|---|---|
| `poc-demo/.env` | `PORT_OFFSET` and the derived ports, the images, `NODE_BIN`, `CLOUDSERVER_DIR` (only the build fallback needs it), the topic names, `DELIVERY_PROBE_PORT_BASE`. Read this one first. |
| `tests/functional/demo/lib/env.js` | the default behind every knob: port bases (kafka 9092, zookeeper 2181, redis 6379, mongo 27117, S3 8010, prometheus 9090, grafana 3000, kafka-ui 8085, krb 19095 and 19096), worker probe base 8920, populator probe 8910, backbeat API 8901, the topic and group names, the two ZooKeeper paths, the S3 keys, six customer topics, compose project `bnaasdemo`, and the node 22 path |
| `tests/functional/demo/lib/conf.js` | the five destination names, which must match the CloudServer container's own list; the failure endpoints (a refused port, an unroutable address); `deliveryTimeoutMs`, `producerIdleMs`, `concurrency`; the customer-topic fallback naming |
| `tests/functional/demo/lib/kafka.js` | the container deny list for the other rigs, the `<project>-<service>-1` naming, the kafka CLI paths inside the broker, and the assumption that the CLI's bootstrap inside the container is `localhost:<KAFKA_PORT>` |
| `tests/functional/demo/lib/zk.js` | `localhost:2181` inside the zookeeper container, and where zkCli lives |
| `tests/functional/demo/lib/procs.js` | the shim list, the three environment variables a worker is handed, the two second auto-restart delay and the six second SIGTERM grace before KILL |
| `tests/functional/demo/lib/wait.js` | the sixty second stall limit that calls a wedge, and the poll intervals |
| `tests/functional/demo/acts/03-dead-destination.js` | which destination plays which failure class, the leaderless topic and its `--replica-assignment 99`, and the stall watch default |
| `tests/functional/demo/acts/06-workgroups.js` | the hashmod modulo and the remainder split, which follow from the md5 of those five destination names: rename a destination and the mapping changes |
| `tests/functional/demo/acts/01-code-and-tests.js` | the file map, the expected suite counts, and that the three gated functional suites need `PORT_OFFSET=0`. The branch names are read off the repository, not hardcoded. |
| `tests/functional/demo/acts/07-kerberos.js` | the krb container names, the test image, the node_modules volume, and where the keytabs and krb5.conf are |
| `poc-demo/bin/lib.sh` | the same ports and names again, for the operator scripts |
| `poc-demo/conf/templates/*.json` | the two configs everything is rendered from; their destination list is where the five names come from |
| `poc-demo/grafana/dashboards/bnaas-delivery-pool.json` | its two textbox variables default to the canonical topic and group names, so a run with `DEMO_TOPIC_SUFFIX` needs them typed in |

## What has been run, and what has not

The suite was built and run against the demo stack at `PORT_OFFSET=1000` on
2026-09-10, with the containerised CloudServer. The command every run used, which is what `yarn ft_test:demo` runs:

```bash
export PATH=$NODE_BIN:$PATH
DEMO_ACTS=02,03 DEMO_PACE=fast DEMO_STALL_WATCH_S=60 yarn ft_test:demo
```

Green end to end:

- **act 02, three times**, the last two with the group warm-up: 25 events, no
  loss, no duplicates, no reordering, the golden event with `size` as a
  string, one assign and no revoke on the processor. Every row of its table
  matched the rig.
- **act 04, both halves**: the cutover delivered 94 events with 0 gaps and 0
  duplicates and left the legacy group committed exactly at the frozen
  internal-topic head; the mirror rollback lost nothing, duplicated nothing,
  re-delivered nothing from the cutover window, and left the delivery topic
  frozen. One same-key inversion appeared in each half, on the straddle keys,
  which is the "0 or 1" the table predicts.
- **act 03's legacy half**: the processor for the refused destination cannot
  start and exits with the setup failure in its log, and the leaderless
  destination's processor holds all four partitions with no committed offset
  on any of them while the failed topic stays at zero. Its pool half and its
  verdict table are the part that still needs a clean pass.
- **act 06, partly**: the generation 1 document written by the real cutover
  tool, both workgroups carrying load, the mapping from the repository's own
  ownership function, the pin cutover producing a barrier on every partition
  and pre-seeding all three new groups at those offsets, and `verify` printing
  the drain report and refusing with exit 2 while the old generation was
  behind its barriers. It also showed the slice filter working on the healthy
  side, 85 delivered and 374 skipped.

Also verified: the JS checker reproduces the rig's Python checker exactly on
the rig's own evidence (M4b 602 events 0/0/0, M2b 601 expected with 69
duplicates and 1 inversion, M12 89 duplicates, M5 657 of 657), which is why
its numbers can be compared with `poc-demo/results/RESULTS.md` directly.

Not yet green end to end: acts 01, 05, 07 and 08, act 03's pool half and act
06's reshard half. They are written against the measured procedures,
syntax-checked, and built on the same library the green acts use. Act 07 needs
the krb profile and skips cleanly without it. Act 01's test runs need
`PORT_OFFSET=0`, because the repository's own suites hardcode localhost:9092.

Five things the dry runs found and fixed, all of which look like product
defects and are not:

1. a lag of zero right after a workload means "nothing published yet", not
   "drained", and a topic that has never moved is not settled either;
2. an empty partition never gets a committed offset, so treating every
   uncommitted partition as a wedge symptom made every pool drain look
   wedged;
3. a fresh legacy consumer group starts at `latest` and can skip what is
   already on the topic, which showed as loss until every act warmed its
   group first;
4. a deleted topic can be auto-created by the next producer with the broker
   default of one partition, which made a leader wait hang forever until the
   suite learned to grow it back;
5. the design/06 consumer wedge fires often enough on this machine that the
   suite now watches every consumer it starts and restarts a churning one,
   which is the operator cure. It also learned the difference between the
   wedge and plain idling: a consumer with nothing to read cycles assign,
   revoke and "processing queue idle, un-assigning" in exactly the same way,
   so the cure only fires when records were waiting and none moved. Without
   that test the cure restarts a healthy idle worker in a loop, which is
   worse than the disease;
6. a restarted worker can lose the race for its own probe port, which leaves
   it delivering happily with no counters to read, so the suite waits for the
   probe and restarts once if it never answers;
7. a worker does not reliably die on SIGTERM, so a restart that only asked
   politely left the old process holding the group and the probe port while
   its replacement could bind neither. Stopping now escalates to KILL, which
   is also what an operator has to do;
8. and a killed consumer keeps its group membership until its session
   expires, 45 seconds by default. The cutover tool refuses to pre-seed a
   group that still has members, correctly and with a clear message, so a run
   that has just killed workers must wait that out. The suite now waits for
   every group of the demo to be memberless before the first act, and act 06
   fails fast on a cutover that did not exit 0 rather than carrying on into a
   half-seeded generation.

One finding worth your attention, and it is now the third sighting of the
same thing. A same-key inversion on the LEGACY path, with no migration in
progress:

1. the rig's own M2b, one inversion in roughly 1200 legacy-delivered events,
   root cause not established;
2. a twenty-operation baseline on a fresh demo stack: the driver did the PUT
   and the DELETE 10 milliseconds apart and the processor issued the two
   sends 6 milliseconds apart in the wrong order, on `base-strad-4`;
3. the cutover act, on `m2b-strad-1`, and again in its rollback half.

Same signature every time: two operations on one key, both delivered by the
legacy processor, inverted in the first consume burst after an assignment. It
supports what the CTO document already says, and it is the reason the
ordering claim is worded as it is: per-key ordering is a property of the
shared consumer, and it should be described as a target with a known rare
exception rather than as a guarantee today's pipeline already gives. Evidence
is in `poc-demo/evidence/02-legacy-baseline*/checker-dest1.json` and
`poc-demo/evidence/04-switch-and-drain/checker-cutover.json`.

State the stack was left in: containers up, including CloudServer; the
backbeat processes stopped, because the suite stops what it starts. If a demo
process is ever left behind, find it by the shim path on its command line
(`poc-demo/conf/shims/oplog-h-shim.js`) and never by the script name alone:
the older rig runs the same scripts with its own shim paths, and a broad
`pkill -f deliveryWorker/task.js` will take its processes down too.
