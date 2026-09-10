<!-- markdownlint-disable MD013 -->

# FROZEN: the demo stack infrastructure

> State at the 2026-09-10 21:00 freeze, carried onto the branch verbatim from the working copy.
> `HANDOVER.md` and `PLAYBOOK.md` supersede it: the .env pins, the shim paths and the act statuses below have moved on since.

The infrastructure half of this directory is frozen and ready to sync onto
the branch. It is validated, it lints clean, and I am making no further
changes to the files listed below unless someone asks for one.

## What this does and does not cover

Frozen: the compose stack, its Prometheus and Grafana configuration, the
`bin/stack-*` scripts plus `wait-ready.sh`, `topics-create.sh`,
`mongo-init.sh` and `zk-show.sh`, the CloudServer config template, and the
Kerberos profile. The manifest below is the exact list.

Not covered, and not mine to freeze:

- the demo suite under `tests/functional/demo/`
- the process scripts in `bin/` other than the ones in the manifest, plus
  `conf/`, `scenarios/`, `evidence/`, `logs/` and `run/`

Those belong to other workstreams. Ask their owners before syncing them.

## This does NOT mean the stack is idle

A freeze on files says nothing about the running containers. Compose project
`bnaasdemo` is up and other people run against it. Before touching it,
check that no act run is in flight:

```sh
ps -eo pid,etime,command | grep -E 'mocha|DEMO_ACTS' | grep -v grep
docker exec -i bnaasdemo-kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:10092 --list
```

A live run shows as a mocha process and as consumer groups named
`bucket-notification-delivery-group-<workgroupId>-gen<N>`. Deleting groups
under a live run invalidates it without looking like a failure: the
symptoms are a cutover that lost its pre-seeded offsets, or a workgroup
consuming from latest. That has already happened once.

## Manifest

Sixteen-character SHA-256 prefixes, so a sync can be checked file by file.

```
1529badeb5a08950  docker-compose.yaml
496775e0d8db4d08  .env
7e9ea8d6d39eaf29  prometheus.yml
ea308fa12ded07df  kafka-entrypoint.sh
4fd020334a611ae8  README.md
f8529f8e4e27ce90  bin/_common.sh
0295b2efc246a068  bin/stack-up.sh
5bbe03b6a74aa91b  bin/stack-down.sh
d7923b67036cc7f2  bin/stack-status.sh
7798735bfd86e842  bin/wait-ready.sh
e3f7729417b3a832  bin/topics-create.sh
b1ecccf022517d7f  bin/mongo-init.sh
3c4061ec511235f9  bin/zk-show.sh
3ca249f74eb4a96d  cloudserver/config.json.tmpl
b2d6ea9291033a6d  cloudserver/Dockerfile
c400aa109682503a  grafana/provisioning/datasources/prometheus.yaml
b3c7358588f44b92  grafana/provisioning/dashboards/dashboards.yaml
4cb7271e1b32ea3d  grafana/dashboards/bnaas-delivery-pool.json
0fdd1e74ad8f1278  krb/Dockerfile.kdc
ee04d71e113fa3d6  krb/kdc.conf
1d6de222657b9c2b  krb/kdc-entrypoint.sh
4bf476e9f64f992c  krb/krb5.conf
a0ff2166c4137c72  krb/krb5.conf.host.tmpl
602ab1e9a4989aae  krb/server-kerberos.properties.tmpl
637aec736c65c752  krb/broker-entrypoint.sh
f692b5173aeff448  krb/setup-topics-acls.sh
197bd2d9cceacc8e  krb/kafka/Dockerfile
```

## Generated at run time, do not commit

These are rendered or written by the scripts and must not be tracked. Add
them to `.gitignore` when the folder lands on the branch.

| path | written by |
|---|---|
| `cloudserver/config.json` | `stack-up.sh`, from the `.tmpl` |
| `krb/krb5.conf.host` | `stack-up.sh`, from the `.tmpl` |
| `krb/keytabs/*.keytab` | the KDC container on first start |

The keytabs hold keys for the fixed test passwords in
`krb/kdc-entrypoint.sh`. They are throwaway test material, but they are
still keys, and they are regenerated on demand, so there is no reason to
track them.

## Changes the branch layout needs

Three, all in `.env`, and all yours to make since they depend on where the
folder sits:

- `CLOUDSERVER_DIR` becomes `../cloudserver` once this folder is at the
  repository root. It is `../../scality/cloudserver` here, correct for its
  current location. It is only read by the opt-in `cloudserver-build`
  profile.
- `BACKBEAT_DIR` can go. It belongs to the process scripts, and their
  `bin/lib.sh` carries its own fallback, so removing it changes no
  behaviour.
- `PORT_OFFSET` goes to 0 when the older rig stops holding the standard
  ports. Everything else derives from it, so that is the only edit.

Nothing else in the manifest carries a machine-specific path. Every compose
bind mount is relative to the compose file.

## Lint

`yarn lint` is `eslint $(git ls-files '*.js')` and the manifest contains no
JavaScript, so it has nothing to say about any of this.

`yarn lint_md` runs `mdlint`, CI runs it as a required step, and the
repository's own markdown passes at zero violations. `README.md` was at
fifteen MD013 line-length errors and is now at zero, checked with the
repository's own `node_modules/.bin/mdlint`. Keep it that way: the 80-column
limit applies to table rows too, which is why the scripts section is a
bulleted list and the metrics table factors out its shared prefix.

## Images a fresh machine needs

Pulled, and pinned in `.env`: CloudServer, kafka-ui, kafka-exporter,
ZooNavigator, socat, prometheus, grafana, zookeeper, redis. CloudServer and
ZooNavigator are amd64 only and run under emulation on Apple silicon.

Built on first use of the `krb` profile, so both need network: the KDC from
Debian plus MIT krb5, and the Kerberos broker from Alpine plus Kafka 3.4.0
from `archive.apache.org`.

**Not published anywhere**: `poc-ft-kafka:latest` and `ci-mongodb:latest`
are local build artifacts. A fresh machine has to build them first, and the
stack cannot come up without them. That is the one hard prerequisite in the
whole handover.

## Validated

At `PORT_OFFSET=1000`, on Docker Desktop, arm64. The README carries the
detail; in summary:

- Topics created before any consumer, every partition leader-confirmed.
  Topics, records and committed offsets survived a container restart.
- Mongo `rs0` PRIMARY with its member host on the mongod port. The
  containerized CloudServer reached it through the socat alias, and a real
  create-bucket, PUT, head-object and list all succeeded, with the bucket
  visible in the compose mongo. Notification ARNs naming a configured
  destination were accepted and an unknown one refused.
- All 22 queries across the 15 dashboard panels returned data, the
  Workgroups row included, with generation-shaped consumer groups showing
  the old generation drained to zero while new ones carried lag.
- `zk-show.sh` printed a three-workgroup document with its generation, the
  derived group id per workgroup, the barriers, and the live populator
  offset. `wait-ready.sh` passes all four checks and fails in six seconds
  naming the one that is missing.
- The port preflight refused to start while another project held the ports,
  naming each owner, and created nothing.
- Kerberos: the broker logged
  `Successfully authenticated client: authenticationID=notifa@SCALITY.TEST`
  over SASL_GSSAPI on the offset port, messages read back over the plaintext
  listener, and the negative control was refused with
  `TOPIC_AUTHORIZATION_FAILED`, leaving that topic at offset 0.

Not validated: `PORT_OFFSET=0`, because the older rig holds those ports
here. The derivation was exercised by switching `.env` between 0 and 1000
and checking every value. The `cloudserver-build` profile parses but was
never built, since the published image pulls.

## Suite: the demo test suite

The suite half of this directory is frozen as of 2026-09-10, 21:05 local.
These files are the demo: `yarn ft_test:demo` runs them and what it prints
is what gets recorded. I am making no further changes unless asked.

Frozen here, and not covered by the manifest above:

- `tests/functional/demo/` in full: `demo.js`, `lib/` and `acts/`
- the process scripts `bin/lib.sh`, `bucket.sh`, `check.sh`,
  `cloudserver.sh`, `conf-render.sh`, `consumer.sh`, `demo-layout.sh`,
  `driver.sh`, `legacy-processor.sh`, `offsets.sh`, `populator.sh` and
  `worker.sh`
- `conf/templates/` and `conf/shims/`
- `scenarios/run.sh`
- the four skills and `HANDOVER.md`, which live outside this folder

`conf/generated/`, `evidence/`, `logs/` and `run/` are run output. They
must not be committed.

### Act-by-act status

| act | status |
|---|---|
| 01 code-and-tests | written, never run here |
| 02 legacy-baseline | green end to end |
| 03 dead-destination | written, reached step 7, no verdict |
| 04 switch-and-drain | green end to end |
| 05 crashes | written, never run to completion |
| 06 workgroups | 2 of 4 tests green, 2 failed from outside |
| 07 kerberos | written, never run here |
| 08 semantics | written, never run here |

The three that carry a measurement:

- Act 02 delivered 25 of 25 events with no gaps, no duplicate extras and
  no per-key inversions, and the event shape matched the rig row for row.
- Act 04 cut over and rolled back with no loss either way. It measured one
  per-key inversion in each direction, where the rig recorded zero or one
  on the cutover and zero on the rollback. That rollback row prints
  DIFFERS and is the known-noisy one.
- Act 06 proved the slice filter and the failure isolation. The static pin
  and the live two to three reshard are unproven. Both failed because
  another run of this same suite deleted the pre-seeded consumer groups
  while the act was running.

### Anything act 06 printed after 18:44:50 local is suspect

The demo broker log records the deletion:

```text
18:44:52,279 The following groups were deleted:
  bucket-notification-delivery-group-wg-b-gen2, 3 offsets removed
18:44:54,161 The following groups were deleted:
  bucket-notification-delivery-group-wg-pin-gen2, 3 offsets removed
```

The generation-2 cutover had seeded both groups 35 seconds earlier. The
wg-b worker then started, found no seeded offsets, and crash-looped on
`assertSeededOffsets` with `InternalError`, which failed the pin test. The
reshard test failed next for the same cause: its drain report shows
committed `-1` on all three partitions of both deleted groups, so verify
refuses to let the old generation stop, correctly and forever. The group
for wg-a survived only because it had a live member, and Kafka refuses to
delete a non-empty group.

Treat the pin and reshard evidence under `evidence/06-workgroups/` as
void. Only a rerun that owns the broker alone proves them.

### Exact command lines

Once the folder sits in the repository, from the repository root:

```sh
export PATH=$HOME/.nvm/versions/node/v22.22.3/bin:$PATH
yarn ft_test:demo                                  # every act, in order
DEMO_ACTS=06 yarn ft_test:demo                     # one act
DEMO_ACTS=02,04 DEMO_PACE=slow yarn ft_test:demo   # slower, for recording
```

What was actually run here, before that layout existed. The working copy
is the one holding the pool and workgroups code, and the suite path is
absolute because it lived outside the repository:

```sh
DEMO_ACTS=02,03 DEMO_PACE=fast node_modules/.bin/mocha \
  <demo>/tests/functional/demo/demo.js --timeout 3000000 --exit

DEMO_ACTS=04 DEMO_PACE=fast node_modules/.bin/mocha \
  <demo>/tests/functional/demo/demo.js --timeout 3000000 --exit

DEMO_ACTS=06 DEMO_PACE=fast DEMO_ACT06_SECONDS=1500 \
  node_modules/.bin/mocha \
  <demo>/tests/functional/demo/demo.js --timeout 3000000 --exit
```

`scenarios/run.sh` is the operator wrapper over the same thing:
`run.sh --list`, `run.sh 06`, `run.sh legacy-baseline`.

The knobs worth knowing: `DEMO_ACTS` picks acts, `DEMO_PACE` is slow,
normal or fast, `DEMO_ACT06_SECONDS` is the workgroups load window,
`DEMO_WORKGROUPS_STOP_EARLY=1` turns act 06 into the loss variant that
stops the old generation before verify exits 0,
`DEMO_RECREATE_TOPICS=all` also recreates the customer topics, and
`DEMO_HOST_CLOUDSERVER=1` falls back to a host CloudServer instead of the
container.

### Files that carry a hardcoded assumption

Every value has a default in code and an override in `.env` or the
environment. Read them in this order.

- `.env` pins this machine in three lines: `BACKBEAT_DIR` as an absolute
  path, `NODE_BIN` as an nvm node 22 path, and `PORT_OFFSET=1000`. Inside
  the repository the first can go, because the suite resolves the
  repository root from its own location.
- `conf/shims/*.js` pinned a machine path, and this is the worst-shaped
  bug in the handover. All three carried `const REPO = '<absolute path>'`
  and required their target from that tree's `node_modules`, arsenal's
  `LogConsumer` for the two oplog shims and node-rdkafka for the metadata
  one. Loaded from any other checkout the require still succeeds, because
  that other tree exists on this machine, so the shim prints `installed`
  and patches a module the running process never loads. The populator then
  dies on its first batch with `Cannot read properties of undefined
  (reading 'toString')` out of `LogReader._processReadRecords`, the
  internal topic stays at zero, and the act waits out its window for
  records that cannot come. Fixed on the branch by resolving the target
  the way the preloaded process does: `process.cwd()`, then
  `BACKBEAT_DIR`, then the shim's own repository, then node's walk-up.
  **The three copies in this folder are superseded by the branch
  versions. Do not sync them back over the fix.** My earlier claim that no
  machine path survived in code was wrong for exactly these three files.
- `tests/functional/demo/lib/env.js` holds the default behind every knob:
  port bases for kafka 9092, zookeeper 2181, redis 6379, mongo 27117, S3
  8010, prometheus 9090, grafana 3000, kafka-ui 8085, kerberos 19095 and
  19096, worker probe base 8920, populator probe 8910, backbeat API 8901,
  the topic and group names, the two ZooKeeper paths, the S3 keys, the six
  customer topics, and the compose project name `bnaasdemo`.
- `lib/conf.js` holds the five destination names, `poc-dest-1`,
  `poc-dest-2`, `poc-dest-3`, `krb-dest-a` and `krb-dest-b`. They must
  match the CloudServer container's own list, or a PUT is refused. It also
  holds the two failure endpoints, `deliveryTimeoutMs`, `producerIdleMs`
  and `concurrency`.
- `lib/kafka.js` holds the deny list that protects the other rigs on this
  machine, the `<project>-<service>-1` container naming, the CLI paths
  inside the broker, and the assumption that the in-container bootstrap is
  `localhost:$KAFKA_PORT`.
- `lib/zk.js` holds `localhost:2181` inside the zookeeper container and
  the zkCli path.
- `lib/procs.js` holds the shim list, the three environment variables a
  worker is handed, the two second restart delay, and the six second
  SIGTERM grace before KILL.
- `lib/wait.js` holds the sixty second stall that counts as a wedge.
- `acts/06-workgroups.js` holds the modulo-4 workgroups document and the
  remainder split, wg-a on 0 and 1 and wg-b on 2 and 3. That split is not
  arbitrary: it follows from the md5 of those five destination names, and
  it is what makes both slices carry load and makes a two to three reshard
  move exactly one destination. Rename a destination and the mapping
  changes, so re-derive the split before editing either list.
- `acts/03-dead-destination.js` holds which destination plays which
  failure class and the leaderless topic's `--replica-assignment 99`.
- `acts/01-code-and-tests.js` holds the branch names, the file map, the
  expected suite counts, and the fact that the repository's own suites
  need `PORT_OFFSET=0`.
- `acts/07-kerberos.js` holds the Kerberos container names, the test
  image, the node_modules volume, and the keytab and krb5.conf paths.
- `bin/lib.sh` repeats the same ports and names for the operator scripts.
- `conf/templates/backbeat-notification.json` and
  `conf/templates/cloudserver-config.json` are where the five destination
  names originate.
- `grafana/dashboards/bnaas-delivery-pool.json` defaults its two textbox
  variables to the canonical topic and group names, so a run with
  `DEMO_TOPIC_SUFFIX` needs them typed in.

### Known weakness left in, worth fixing before the next long run

`procs.waitReady` decides a process is up from a line in its log and then
stops watching it. If the process exits after that line, the act sits in
`topicAtLeast` or `frozen` for the whole window and reports a timeout
rather than the exit. Seen for real: the populator logged `notification
extension is active`, waitReady returned "up after 2s", the process exited
code 1 three seconds later, and the act waited sixty seconds for records
from a dead process. A liveness check inside the record-wait loops turns
that into an immediate and accurate failure. Not changed here, because the
suite is frozen and the runs that matter were green, but it is the first
thing I would fix.

### The seven harness fixes, in case a symptom comes back

1. CloudServer would not start on a second offset: only `port` was being
   offset. Every port in the rendered config is offset now, metrics,
   daemons, clients and backbeat included.
2. Group lag never reached zero when a group carried a stale committed
   offset on another topic, so every lag, committed and unknown figure is
   computed per topic.
3. Every pool drain looked wedged because an empty partition has no
   committed offset. Unknown partitions count only when their
   LOG-END-OFFSET is above zero.
4. The drain gate returned lag zero before anything had been published,
   which reported phantom gaps. It now needs a floor: `topicAtLeast`,
   `frozen({atLeast})` and at least one assigned partition.
5. A fresh legacy consumer group starts at `latest` and skipped the
   measured window, so acts 02 and 08 warm the group first, the way the
   rig did.
6. Setup hung for minutes when a deleted topic was auto-created with one
   partition. Topic recreation is one batched delete, one list poll,
   bounded leader confirmation, and a partition grow for anything that
   came back short.
7. Consumers wedge on most starts with the design/06 signature, so the
   suite cures them by restarting that one worker. The cure fires only
   when records actually arrived or the group has lag, because benign
   idling logs the same lines.

Three later additions belong with them: `Proc.stop()` escalates SIGTERM to
SIGKILL after six seconds, because SIGTERM does not reliably stop a
worker and six were leaked; `flow.waitForProbe` restarts a worker once
when its probe lost the race for its port; and setup waits for the
delivery groups to be memberless before a cutover pre-seeds them, because
a killed consumer keeps its membership for about 45 seconds and the
cutover refuses to seed a group that has members.

### A pkill that reached the wrong rig

An unqualified `pkill -9 -f "deliveryWorker/task.js"` of mine matched the
older rig's workers as well as the demo's. Their supervisor restarted
them, so nothing was lost, but it could have been. Match demo processes by
the shim path only, never by the task name:

```sh
pkill -f 'poc-demo/conf/shims/oplog-h-shim.js'
```
