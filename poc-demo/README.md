<!-- markdownlint-disable MD013 -->

# BNaaS demo stack

**The demo is a test suite.** Three commands, from the repository root:

```bash
yarn demo:up                     # docker compose: the whole infrastructure
yarn demo:wait                   # blocks until broker, mongo, S3 and grafana answer
yarn ft_test:demo                # eight acts, end to end, about 60 minutes
```

Then open these beside the terminal, at the default `PORT_OFFSET=1000`:

| what | url | what to look at |
|---|---|---|
| Grafana | <http://localhost:4000> | dashboard "BNaaS delivery pool" |
| Kafka UI | <http://localhost:9085> | the internal topic, the delivery topic, the consumer groups |
| ZooNavigator | <http://localhost:10000> | `/bnaas-demo/delivery-workgroups` and `/bnaas-demo/queue-populator` |
| Prometheus | <http://localhost:10090> | the raw worker and populator metrics |
| S3 endpoint | <http://localhost:9010> | `accessKey1` / `verySecretKey1` |

Subtract 1000 from each at `PORT_OFFSET=0`, where the S3 endpoint is 8010 and
ZooNavigator is 9000.

`DEMO_ACTS=02,04,06 yarn ft_test:demo` picks acts, `DEMO_PACE=slow` slows the
waits for a recording and `DEMO_PACE=fast` speeds them up while iterating.
Act 07 needs the Kerberos profile, so use `yarn demo:up:krb` if you want it;
without it that act skips and says why. The suite is `tests/functional/demo/`
and it is out of every CI glob. `HANDOVER.md` in this directory is the map,
including the exact commands for recording the demo; `results/` holds the
measurements the acts compare themselves against.

One `docker compose` brings up everything the bucket-notification pipeline
needs, plus the observability that makes a legacy-to-delivery-pool cutover
visible while it happens. Nothing runs on the host except the test suite and
the backbeat processes it spawns.

The `bin/` scripts below can be run from this directory instead of through
yarn, and they take the same options: `bin/stack-up.sh --krb`,
`bin/stack-status.sh`, `bin/stack-down.sh`. Requires Docker Desktop and, for
the suite, Node 22.

Every path in this folder is relative to the folder itself. The repository is
found from the scripts' own location, so nothing needs editing on a new
machine except `NODE_BIN` in `.env` if Node 22 lives somewhere else.

## What runs where

| service | image | container port | host port at offset 1000 | at offset 0 |
|---|---|---|---|---|
| zookeeper | `zookeeper:3.9.4` | 2181 | 3181 | 2181 |
| kafka | `poc-ft-kafka:latest` | `${KAFKA_PORT}` and 29092 | 10092 | 9092 |
| redis | `redis:7-alpine` | 6379 | 7379 | 6379 |
| mongo | `ci-mongodb:latest` | `${MONGO_PORT}` | 28117 | 27117 |
| cloudserver | `ghcr.io/scality/cloudserver:9.3.20` | 8000 | 9010 | 8010 |
| prometheus | `prom/prometheus:v2.54.1` | 9090 | 10090 | 9090 |
| grafana | `grafana/grafana:11.3.0` | 3000 | 4000 | 3000 |
| kafka-ui | `provectuslabs/kafka-ui:v0.7.2` | 8080 | 9085 | 8085 |
| zoonavigator | `elkozmon/zoonavigator:1.1.2` | 9000 | 10000 | 9000 |
| kafka-exporter | `danielqsj/kafka-exporter:v1.7.0` | 9308 | 10308 | 9308 |
| krb KDC (`krb`) | from `krb/` | 1088 | 2088 | 1088 |
| krb broker (`krb`) | from `krb/kafka/` | `${KRB_BROKER_PORT}` | 20095 | 19095 |
| krb plaintext (`krb`) | same | `${KRB_VERIFY_PORT}` | 20096 | 19096 |

The queue populator, the delivery workers and the legacy queue processors run
**on the host**, spawned by the suite. Everything they depend on is in the
compose file. `stack-up.sh` and `stack-status.sh` print the URLs for the
current offset.

### Two Kafka listeners, and why

The broker advertises two names for itself:

- `PLAINTEXT://localhost:${KAFKA_PORT}` for host processes, published 1:1.
- `INTERNAL://kafka:29092` for containers, which is what kafka-ui and
  kafka-exporter use.

A Kafka client connects to a bootstrap address, is handed back the advertised
listener, and then connects to *that*. So the host-facing port cannot be
remapped: the container listener, the advertised port and the published port
must all be the same number. Mongo is published 1:1 for a related reason,
below.

## The offset

Every published port derives from one knob, `PORT_OFFSET` in `.env`.
`bin/_common.sh` recomputes the derived block of `.env` on each run, so a bare
`docker compose` sees the same values. Change one number:

```bash
sed -i '' 's/^PORT_OFFSET=.*/PORT_OFFSET=0/' .env
./bin/stack-up.sh
```

`PORT_OFFSET=0` is the handover shape. It is 1000 while an older rig still
owns the standard ports on the machine this was built on. Container-internal
ports never move, so the Grafana datasource (`http://prometheus:9090`) and the
exporter's broker address (`kafka:29092`) are stable across offsets.

A value already in the environment wins over `.env`, the way docker compose
itself treats it, so a one-off override needs no file edit:

```bash
CLOUDSERVER_PORT=9011 ./bin/stack-up.sh
```

`_common.sh` rewrites only the derived port keys and leaves everything else in
`.env` alone, because the process scripts read the same file.

## Scripts

All idempotent, all read `.env`, all print what they do.

- **`bin/stack-up.sh [--krb] [--no-topics]`** Preflight the ports and refuse
  if one is held by something that is not ours, naming the owner. Render the
  CloudServer config, bring up compose, wait for the broker, initiate the
  mongo replica set, create the topics, wait for the observability and
  CloudServer, print the URLs. `--krb` adds the KDC and the SASL_GSSAPI broker
  with its topics and ACLs.
- **`bin/stack-down.sh [--volumes]`** Stop everything including the krb
  profile. Without `--volumes`, topics, consumer offsets, mongo metadata and
  prometheus history survive.
- **`bin/stack-status.sh`** Containers, published ports, topics with partition
  counts and end offsets, consumer groups with total lag, prometheus target
  health per job, provisioned dashboards, CloudServer and its destination
  list, the workgroups document, the krb broker, and the URLs. Read-only.
- **`bin/wait-ready.sh [--timeout S] [--quiet]`** Blocks until the broker
  answers a metadata request, mongo reports PRIMARY, CloudServer answers as
  S3, and grafana answers its health endpoint. For the suite's before-hook:
  quiet on success, names the missing piece on failure, exit 1.
- **`bin/topics-create.sh`** The legacy internal topic (P=4), the failed topic
  (P=1), the delivery topic (P=3) and the customer topics (P=1), then proves
  every partition has a leader three consecutive times.
- **`bin/mongo-init.sh`** Initiate replica set `rs0` with the member host
  pinned to the port mongod actually runs on.
- **`bin/zk-show.sh [znode]`** The workgroups document pretty-printed, the
  generation it declares, the consumer group each workgroup therefore joins,
  the previous generation's groups, the cutover barriers, and the populator's
  log offsets. With an argument, any node.

`bin/_common.sh` is sourced by all of them and needs bash.

### Why topics are created before anything consumes

A consumer that subscribes to a topic which does not exist yet, or whose
partitions have no leader yet, can wedge: it becomes a live group member
holding all its partitions, cycling assign then revoke about once a second,
delivering nothing, while its liveness probe still answers 200 and `/metrics`
shows no delivery counters at all. That fired on 5 of 21 consumer starts during
the migration experiments. `topics-create.sh` runs before any process starts
and confirms leadership three times running.

## Observability

`prometheus.yml` scrapes three jobs:

- **kafka-exporter**, in-network. Consumer-group lag per group, topic and
  partition, and topic offsets.
- **delivery-workers**, on the host at `host.docker.internal:8920-8930` and
  again at `9920-9930`. Those are the same eleven workers under the two
  offsets, since the process scripts derive their probe base as
  `8920 + PORT_OFFSET`. Listing both means one config works at either offset,
  at the cost of eleven targets that always read down.
- **queue-populator**, on the host at 4142, 8910 and 9910, for the same reason.

**The legacy per-destination queue processor exposes nothing on S3C.** The code
has a counter, `s3_notification_queue_processor_events_total{target,eventType}`,
and a probe server, but it only starts when
`extensions.notification.probeServer` is set in the backbeat config, and
neither S3C's rendered config nor the rig's configs set it. During a cutover
the legacy side is therefore observable *only* through consumer-group lag,
which is why the dashboard leads with lag rather than throughput.

### Dashboard

Grafana provisions one datasource and one dashboard, "BNaaS delivery pool", in
folder BNaaS, with anonymous viewer access. Prometheus only, no other data
source. Fifteen panels in two areas.

The pool: delivered per second by destination; drops per second by reason and
by target; delivery delay p50 and p99; consumer-group lag by group and topic;
legacy groups versus the pool group on one axis; records per partition on the
delivery topic; workers up; deliveries in flight; open producers per endpoint.

A **Workgroups** row: delivered per second by workgroup; lag for every group
matching the pool's base group id, which is what makes a generation cutover
visible; drops per second by workgroup; records skipped by reason; cutover
barriers seen; and a table of the generation each worker is running.

A workgroup's real consumer group is `<base>-<workgroupId>-gen<generation>`.
So during a cutover the old generation's group drains to zero while the new
generation's starts from the barrier, and both lines are on the lag panel. Both
matter: stopping the old generation before its lag reaches zero is what strands
records. Lag alone is not enough either, because a wedged consumer holds its
partitions with a lag that simply stops falling; read it next to the delivered
rate.

Metric names and labels, read off the code rather than guessed. Every name
below carries the prefix `s3_notification_delivery_worker_`:

| name, after the prefix | type | labels |
|---|---|---|
| `delivered_total` | counter | `workgroup`, `target` |
| `dropped_total` | counter | `workgroup`, `target`, `reason` |
| `delivery_delay_seconds` | histogram | `workgroup`, `target`, `status` |
| `skipped_total` | counter | `workgroup`, `reason` |
| `barrier_seen_total` | counter | `workgroup`, `match` |
| `lanes` | gauge | none |
| `producers` | gauge | `endpoint` |
| `workgroup_generation` | gauge | `workgroup`, `source` |
| `workgroup_config_changes_total` | counter | `workgroup` |

`reason` on the drop counter is one of `delivery_timeout`, `delivery_error`,
`producer_error`, `unknown_destination` or `parse_error`. On the skip counter
it is `not_in_slice` or `barrier`. The `workgroup` label is absent entirely
when workgroups are off, so a single unlabelled series is the flag-off shape,
not a fault. Histogram buckets stop at 30s, so a hanging destination shows as
p99 pinned to the last bucket and then as a `delivery_timeout` drop.

The metrics route is the arsenal ProbeServer default, `/metrics`, and the port
comes from `deliveryPool.probeServer.port` unless `DELIVERY_POOL_PROBE_PORT`
overrides it, which is how several workers run from one rendered config.

### ZooKeeper

ZooNavigator browses the tree at the URL the scripts print, auto-connecting to
`zookeeper:2181`, so the landing page is already inside it. Two paths matter:

- the workgroups document, `ZK_WORKGROUPS_PATH` in `.env`
- the populator's log offsets, under `ZK_POPULATOR_PATH`

Both are relative to any chroot on the backbeat config's ZooKeeper connection
string, and both must match the config the processes are started with.
`bin/zk-show.sh` prints them for a terminal pane.

## Gotchas

**A probe server on `localhost` cannot be scraped.** The backbeat configs set
`"bindAddress": "localhost"`, which binds 127.0.0.1 and is unreachable from
the prometheus container. Set it to `0.0.0.0` in the `probeServer` block of
whatever config the workers and the populator are started with, or every
host-process scrape fails silently. The compose file pins
`host.docker.internal` to `host-gateway`, so the route is not the problem.

**The replica set advertises one address, and both a host process and a
container have to use it.** The address a mongo client dials after the
handshake is the one in the replica-set config, not the one it was given as a
seed. That address has to be `127.0.0.1:${MONGO_PORT}` for the host-side
populator, and inside a container 127.0.0.1 is the container itself. So
`cloudserver-net` owns the network namespace CloudServer runs in, publishes
the S3 port from there, and runs socat so that `127.0.0.1:${MONGO_PORT}`
inside that namespace really is mongod. Nothing about the mongo service
changes and the host keeps the address it already uses. The alternative,
pointing the replica set at the service name, breaks every host process.

**The `ci-mongodb` image pins its replica-set member to 127.0.0.1:27018.** Its
own `initReplicaSet.js` writes that address, so running the image as-is on a
remapped port would silently point clients at whatever answers on 27018, which
on the build machine is another workstream's database. mongod therefore runs
with an explicit `--port ${MONGO_PORT}` and `bin/mongo-init.sh` initiates the
set with a matching member host. Do not replace that with the image's script.

**CloudServer answers an unsigned `GET /` with 403, and that is healthy.** A
readiness probe built on `curl -f` calls a working server broken. The scripts
assert on the `x-amz-request-id` response header instead, which also proves
CloudServer is what holds the port. There is no healthcheck route on this
build; `/_/healthcheck` returns `InvalidURI`.

**Two images are amd64 only and run under emulation on Apple silicon:**
CloudServer, because no arm64 manifest is published for the 9.3 tags, and
ZooNavigator. Both work; CloudServer takes about 30 seconds longer to start,
which is why its wait is generous.

**Restarting zookeeper under a live broker breaks the broker.** Topic metadata
lives in ZooKeeper here, so its data directory is a named volume, which makes
this likely: ZooKeeper restores the broker's ephemeral `/brokers/ids/1` znode
from its snapshot and the broker exits with
`NodeExistsException ... registerBroker`. The znode disappears when the old
session expires, about a minute. `stack-up.sh` detects exactly this, waits for
`/brokers/ids` to read `[]`, and starts kafka again. To avoid it, restart kafka
alone and never zookeeper underneath it.

```bash
docker compose exec zookeeper zkCli.sh -server localhost:2181 ls /brokers/ids
```

**The zookeeper image whitelists only the `srvr` four-letter word.** `ruok`
answers "not in the whitelist", so a healthcheck built on it never passes.

**The `poc-ft-kafka` image's `start-kafka.sh` understands five `KAFKA_*`
variables:** broker id, zookeeper connect, listeners, advertised listeners and
the offsets topic replication factor. Two named listeners also need
`listener.security.protocol.map` and `inter.broker.listener.name`, and
persisted topic data needs `log.dirs`, so `kafka-entrypoint.sh` appends those
before handing over. Java Properties takes the last occurrence of a key, so
the image's own lines still win for the keys it writes.

**`yarn start` in the CloudServer image runs the file metadata daemon in
parallel,** which this configuration does not use, so the compose command is
`node index.js`. The image's entrypoint also rewrites its own `config.json`
with jq when `ENDPOINT` or `LOG_LEVEL` are set, so neither is set here and the
mounted config is handed over untouched through `S3_CONFIG_FILE`.

**macOS sleep kills the populator's mongo connection.** The populator tails the
mongo oplog; if the Mac sleeps, that connection dies and it does not always
recover, sitting there looking healthy with its ZooKeeper offset frozen.
Disable sleep for a demo, and restart the populator if it has gone quiet after
a lid close.

**zsh does not word-split.** `K="docker exec ..."; $K --list` fails with "no
such file or directory". Spell commands out or use a function. Same for an
endpoint flag in a variable: write `--endpoint-url http://...` in full.

## CloudServer

Published image, run with `S3METADATA=mongodb S3DATA=mem S3VAULT=mem` and
remote management disabled. Its config is rendered by `stack-up.sh` from
`cloudserver/config.json.tmpl`, because the mongo address carries the offset.
Credentials are the image's own `accessKey1` / `verySecretKey1`.

`bucketNotificationDestinations` lists `poc-dest-1`, `poc-dest-2`,
`poc-dest-3` and the Kerberos destinations `krb-dest-a` and `krb-dest-b`.
CloudServer only ever compares the `resource` of a notification ARN against
that list; it never connects to a destination, so the addresses in it are
written as they resolve from inside the container. An ARN naming something
not in the list is rejected with `InvalidArgument`.

If no published tag pulls on some machine, `--profile cloudserver-build` builds
from a clone instead. It is slower, needs `CLOUDSERVER_DIR` pointing at the
clone, and the clone must already have its dependencies installed. The clone
arrives as a named build context, so `cloudserver/Dockerfile` can live here
rather than inside the clone.

## Kerberos profile

```bash
./bin/stack-up.sh --krb
```

An MIT KDC for realm `SCALITY.TEST` and a second Kafka broker with a
`SASL_PLAINTEXT/GSSAPI` listener. Four containers share one network namespace,
held by `krb-net` whose hostname is `localhost`. That is the trick: it keeps
the broker principal `kafka/localhost@SCALITY.TEST` valid for a client that
connects to `localhost`, with no DNS and no hosts-file editing. Published ports
are the only way in from the host.

`stack-up.sh` creates `topic-a` and `topic-b` and the ACLs that make the
identity assertion binary: `User:notifa` may write `topic-a` only,
`User:notifb` may write `topic-b` only, and `allow.everyone.if.no.acl.found` is
false. A destination that loses its principal then lands nothing at all rather
than quietly delivering as somebody else.

The KDC writes four keytabs to `krb/keytabs/`: one for the broker, one per
client principal, and `merged.keytab` holding every client principal. The
merged one plus an empty `DIR:` credential cache is the shape the Node producer
uses, where MIT acquires and refreshes each principal on demand with no
`kinit` and no relogin timer. `stack-up.sh` prints the three environment
variables a host producer needs and renders `krb/krb5.conf.host` with the
published KDC port. Containers use `krb/krb5.conf`, which points at
127.0.0.1:1088, correct inside their shared namespace.

**`ktadd` without `-norandkey` invalidates every keytab written earlier.**
Plain `ktadd` randomises the principal's key and bumps its kvno, so writing one
principal into both a per-principal keytab and the merged keytab leaves only
the last one working, and the client then fails the AS exchange with an AES
decryption error rather than anything that names the real problem.
`kdc-entrypoint.sh` uses `-norandkey`, and drops `-e keysaltlist` because
kadmin refuses the two together; `kdc.conf` restricts `supported_enctypes` to
aes256 anyway.

To rotate the principals, delete `krb/keytabs/*.keytab` and recreate the KDC
container. `KRB_CLIENT_PRINCIPALS` on the `krb-kdc` service changes the client
list, default `notifa notifb`.

## Images

Pinned in `.env`, so a handover can substitute one without touching the compose
file. Pulled: CloudServer, kafka-ui, kafka-exporter, ZooNavigator, socat,
prometheus, grafana, zookeeper, redis. Built from `krb/` when that profile
first comes up: the KDC from Debian plus MIT krb5, and the broker from Alpine
plus Kafka 3.4.0 from `archive.apache.org`, so both need network on a fresh
machine.

`poc-ft-kafka:latest` and `ci-mongodb:latest` are local build artifacts rather
than published images, and a fresh machine needs them built first.

## Files

```
.env                       PORT_OFFSET, image pins, topic and znode names
docker-compose.yaml        the stack, with the krb and cloudserver-build profiles
kafka-entrypoint.sh        two-listener config the base image cannot express
prometheus.yml             kafka-exporter, delivery workers, populator
README.md                  this file
bin/
  _common.sh               ports, compose wrapper, kafka and zk CLIs, waits
  stack-up.sh              one command up, --krb for kerberos
  stack-down.sh            down, --volumes to wipe state
  stack-status.sh          what is running, and where
  wait-ready.sh            blocks until the stack answers, for a before-hook
  topics-create.sh         topics before any consumer, leaders confirmed
  mongo-init.sh            replica set rs0 with a matching member host
  zk-show.sh               workgroups document, generation, populator offsets
cloudserver/
  config.json.tmpl         rendered by stack-up.sh
  Dockerfile               the build-from-a-clone fallback
grafana/
  provisioning/datasources/prometheus.yaml
  provisioning/dashboards/dashboards.yaml
  dashboards/bnaas-delivery-pool.json
krb/
  Dockerfile.kdc kdc.conf kdc-entrypoint.sh
  krb5.conf                        for the containers
  krb5.conf.host.tmpl              rendered for host processes
  server-kerberos.properties.tmpl  ports substituted at start
  broker-entrypoint.sh
  setup-topics-acls.sh
  kafka/Dockerfile
  keytabs/                         written by the KDC, not in git
conf/
  templates/                 the two configs everything is rendered from
  shims/                     the three preload shims, plus a pidfile one
  generated/                 what conf-render.sh writes, not in git
scenarios/run.sh             a thin wrapper round the suite, by act name
results/                     RESULTS.md, the two Kerberos write-ups, and the
                             design critique: what the acts compare against
evidence/<act>/              what each act leaves behind, not in git
HANDOVER.md                  the map, including the recording commands
```

`bin/` also holds the process scripts that drive one process by hand:
`populator.sh`, `worker.sh`, `legacy-processor.sh`, `consumer.sh`,
`driver.sh`, `check.sh`, `offsets.sh`, `bucket.sh`, `conf-render.sh`,
`cloudserver.sh` and `demo-layout.sh`. They read the same `.env` as the
suite, and render their configs through the same renderer, so a config an
act runs with and a config you start a process with cannot drift.

The demo itself is not here: it is the mocha suite at
`tests/functional/demo/`, run with `yarn ft_test:demo`.

## Validated

At `PORT_OFFSET=1000`, on Docker Desktop, arm64.

- Topics created before any consumer, every partition leader-confirmed. Topics,
  records and committed offsets survived a container restart on the volumes.
- Mongo `rs0` reached PRIMARY with the member host on the mongod port.
- The containerized CloudServer logged `connected to mongodb` through the socat
  alias, then a create-bucket, a real object PUT and a list-objects over the
  aws cli all succeeded, and the bucket appeared in the compose mongo's
  `metadata` database. A notification configuration naming `poc-dest-1` and one
  naming `krb-dest-a` were both accepted; one naming an unknown resource was
  rejected with `InvalidArgument`.
- kafka-ui reported the cluster online with the right partition counts.
  kafka-exporter exposed lag per group, topic and partition.
- All 22 queries across the 15 panels returned data, the workgroup panels
  included, with three generation-shaped consumer groups showing the old
  generation drained to zero and two new ones carrying lag. Grafana served the
  dashboard to an anonymous reader and its datasource proxy answered the same
  PromQL a panel issues.
- `zk-show.sh` printed a three-workgroup document with its generation, the
  derived group id per workgroup, the previous generation's groups, the
  barriers, and the populator's live log offset.
- `wait-ready.sh` passed all four checks and, pointed at a port with nothing on
  it, failed in six seconds naming that check.
- The port preflight refused to start at offset 0 while another project held
  those ports, naming each owner, and created nothing.
- Kerberos: the broker logged
  `Successfully authenticated client: authenticationID=notifa@SCALITY.TEST`
  over SASL_GSSAPI on the offset port, the messages read back over the
  plaintext listener, and the negative control was denied with
  `TOPIC_AUTHORIZATION_FAILED`, leaving that topic at offset 0.

Not validated: `PORT_OFFSET=0`, because an older rig holds those ports on the
build machine; the derivation was exercised by switching `.env` between 0 and
1000 and checking every value. The `cloudserver-build` profile parses but was
not built, since the published image pulls.
