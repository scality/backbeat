---
name: bnaas-demo-stack
description: Bring the BNaaS (bucket notifications as a service) demo infrastructure up or down, check its status, and get the Grafana, Kafka UI, Prometheus and ZooKeeper URLs. Use before running the demo test suite, when the stack looks wrong, or when a port is already held by another rig.
---
<!-- markdownlint-disable MD013 -->

# The BNaaS demo stack

Two commands make the demo: `docker compose up` for the infrastructure, then
`yarn ft_test:demo` for the demo itself, which is a mocha functional suite.
This skill is the first half.

The material lives in `poc-demo/` inside the backbeat repository, on the
branch `poc/S3C-11127-demo`. The suite is `tests/functional/demo/`, the
measurements the acts compare against are `poc-demo/results/`, and
`poc-demo/HANDOVER.md` is the map.

Infrastructure in compose: kafka, zookeeper, redis, a mongo replica set,
CloudServer, prometheus, grafana, kafka-ui, kafka-exporter, ZooNavigator for
the ZooKeeper browser, and with `--krb` a Kerberos KDC plus a SASL_GSSAPI
broker. Only the backbeat processes are host processes, from the worktree,
and the suite or the scripts in `bin/` start those.

## Ports, and the one path knob

`.env` is the single source of truth. `PORT_OFFSET` is **1000** while the
older rig (compose project `ft`, tmux `bnaas-rig`) still owns the standard
ports on this machine, which puts kafka on 10092, zookeeper 3181, redis 7379,
mongo 28117, prometheus 10090, grafana 4000, kafka-ui 9085, CloudServer 9010
and the worker probes on 9921 and up. Set it to 0 once that rig is retired.

One knob cannot be derived: `NODE_BIN`, the node 22 bin directory, because
the native modules break on 24. The repository is resolved from the scripts'
own location, so no path in `.env` needs editing on a new machine.
`CLOUDSERVER_DIR` is only read by the `cloudserver-build` fallback profile
and by `bin/cloudserver.sh`, and it defaults to a checkout beside this one.

Never touch the other rigs on this machine: compose project `ft`, containers
`bnaas-mongo`, `f9-mongo` and the `bnaaskrb-*` set. Every script here excludes
them by name, and identifies its own processes by the shim path on their
command line.

## Up

From the repository root:

```bash
yarn demo:up                    # containers, mongo, topics, CloudServer
yarn demo:up:krb                # also the Kerberos KDC and broker, for act 07
yarn demo:wait                  # broker, mongo PRIMARY, S3, grafana
yarn demo:status                # what is up, and the URLs
```

or the same scripts directly, from `poc-demo/`:

```bash
bin/stack-up.sh [--krb]
bin/conf-render.sh              # conf/generated/*.json from .env and the templates
bin/wait-ready.sh
```

`wait-ready.sh` is what the suite's before-hook runs too. It asserts on the
S3 response header rather than on a 200, because an unsigned GET / answers
403 and that is healthy.

`stack-up.sh` refuses to start if a port it wants is held by a container that
is not the demo's, and names the owner. `conf-render.sh` is a thin wrapper
around the suite's own renderer, so a config an operator starts a process with
and a config an act starts one with cannot drift. Every probe server it
renders binds 0.0.0.0, not localhost, or prometheus cannot scrape from its
container.

Then run the demo:

```bash
yarn ft_test:demo                                    # every act, in order
DEMO_ACTS=02,04,06 DEMO_PACE=slow yarn ft_test:demo  # a few, at recording pace
poc-demo/scenarios/run.sh 04 06                      # the same, by act name
```

The suite starts and stops CloudServer, the populator, the processors and the
workers itself, and it recreates every topic and deletes every consumer group
it uses before the first act, so it never inherits somebody's leftovers.

## By hand

The scripts in `bin/` drive the same processes for ad-hoc work:

```bash
bin/cloudserver.sh start|stop|status   # only the source fallback
bin/populator.sh start legacy|pool | stop | status
bin/legacy-processor.sh start <dest> | stop <dest> | status
bin/worker.sh start <n> [--workgroup <id>] | stop <n> | status | signal <n> KILL
bin/consumer.sh start <customer-topic>      # a console consumer, own group
bin/driver.sh --bucket demo-bucket --rate 2 --count 20
bin/check.sh <label> --events <dump> --driver <log>
bin/offsets.sh | bin/offsets.sh --watch
bin/bucket.sh create <bucket> <dest>[:<prefix>] ...
bin/zk-show.sh                              # the workgroups document
bin/demo-layout.sh                           # the four-pane recording layout
```

## Status

```bash
bin/stack-status.sh    # containers, ports, topics, groups, scrape targets, URLs
bin/offsets.sh         # every offset: topic ends, group lag, zookeeper offsets
bin/worker.sh status   # delivered counters, assign/revoke, liveness
```

The check that matters after starting any consumer is whether it is
**delivering**, not whether it is assigned. A wedged consumer is a live group
member holding all its partitions with a lag that stops falling, no delivery
counters on `/metrics`, and `/_/live` still answering 200. It fired on 5 of 21
consumer starts during the migration round. The cure is a restart of that one
consumer, and up to 45 seconds of it is the wedged member's group session
expiring.

## URLs

| what | url at offset 1000 |
|---|---|
| Grafana | http://localhost:4000, dashboard "BNaaS delivery pool" |
| Prometheus | http://localhost:10090 |
| Kafka UI | http://localhost:9085 |
| ZooNavigator | http://localhost:10000, auto-connects to zookeeper:2181; the workgroups document is at /bnaas-demo/delivery-workgroups |
| kafka-exporter | http://localhost:10308/metrics |
| worker probe n | http://localhost:992n/metrics, plus /_/live and /_/ready |
| S3 | http://localhost:9010, keys accessKey1 / verySecretKey1 |

Subtract 1000 at offset 0, where S3 is 8010 and the probes are 8921 and up.

## Down

```bash
yarn demo:down                  # volumes survive: topics, offsets, mongo, history
poc-demo/bin/stack-down.sh --volumes     # wipe everything
```

The suite stops what it started. Anything you started by hand, stop with the
same script that started it.

## When something is wrong

- **`no demo kafka container is running`**: `bin/stack-up.sh`, or set
  `KAFKA_CONTAINER` in `.env`.
- **CloudServer dies with `EADDRINUSE`**: another CloudServer holds one of its
  ports. The renderer moves them all by the offset, so re-render rather than
  editing a config by hand.
- **A Grafana panel is empty**: check the probe bind address is 0.0.0.0 and
  that prometheus lists the target as up. The two dashboard variables default
  to the canonical topic and group names, which is what the suite uses.
- **The out-of-CI functional suites fail to find a broker**: the pool,
  workgroups and kerberos suites hardcode localhost:9092, so run them with
  the stack at `PORT_OFFSET=0`. The unit suite is fine at any offset.
- **macOS sleep** ruins a long act: the containers survive but the consumer
  groups rebalance. Disable sleep before a take.

## Related

- Run the demo and read its numbers: `bnaas-run-scenario`
- The recording storyline, act by act: `bnaas-demo-walkthrough`
- Branches, evidence, open decisions: `bnaas-poc-state`
