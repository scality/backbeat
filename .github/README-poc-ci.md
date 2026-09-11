<!-- markdownlint-disable MD013 -->

# Delivery pool suites in CI

What the bucket-notification delivery pool work runs in GitHub Actions, on
top of the jobs every branch already has (lint, unit, the functional matrix,
ballooning, queue populator).

| job | suite | what it proves | needs | status |
|---|---|---|---|---|
| `notification:deliverypool tests` | `yarn ft_test:notification:deliverypool` | one worker delivering to many destinations, drops counted by reason, the drainer | kafka, zookeeper, mongo (job services) | blocking |
| `notification:workgroups tests` | `yarn ft_test:notification:workgroups` | workgroups as consumer groups with a slice filter, pins, generation changes, seeded offsets | same | blocking |
| `notification:internal tests` | `yarn ft_test:notification:internal` | the worker on today's internal topic: per-destination matching, seeding from processor offsets, per-destination watermarks, a bucket configured after start | same | blocking |
| `notification:kerberos tests` | `yarn ft_test:notification:kerberos` | one worker process authenticating as several Kerberos principals, ACL-checked | a KDC and a kerberised broker, built and started by the job | blocking |

The unit specs for the same code run in the `unit` job with everything else.
The demo suite under `tests/functional/demo` is not in CI on purpose: it
drives a full stack for about an hour and exists to be watched.

## Blocking

All four block the workflow. They were introduced carrying
`continue-on-error`, so that a red one was visible on the run page without
failing the run while the last timing and Kerberos races were taken out of
them, and the flags came off once each had five consecutive green runs on
this branch. Nothing in the delivery pool work is experimental any more: a
red POC job now fails the workflow like `replication` or `lifecycle` does.

Two reds seen on this branch come from outside these suites and from outside
the delivery pool code: `lib tests` failed the BackbeatConsumer shutdown spec
once and passed on the runs either side of it, and `queue-populator` failed
once in `yarn install`, building `diskusage` against corrupted node-gyp
headers on the runner.

## The first three

They are ordinary entries of the `functional-tests` matrix and run through
`.github/scripts/run_ft_tests.bash` like `replication` or `lifecycle`. The
suites read `KAFKA_HOSTS` and `ZOOKEEPER_HOSTS` and default to
`localhost:9092` and `localhost:2181`, which are the job's service ports, so
no configuration is passed.

Locally, against any broker and ZooKeeper (the demo stack publishes them at
`localhost:10092` and `localhost:3181` with the default `PORT_OFFSET=1000`):

```bash
KAFKA_HOSTS=localhost:10092 ZOOKEEPER_HOSTS=localhost:3181 yarn ft_test:notification:deliverypool
KAFKA_HOSTS=localhost:10092 ZOOKEEPER_HOSTS=localhost:3181 yarn ft_test:notification:workgroups
KAFKA_HOSTS=localhost:10092 ZOOKEEPER_HOSTS=localhost:3181 yarn ft_test:notification:internal
```

Do not run them while the demo suite holds the same broker: both create and
delete consumer groups and topics.

## The Kerberos job

`kerberos-tests` is its own job because the suite needs the docker socket:
it reads the broker's own `authenticationID=` log lines as the proof of
identity, restarts the broker mid-run (arm E) and shortens ticket lifetimes
through `kadmin.local` in the KDC (arm F). Job services cannot be restarted
or exec'd into, so `.github/scripts/run_krb_rig.bash up` builds two images
from `poc-demo/krb` (a MIT KDC on Debian, Kafka 3.4 on Alpine) and starts
three plain containers on the runner's host network: ZooKeeper on 2181, the
KDC on 1088, the broker with a SASL_PLAINTEXT/GSSAPI listener on 19095 and a
plaintext listener on 19096, topics `topic-a` and `topic-b`, and ACLs binding
`notifa` and `notifb` to their topic. Host networking is what makes
`kafka/localhost` and `localhost:19095` true for the containers and the test
process at the same time.

The runner gets `krb5-user`, `libkrb5-dev`, `libsasl2-dev` and
`libsasl2-modules-gssapi-mit` before `yarn install`, so the `kerberos`
binding builds and librdkafka is compiled with SASL GSSAPI (the suite's
collision-control arm uses the shipping node-rdkafka producer). The suite
runs with:

```bash
CONF_DIR=$RUNNER_TEMP/krb            # its ssl/ holds notifa.keytab and notifb.keytab
KRB_BROKERS=localhost:19095
KRB_VERIFY_BROKERS=localhost:19096
KRB_KAFKA_CONTAINER=krb-kafka
KRB_KDC_CONTAINER=krb-kdc
KRB5_CONFIG=$RUNNER_TEMP/krb/krb5.conf   # poc-demo/krb/krb5.conf plus an empty qualify_shortname
```

Locally on a Linux box with docker, the rig script does all of that:

```bash
.github/scripts/run_krb_rig.bash up
.github/scripts/run_krb_rig.bash probe
CONF_DIR=/tmp/krb KRB5_CONFIG=/tmp/krb/krb5.conf KRB_BROKERS=localhost:19095 \
  KRB_VERIFY_BROKERS=localhost:19096 KRB_KAFKA_CONTAINER=krb-kafka \
  KRB_KDC_CONTAINER=krb-kdc yarn ft_test:notification:kerberos
.github/scripts/run_krb_rig.bash down
```

`KRB_DIR` defaults to `$RUNNER_TEMP/krb` and falls back to `/tmp/krb` off a
runner. On a Mac there is no host networking, so run the suite in a container
as `tests/functional/deliverypool/README-kerberos.md` describes.

One line differs from the rig's own `krb5.conf`: `qualify_shortname = ""`.
MIT krb5 1.18 and later append the machine's DNS domain to a single-label
hostname before mapping it to a realm, so on a cloud runner `localhost`
became `localhost.<runner domain>` and every client asked for a cross-realm
ticket the KDC does not have. Containers have no search domain and never see
this. The `probe` step (`kinit` from the keytab, then `kvno -S kafka
localhost`, both with `KRB5_TRACE`) fails fast if the runner's Kerberos
resolution is wrong, before the 20-minute suite starts.

### Arm C, the collision control

Arm C is the control that says what the shipping node-rdkafka producer does
with two principals in one process: librdkafka passes `GSS_C_NO_CREDENTIAL`,
so every GSSAPI handshake authenticates as whatever principal the process
default credential cache holds, and `sasl.kerberos.principal` only renders
the `kinit` command librdkafka runs per client through `system()`. Two
clients created back to back therefore race to write that one cache, so the
arm populates it once and puts a `kinit` that does nothing ahead of the real
one on `PATH`: both producers then authenticate as `notifa`, and the
destination that needs `notifb` is denied every record with a topic
authorization failure.

The rig's container logs are uploaded as the `kerberos-rig-logs` artifact on
every run. `tests/functional/deliverypool/README-kerberos.md` describes the
arms and the environment variables in full, and how to run the suite in a
container on a Mac, where host networking is not available.
