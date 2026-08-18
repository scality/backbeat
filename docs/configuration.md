# Configuration

Backbeat reads its configuration from `conf/config.json`, or from the file named
by the `BACKBEAT_CONFIG_FILE` environment variable.

Any configuration field can be overridden by an environment variable. Overrides
are applied in memory when the configuration is loaded: the file is never
modified, and can be mounted read-only.

## Variable names

The name of a variable is its configuration path, with each segment converted
from camelCase to SNAKE_CASE and joined with `_`:

- `kafka.hosts` -> `KAFKA_HOSTS`
- `queuePopulator.batchMaxRead` -> `QUEUE_POPULATOR_BATCH_MAX_READ`
- `extensions.gc.topic` -> `EXTENSIONS_GC_TOPIC`
- `extensions.lifecycle.conductor.concurrency` ->
  `EXTENSIONS_LIFECYCLE_CONDUCTOR_CONCURRENCY`

Two annotations, set next to the field they name, tune these names. Given the
schema:

```js
joi.object({
    foo: joi.object({
        bar: joi.string(),
        baz: joi.object({ qux: joi.number() }),
    }),
})
```

`foo.bar` is set by `FOO_BAR`, and `foo.baz.qux` by `FOO_BAZ_QUX`. Annotating
`baz` changes the name of the fields it holds:

- `.meta({ env: 'ZAB' })` renames the segment `baz` contributes, for itself and
  its children: `foo.baz.qux` is set by `FOO_ZAB_QUX`, and `FOO_BAZ_QUX` is no
  longer a name.
- `.meta({ envVarAlias: 'ZAB' })` adds an extra name to reach the field within
  its schema: `foo.baz.qux` is set by `ZAB_QUX`, as well as by `FOO_BAZ_QUX`.

This is how the historic names keep working, without a hand written mapping:
`s3` carries `env: 'CLOUDSERVER'`, so `s3.host` is set by
`CLOUDSERVER_HOST` (instead of `S3_HOST`); `queuePopulator.mongo` carries
`envVarAlias: 'MONGODB'`, so `queuePopulator.mongo.database` answers to
`MONGODB_DATABASE` as well as to `QUEUE_POPULATOR_MONGO_DATABASE`. In an
extension's schema, an alias keeps the `EXTENSIONS_<name>` prefix, e.g.
`EXTENSIONS_REPLICATION_QUEUE_PROCESSOR_AWS_S3_RETRY_TIMEOUT_S`.

The fields of an object with unconstrained keys, such as `kafka.producerParams`,
have no derived name, and neither have the fields the schema forbids.

## Values conversion

A variable holds a string, which gets converted to the type of the field before
the configuration is validated. The schema has the last word: an invalid value
eventually fails the startup, when joi validates the schema, instead of being
silently ignored.

- numbers and strings are passed through, and coerced by the schema
- booleans accept `true`/`false`, `1`/`0`, `yes`/`no`, `on`/`off`
- lists are comma separated, e.g. `HEALTHCHECKS_ALLOWFROM=10.0.0.0/8,::1`
- structured values are JSON documents, e.g.
  `EXTENSIONS_NOTIFICATION_DESTINATIONS='[{ "resource": "d1", ... }]'`
- an empty value is ignored, and leaves the field as configured: a variable
  cannot clear a field

## Variables setting several fields

- `LIVENESS_PROBE_PORT`: the port of every probe server configured, bound to
  `0.0.0.0`. Per site probe servers are left alone.
- `MONGODB_HOSTS`: `queuePopulator.mongo.replicaSetHosts`.
- `REDIS_SENTINELS`, `REDIS_HA_NAME`: `redis.sentinels` and `redis.name`
  (`mymaster` by default). They replace the standalone host and port. The
  sentinels are a comma separated list of `host:port`, e.g.
  `REDIS_SENTINELS=sentinel1:26379,sentinel2:26379`.
- `REDIS_HOST`, `REDIS_PORT`: standalone `redis.host` and `redis.port` (6379 by
  default). Both are ignored when sentinels are configured.
- `EXTENSIONS_REPLICATION_DEST_BOOTSTRAPLIST`: the servers of the `zenko` site
  of the replication bootstrap list, comma separated.
  `EXTENSIONS_REPLICATION_DEST_BOOTSTRAPLIST_MORE` holds the additional sites,
  as raw JSON objects, e.g. `{ "site": "aws", "type": "aws_s3" }`.

## Other variables

These are read by the code directly, wherever they are needed, rather than
setting a field of the configuration file. No schema declares them, so no name
is derived for them, and their value is not validated.

- `BACKBEAT_CONFIG_FILE`: path of the configuration file.
- `BACKBEAT_QUEUEPOPULATOR_EXTENSIONS`: extensions run by this queue populator,
  comma separated.
- `BOOTSTRAP_SITE_NAME`: restricts the replication bootstrap list to one site.
- `KAFKA_TOPIC_PREFIX`: prepended to every topic name, to share a cluster.
- `CONF_DIR`: directory holding the notification destination credential files.
- `TYPE`, `SSL`, `PROTOCOL`, `CA`, `CLIENT`, `KEY`, `KEY_PASSWORD`, `KEYTAB`,
  `PRINCIPAL`, `SERVICE_NAME`, `BASIC_USERNAME`, `BASIC_PASSWORD`,
  `SCRAM_MECHANISM`: auth of the destination a notification processor serves,
  passed by the deployment rather than configured in the file.
- `S3AUTH_CONFIG`: path of the account credentials file, for the `account` auth
  type.
- `MANAGEMENT_BACKEND`, `REMOTE_MANAGEMENT_DISABLE`: management backend of the
  Zenko deployment, and whether to run it.
- `LIFECYCLE_OBJECT_PROCESSOR_TYPE`: the lifecycle object tasks this processor
  consumes, `expiration` (the default) or `transition`.
- `LIFECYCLE_MAX_AUTO_INDEX_DOC_COUNT`,
  `LIFECYCLE_MAX_AUTO_INDEX_STORAGE_BYTES`: limits above which lifecycle
  indexes are not built automatically.
- `BATCH_TIMEOUT_SECONDS`: how long a queue populator batch may run before it
  is reported as stuck, 300 by default.
- `CRASH_ON_BATCH_TIMEOUT`, `CRASH_ON_REBALANCE_TIMEOUT`: exit when a batch or a
  consumer rebalance times out, rather than waiting for the liveness probe to
  report it. Set on S3C, where supervisord only restarts a program on exit.
- `RDKAFKA_DEBUG_LOGS`: librdkafka debug contexts to enable, comma separated.

The following are meant for testing only:

- `TIME_PROGRESSION_FACTOR`: decreases the weight of a day, to expedite the
  lifecycle of objects.
- `EXPIRE_ONE_DAY_EARLIER`, `TRANSITION_ONE_DAY_EARLIER`: deprecated in favor of
  `TIME_PROGRESSION_FACTOR`.
- `BACKBEAT_ECHO_TEST_MODE`, `BACKBEAT_INJECT_REPLICATION_ERROR_RATE`,
  `BACKBEAT_INJECT_REPLICATION_ERRORS`, `CI`: fault injection and test
  fixtures.
