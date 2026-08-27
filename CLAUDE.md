<!-- markdownlint-disable MD013 -->

# Backbeat

This is a **Node.js asynchronous queue and job manager** for Scality's S3C and Artesca products. It processes metadata updates and dispatches background tasks via Kafka. It contains:

- Kafka consumers/producers (`lib/BackbeatConsumer.js`, `lib/BackbeatProducer.js`)
- Pluggable extensions for replication, lifecycle, notifications, ingestion, GC (`extensions/`)
- Queue population from MongoDB oplog and Metadata (raft) oplog (`lib/queuePopulator/`)
- Management API and routes (`lib/api/`)
- Configuration management with Joi validation (`lib/Config.js`)
- Git-based internal deps: arsenal, vaultclient, bucketclient, werelogs, breakbeat, httpagent
- CommonJS modules; legacy code is callback-based, migrating to async/await (see below)
- Mocha + Sinon test suites (`tests/unit/`, `tests/functional/`, `tests/behavior/`)

## Configuration

Every configuration field is settable from the environment, with the variable name derived from the joi schema — see [docs/configuration.md](docs/configuration.md):

- Add a setting by declaring it in the joi schema (`lib/config.joi.js`, `lib/config/configItems.joi.js`, or the extension's own validator). The env var follows from the config path, so nothing else is needed, and the value is validated.
- Do not read `process.env` directly for something the configuration could hold, and do not
  hand-roll parsing: prefer a schema field, an `env`/`envVarAlias` annotation to adjust variable
  name, or an `envDecodeHook` for custom decoding of the value.
- Anything that escapes that path — a variable read straight from `process.env`, one setting several fields, or a decode hook — is invisible to the schema, so document it in [docs/configuration.md](docs/configuration.md) in the same change, and cover it with a test.

## Async code style

The codebase is migrating from callbacks and the `async` library to async/await, per the [Scality migration guide](https://scality.atlassian.net/wiki/spaces/OS/pages/3523346468/2025-10-30+-+Async+Await+migration):

- New functions use async/await — no callback parameters, no new uses of the `async` library (except utilities with concurrency limits, e.g. `async.eachLimit`).
- **Migrate when you touch**: a function you significantly change (signature, rewritten logic, substantial edits) gets migrated as part of the change; minor edits do not require migration. Keep large migrations in a dedicated commit.
- Class methods migrate hierarchy-wide: making a method async changes its contract for every subclass (e.g. `MultipleBackendTask extends ReplicateObject extends BackbeatTask`) — migrate overrides and `super` calls together with the base method.
- Wrap callback-based dependencies (node-rdkafka, arsenal, bucketclient, ...) with `util.promisify` (bind object methods) rather than hand-rolled `new Promise`; use native Promise APIs where available.
- If out-of-scope callers still pass callbacks, keep backward compatibility with `util.callbackify` or a continuation callback; never invoke the callback inside a `try/catch` — exceptions it throws would be swallowed.
- `return await` rather than returning a bare promise; a function with nothing to await should not be `async`; never `forEach` with async callbacks — use `for...of` or `Promise.all(array.map(...))`.
