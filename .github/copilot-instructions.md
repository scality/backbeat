<!-- markdownlint-disable MD013 -->

# Code review instructions

Repo context lives in [CLAUDE.md](../CLAUDE.md) — read it first.

When reviewing a PR, analyze the changes against these criteria:

| Area | What to check |
|------|---------------|
| Async error handling | Uncaught promise rejections, missing error callbacks, swallowed errors in streams. Double callbacks in try/catch blocks (callback called in try then again in catch) |
| Async/await usage | New or modified code should use async/await instead of callbacks (see [Async/await migration suggestions](#asyncawait-migration-suggestions) below for when to suggest migrating). When code is migrated from callbacks to async/await, verify: no leftover callback or next params, no mixed callback + promise patterns, proper try/catch around awaited calls, errors are re-thrown or handled (not silently swallowed), `return await` rather than returning a bare promise, no `forEach` with async callbacks (use `for...of` or `Promise.all`), callers updated or backward compatibility kept via `util.callbackify`. Watch for the anti-pattern: `try { cb(); } catch(err) { cb(err); }` where an exception after the first `cb()` triggers a second call |
| Kafka consumer/producer | Correct topic configuration, proper offset commits, consumer group handling, message serialization. Verify `onEntryCommittable` is always reachable. Check circuit breaker thresholds when adding new downstream topics |
| Stream handling | Backpressure, proper cleanup on error, no leaked file descriptors, correct pipe chains |
| Dependency pinning | Git-based deps (arsenal, vaultclient, bucketclient, werelogs, breakbeat, httpagent) must pin to a tag, not a branch |
| Logging | Proper use of werelogs, no `console.log` in production code, log levels match severity. Include enough context (bucket, object key, version, offset) for production troubleshooting |
| Prometheus metrics | New metrics follow existing naming conventions (`s3_backbeat_*`), correct metric types (counter vs gauge vs histogram), bounded label cardinality — avoid per-connector or per-bucket labels that explode with scale |
| Config changes | Backward compatibility, Joi schema updates match new fields, environment variable naming, default values. Env var overrides in `lib/Config.js` must stay consistent with the config file schema |
| MongoDB / Redis resilience | Reconnection handling, proper timeouts on external calls, no indefinite waits. Network errors to MongoDB must not cause stuck tasks or silent data loss |
| Extension architecture | Changes respect the pluggable extension pattern, no cross-extension coupling |
| Security | Command injection, prototype pollution, unsafe deserialization, credential exposure in config/env vars, OWASP-relevant issues for Node.js |
| Breaking changes | Anything that changes public APIs, Kafka message formats, inter-service contracts, or oplog/change stream projections |

## Async/await migration suggestions

Backbeat is migrating from callbacks and the `async` library to async/await, per the [Scality migration guide](https://scality.atlassian.net/wiki/spaces/OS/pages/3523346468/2025-10-30+-+Async+Await+migration) and its **migrate when you touch** rule: functions a PR significantly changes get migrated in that PR; untouched or lightly-touched code is left alone. Apply judgement:

- If the PR significantly reworks a function (signature, rewritten logic, most of its body) and it remains callback-based, suggest migrating it to async/await — in a dedicated commit if large.
- For class methods, consider the whole hierarchy (e.g. `MultipleBackendTask extends ReplicateObject extends BackbeatTask`): making a method async changes its contract for every subclass, so a suggestion must cover overrides and `super` calls together — and flag partial migrations where a base method became async but overrides or `super` callers still use callbacks.
- Flag new callback-style functions (last parameter named `cb`, `callback`, `done`, `next`) and new uses of the `async` library: new code must use async/await. Exception: `async` utilities with concurrency limits (`async.eachLimit`, `async.queue`) that raw Promises cannot easily replicate.
- Do **not** suggest migration for minor edits inside a large callback-based function, callback shapes imposed by external APIs (node-rdkafka event handlers, stream/event-emitter callbacks, Mocha hooks), or test-only changes.
- When new code consumes a callback-only dependency, suggest `util.promisify` (bind object methods) over hand-rolled `new Promise`.
- At most one migration suggestion per function, phrased as a suggestion, not a blocker.
