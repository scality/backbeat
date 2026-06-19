<!-- markdownlint-disable MD013 -->

# Code review instructions

Repo context lives in [CLAUDE.md](../CLAUDE.md) — read it first.

When reviewing a PR, analyze the changes against these criteria:

| Area | What to check |
|------|---------------|
| Async error handling | Uncaught promise rejections, missing error callbacks, swallowed errors in streams. Double callbacks in try/catch blocks (callback called in try then again in catch) |
| Async/await usage | New or modified code should use async/await instead of callbacks when possible. When code is migrated from callbacks to async/await, verify: no leftover callback or next params, no mixed callback + promise patterns, proper try/catch around awaited calls, errors are re-thrown or handled (not silently swallowed). Watch for the anti-pattern: `try { cb(); } catch(err) { cb(err); }` where an exception after the first `cb()` triggers a second call |
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
