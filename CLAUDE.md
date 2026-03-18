<!-- markdownlint-disable MD013 -->

# Backbeat

This is a **Node.js asynchronous queue and job manager** for Scality's S3C and Artesca products. It processes metadata updates and dispatches background tasks via Kafka. It contains:

- Kafka consumers/producers (`lib/BackbeatConsumer.js`, `lib/BackbeatProducer.js`)
- Pluggable extensions for replication, lifecycle, notifications, ingestion, GC (`extensions/`)
- Queue population from MongoDB oplog and Metadata (raft) oplog (`lib/queuePopulator/`)
- Management API and routes (`lib/api/`)
- Configuration management with Joi validation (`lib/Config.js`)
- Git-based internal deps: arsenal, vaultclient, bucketclient, werelogs, breakbeat, httpagent
- CommonJS modules with callback-based async patterns
- Mocha + Sinon test suites (`tests/unit/`, `tests/functional/`, `tests/behavior/`)
