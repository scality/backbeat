/*
 * Demo-only preload shim, loaded with `node --require`. It patches a module
 * in memory for the life of one process, so nothing on disk, in the
 * repository or in node_modules, is modified.
 *
 * WHY
 * ---
 * lib/BackbeatConsumer.js sets, for every consumer:
 *     'metadata.max.age.ms': 5000
 * but leaves librdkafka's `topic.metadata.refresh.interval.ms` at its default
 * 300000. The cache entry for a subscribed topic therefore expires 5s after it
 * was fetched, while the periodic refresh that would renew it is 5 minutes
 * away. librdkafka then flips the topic:
 *     Topic backbeat-bucket-notification metadata information timed out (5009ms old)
 *     Topic backbeat-bucket-notification changed state exists -> unknown
 * and reports to the consumer group:
 *     (re)joining ... "Metadata for subscribed topic(s) has changed"
 *
 * The group rebalances every ~1-2s forever. Each ASSIGN is revoked about a
 * second later, the processing queue is idle so BackbeatConsumer un-assigns,
 * librdkafka rejoins, and the consumer never stays assigned long enough to
 * fetch. Observed on the rig as 50+ assign/revoke cycles per minute with
 * CURRENT-OFFSET '-' on all four partitions while 20 messages sat in the topic.
 *
 * WHAT THIS DOES
 * --------------
 * Injects `topic.metadata.refresh.interval.ms` = 2000 (below the 5000ms max
 * age) into every KafkaConsumer's global config, so the cache is renewed before
 * it expires and subscribed topics stop flapping. Nothing else is changed: no
 * offset policy, no group id, no delivery behaviour.
 *
 * A Proxy is used rather than a wrapper function so prototypes and instanceof
 * keep working.
 */
'use strict';

const path = require('path');

/**
 * Resolve a module the way the process this shim is preloaded into resolves
 * it, so the copy that gets patched is the copy that gets loaded.
 *
 * The suite spawns every backbeat process with the repository root as its
 * working directory, so that is the first place to look. BACKBEAT_DIR is the
 * explicit override, and this file's own repository is the fallback for a
 * process started from somewhere else. There is no hardcoded path: a shim
 * that patches another checkout's node_modules reports success and changes
 * nothing.
 *
 * @param {String} spec - a module path under node_modules
 * @return {String} an absolute, resolvable module path
 */
function resolveInRepo(spec) {
    const bases = [
        process.cwd(),
        process.env.BACKBEAT_DIR,
        // poc-demo/conf/shims -> the repository root
        path.resolve(__dirname, '..', '..', '..'),
    ].filter(Boolean);
    for (const base of bases) {
        try {
            return require.resolve(path.join(base, 'node_modules', spec));
        } catch {
            // try the next base
        }
    }
    return require.resolve(spec);
}

const RDKAFKA = resolveInRepo('node-rdkafka');
const REFRESH_MS = 2000;

const kafka = require(RDKAFKA);
const OrigConsumer = kafka.KafkaConsumer;

kafka.KafkaConsumer = new Proxy(OrigConsumer, {
    construct(target, args) {
        const [globalConf, topicConf, ...rest] = args;
        const patched = Object.assign({}, globalConf, {
            'topic.metadata.refresh.interval.ms': REFRESH_MS,
        });
        return new target(patched, topicConf, ...rest);
    },
});

// eslint-disable-next-line no-console
console.error(
    `[rig] kafka shim installed: topic.metadata.refresh.interval.ms=${REFRESH_MS}`
);
