'use strict';

/**
 * Generated configuration. One renderer for everything: the demo test suite
 * and the operator scripts both come through here, so a config an act runs
 * with and a config an operator starts a process with cannot drift.
 *
 * The path a backbeat process takes is decided by ONE thing: whether
 * extensions.notification.deliveryPool is present. Same code, same tree.
 *
 * Every probe server binds 0.0.0.0, not localhost, or prometheus in its
 * container cannot scrape the workers and the populator.
 */

const fs = require('fs');
const path = require('path');
const env = require('./env');

const TEMPLATES = path.join(env.CONF, 'templates');
const SHIM_DIR = path.join(env.CONF, 'shims');
const SHIMS = ['oplog-h-shim.js', 'oplog-v2diff-shim.js',
    'kafka-metadata-refresh-shim.js', 'pidfile-shim.js'];

function template(name) {
    const file = path.join(TEMPLATES, name);
    if (!fs.existsSync(file)) {
        throw new Error(`missing config template ${file}`);
    }
    return JSON.parse(fs.readFileSync(file, 'utf8'));
}

/**
 * The three preload shims, plus the pidfile one, as node --require
 * arguments. They are not optional on a mac: without them the mongo log
 * source cannot be decoded on MongoDB 5, and no notification consumer stays
 * assigned long enough to fetch.
 *
 * @return {Array} node arguments
 */
function shimArgs() {
    const out = [];
    SHIMS.forEach(s => {
        const p = path.join(SHIM_DIR, s);
        if (fs.existsSync(p)) {
            out.push('--require', p);
        }
    });
    return out;
}

// The destination names the demo can use are the ones CloudServer validates
// a bucket notification configuration against, and its list is read once at
// startup. The containerised CloudServer of the demo stack knows
// poc-dest-1, poc-dest-2, poc-dest-3, krb-dest-a and krb-dest-b, so those
// are the five the acts use. Backbeat's own list, generated here, is what
// decides where a record is actually delivered, and it can point any of
// those names anywhere, including nowhere.
const KNOWN = ['poc-dest-1', 'poc-dest-2', 'poc-dest-3',
    'krb-dest-a', 'krb-dest-b'];

// The three failure classes the dead-destination act needs: a refused
// connection, an unroutable address that hangs, and a reachable broker whose
// topic has no leader (the topic itself is created by that act).
const FAILURE = {
    refused: { host: 'localhost', port: 9999 + env.PORT_OFFSET },
    blackhole: { host: '10.255.255.1', port: 9092 },
};

/**
 * The backbeat destination list.
 *
 * @param {Object} opts - only: restrict to these ids; dead: { id: 'refused'
 *   or 'blackhole' }; topics: { id: customer topic }; leaderless: { id:
 *   topic } for a reachable broker with an unwritable topic
 * @return {Array} destination list for a backbeat config
 */
function destinations(opts) {
    const o = opts || {};
    const topics = o.customerTopics || {};
    const ids = o.only || KNOWN;
    return ids.map(id => {
        const dest = {
            resource: id,
            type: 'kafka',
            host: 'localhost',
            port: env.KAFKA_PORT,
            topic: topics[id] || `customer-topic-${id.slice(-1)}`,
            auth: {},
        };
        if (o.leaderless && o.leaderless[id]) {
            dest.topic = o.leaderless[id];
        }
        if (o.dead && FAILURE[o.dead[id]]) {
            Object.assign(dest, FAILURE[o.dead[id]]);
        }
        return dest;
    });
}

/**
 * Build a backbeat configuration.
 *
 * @param {Object} [opts] - pool: add the deliveryPool block; workgroups: add
 *   the workgroups block; only, dead, leaderless and customerTopics as
 *   destinations() takes them; concurrency, deliveryTimeoutMs, probePort,
 *   internalTopic, deliveryTopic, legacyGroupPrefix, deliveryGroup
 * @return {Object} the configuration
 */
function backbeat(opts) {
    const o = opts || {};
    const d = template('backbeat-notification.json');
    d.zookeeper.connectionString = `127.0.0.1:${env.ZK_PORT}`;
    d.kafka.hosts = `127.0.0.1:${env.KAFKA_PORT}`;
    d.redis.port = env.REDIS_PORT;
    d.server.port = env.BACKBEAT_API_PORT;
    d.queuePopulator.mongo.replicaSetHosts = `localhost:${env.MONGO_PORT}`;
    d.queuePopulator.zookeeperPath = env.ZK_POPULATOR_PATH;
    d.queuePopulator.probeServer = {
        bindAddress: '0.0.0.0',
        port: o.populatorProbePort || env.POPULATOR_PROBE_PORT,
    };
    const n = d.extensions.notification;
    n.topic = o.internalTopic || env.INTERNAL_TOPIC;
    n.notificationFailedTopic = env.FAILED_TOPIC;
    n.queueProcessor.groupId = o.legacyGroupPrefix || env.LEGACY_GROUP_PREFIX;
    n.destinations = destinations(o);
    if (o.pool) {
        n.zookeeperPath = env.ZK_POPULATOR_PATH;
        n.deliveryPool = {
            enabled: true,
            topic: o.deliveryTopic || env.DELIVERY_TOPIC,
            groupId: o.deliveryGroup || env.DELIVERY_GROUP,
            deliveryTimeoutMs: o.deliveryTimeoutMs || 30000,
            producerIdleMs: 300000,
            maxProducers: 50,
            concurrency: o.concurrency || 1000,
            maxQueued: 1000,
            probeServer: {
                bindAddress: '0.0.0.0',
                port: o.probePort || env.probePort(1),
            },
        };
        if (o.workgroups) {
            n.deliveryPool.workgroups = Object.assign({
                zookeeperPath: env.ZK_WORKGROUPS_PATH,
                cachePath: path.join(env.RUN,
                    `workgroups-cache-${env.RUN_ID}.json`),
            }, typeof o.workgroups === 'object' ? o.workgroups : {});
        }
    } else {
        delete n.deliveryPool;
    }
    return d;
}

/**
 * Build a CloudServer configuration for the OPTIONAL host fallback. The demo
 * normally uses the containerised CloudServer of the compose stack, which
 * carries its own config and its own destination list.
 *
 * Every port it binds or dials moves by the offset, not only the S3 one:
 * with only `port` moved, a second CloudServer on the machine starts its
 * listener and then dies on the first one's metrics port.
 *
 * CloudServer never delivers anything. Its destination list only decides
 * which ARNs a PutBucketNotificationConfiguration may name, so it can list
 * every destination with any endpoint and never needs restarting when the
 * backbeat side changes.
 *
 * @return {Object} the configuration
 */
function cloudserver() {
    const cs = template('cloudserver-config.json');
    cs.port = env.CLOUDSERVER_PORT;
    cs.mongodb.replicaSetHosts = `localhost:${env.MONGO_PORT}`;
    if (typeof cs.metricsPort === 'number') {
        cs.metricsPort += env.PORT_OFFSET;
    }
    ['dataDaemon', 'metadataDaemon', 'pfsDaemon', 'dataClient',
        'metadataClient', 'pfsClient', 'backbeat'].forEach(k => {
        if (cs[k] && typeof cs[k].port === 'number') {
            cs[k].port += env.PORT_OFFSET;
        }
    });
    // every destination the backbeat side might use has to be listed here,
    // because this list is what a PutBucketNotificationConfiguration is
    // validated against. The endpoints in it are never dialled.
    cs.bucketNotificationDestinations = cs.bucketNotificationDestinations
        .map(d => Object.assign({}, d, { host: 'localhost', port: env.KAFKA_PORT }));
    return cs;
}

/**
 * Write a config and return its path.
 *
 * @param {String} name - file name under demo/conf/generated
 * @param {Object} doc - the configuration
 * @param {String} [alsoIn] - a directory to copy it into, usually the act's
 *   evidence directory
 * @return {String} path written
 */
function write(name, doc, alsoIn) {
    const dir = path.join(env.CONF, 'generated');
    fs.mkdirSync(dir, { recursive: true });
    const file = path.join(dir, name);
    fs.writeFileSync(file, `${JSON.stringify(doc, null, 4)}\n`);
    if (alsoIn) {
        fs.mkdirSync(alsoIn, { recursive: true });
        fs.writeFileSync(path.join(alsoIn, name),
            `${JSON.stringify(doc, null, 4)}\n`);
    }
    return file;
}

module.exports = {
    KNOWN,
    TEMPLATES,
    SHIM_DIR,
    shimArgs,
    destinations,
    backbeat,
    cloudserver,
    write,
    template,
};
