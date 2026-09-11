'use strict';

/**
 * Everything the demo suite needs to know about where it is and which ports
 * to use. Nothing here is hardcoded to one machine: the backbeat repo is
 * resolved from this file's own location, and the two knobs that cannot be
 * derived (the cloudserver checkout and the node 22 bin directory) come from
 * demo/.env or the environment.
 */

const { execFileSync } = require('child_process');
const fs = require('fs');
const os = require('os');
const path = require('path');

function gitToplevel(from) {
    try {
        return execFileSync('git', ['-C', from, 'rev-parse', '--show-toplevel'],
            { encoding: 'utf8', stdio: ['ignore', 'pipe', 'ignore'] }).trim();
    } catch {
        return null;
    }
}

// The stack lives at poc-demo/ in the repository root, and this file at
// tests/functional/demo/lib/, so four levels up is the root.
const DEMO = path.resolve(__dirname, '..', '..', '..', '..', 'poc-demo');

/**
 * Read demo/.env without a shell. Values may reference $HOME.
 *
 * @return {Object} key to value
 */
function readEnvFile() {
    const file = path.join(DEMO, '.env');
    const out = {};
    if (!fs.existsSync(file)) {
        return out;
    }
    fs.readFileSync(file, 'utf8').split('\n').forEach(line => {
        const m = /^\s*([A-Za-z_][A-Za-z0-9_]*)=(.*)$/.exec(line);
        if (!m) {
            return;
        }
        let value = m[2].trim();
        if ((value.startsWith('"') && value.endsWith('"'))
            || (value.startsWith("'") && value.endsWith("'"))) {
            value = value.slice(1, -1);
        }
        out[m[1]] = value.replace(/\$HOME/g, os.homedir())
            .replace(/\$\{HOME\}/g, os.homedir());
    });
    return out;
}

const FILE_ENV = readEnvFile();

/**
 * Environment wins over demo/.env, which wins over the default.
 *
 * @param {String} name - variable name
 * @param {String|Number} def - default
 * @return {String} value
 */
function knob(name, def) {
    if (process.env[name] !== undefined && process.env[name] !== '') {
        return process.env[name];
    }
    if (FILE_ENV[name] !== undefined && FILE_ENV[name] !== '') {
        return FILE_ENV[name];
    }
    return def === undefined ? undefined : String(def);
}

function num(name, def) {
    const v = knob(name, def);
    return v === undefined ? undefined : Number(v);
}

// The backbeat repo: an explicit override, else this file's own repository,
// which is what it will be once this material lives in the branch.
const BACKBEAT_DIR = knob('BACKBEAT_DIR') || gitToplevel(__dirname);
if (!BACKBEAT_DIR || !fs.existsSync(path.join(BACKBEAT_DIR, 'package.json'))) {
    throw new Error('cannot find the backbeat repository. Set BACKBEAT_DIR in '
        + `${path.join(DEMO, '.env')} or in the environment.`);
}

// CLOUDSERVER_DIR may be relative in demo/.env. Resolve it against the
// backbeat repo, then against demo/, and take the first that exists, so it
// does not depend on the process's working directory.
function resolveCloudserver() {
    const raw = knob('CLOUDSERVER_DIR');
    if (!raw) {
        return path.resolve(BACKBEAT_DIR, '..', 'cloudserver');
    }
    if (path.isAbsolute(raw)) {
        return raw;
    }
    const bases = [BACKBEAT_DIR, DEMO, process.cwd()];
    const found = bases.map(b => path.resolve(b, raw))
        .find(p2 => fs.existsSync(p2));
    return found || path.resolve(BACKBEAT_DIR, raw);
}

const CLOUDSERVER_DIR = resolveCloudserver();

// The node 22 bin directory. mocha is already running on the right node, so
// the safe default is the directory of THIS process's own node: on a fresh
// machine the pinned nvm path does not exist, and procs.js would then spawn
// with a PATH that has no node at all. An explicit NODE_BIN knob still wins,
// but only when the node in it is actually executable; otherwise fall back
// to the interpreter this process is running under.
function resolveNodeBin() {
    const raw = knob('NODE_BIN');
    if (raw) {
        try {
            fs.accessSync(path.join(raw, 'node'), fs.constants.X_OK);
            return raw;
        } catch {
            // the knob points somewhere with no usable node; fall through
        }
    }
    return path.dirname(process.execPath);
}
const NODE_BIN = resolveNodeBin();

const PORT_OFFSET = num('PORT_OFFSET', 0);
const port = (name, base) => num(name, base + PORT_OFFSET);

// 'demo' is the recording default: brisk enough that a full run fits a
// recording window, slow enough to talk over. 'slow' doubles every
// deliberate wait for a careful take, 'fast' is for iterating, 'normal' is
// the original one-to-one.
const PACE = (knob('DEMO_PACE', 'demo') || 'demo').toLowerCase();
const PACE_FACTOR = { slow: 2, normal: 1, demo: 0.6, fast: 0.35 }[PACE] || 1;

const RUN_ID = knob('DEMO_RUN_ID',
    new Date().toISOString().replace(/[-:T.Z]/g, '').slice(2, 14));

// Canonical names by default, so the Grafana dashboard's own defaults match
// with no typing. DEMO_TOPIC_SUFFIX=<id> isolates a run instead, at the cost
// of setting the dashboard's two textbox variables.
const SUFFIX = knob('DEMO_TOPIC_SUFFIX', '');
const suffixed = base => (SUFFIX ? `${base}-${SUFFIX}` : base);

const env = {
    DEMO,
    EVIDENCE: path.join(DEMO, 'evidence'),
    CONF: path.join(DEMO, 'conf'),
    RUN: path.join(DEMO, 'run'),
    BACKBEAT_DIR,
    CLOUDSERVER_DIR,
    NODE_BIN,
    NODE: path.join(NODE_BIN, 'node'),
    PROJECT: knob('COMPOSE_PROJECT_NAME', 'bnaasdemo'),
    PORT_OFFSET,
    RUN_ID,
    PACE,
    PACE_FACTOR,

    KAFKA_PORT: port('KAFKA_PORT', 9092),
    ZK_PORT: port('ZK_PORT', 2181),
    REDIS_PORT: port('REDIS_PORT', 6379),
    MONGO_PORT: port('MONGO_PORT', 27117),
    CLOUDSERVER_PORT: port('CLOUDSERVER_PORT', 8010),
    PROMETHEUS_PORT: port('PROMETHEUS_PORT', 9090),
    GRAFANA_PORT: port('GRAFANA_PORT', 3000),
    KAFKA_UI_PORT: port('KAFKA_UI_PORT', 8085),
    KRB_BROKER_PORT: port('KRB_BROKER_PORT', 19095),
    KRB_VERIFY_PORT: port('KRB_VERIFY_PORT', 19096),
    // probe ports: prometheus already scrapes 8920-8930 and 9920-9930
    PROBE_BASE: num('WORKER_PROBE_BASE', 8920 + PORT_OFFSET),
    POPULATOR_PROBE_PORT: num('POPULATOR_PROBE_PORT', 8910 + PORT_OFFSET),
    BACKBEAT_API_PORT: num('BACKBEAT_API_PORT', 8901 + PORT_OFFSET),

    INTERNAL_TOPIC: suffixed(knob('LEGACY_TOPIC', 'backbeat-bucket-notification')),
    FAILED_TOPIC: suffixed(knob('FAILED_TOPIC', 'backbeat-bucket-notification-failed')),
    DELIVERY_TOPIC: suffixed(knob('DELIVERY_TOPIC', 'bucket-notification-delivery')),
    DELIVERY_GROUP: suffixed(knob('DELIVERY_GROUP', 'bucket-notification-delivery-group')),
    LEGACY_GROUP_PREFIX: suffixed(knob('LEGACY_GROUP_PREFIX', 'bnaas-demo-notification-group')),
    // six, because the workgroups act needs six destinations to spread
    CUSTOMER_TOPICS: [1, 2, 3, 4, 5, 6].map(n => suffixed(`customer-topic-${n}`)),
    INTERNAL_PARTITIONS: num('LEGACY_TOPIC_PARTITIONS', 4),
    DELIVERY_PARTITIONS: num('DELIVERY_TOPIC_PARTITIONS', 3),
    // Which topic the pool worker consumes. 'internal' is the decided model:
    // the workers read today's topic and match per destination themselves,
    // the populator is untouched, and a migration or a layout change is a
    // container swap. 'delivery' is the previous model, a destination-keyed
    // topic the populator addressed records to, kept for reference runs.
    SOURCE: (knob('DEMO_SOURCE', 'internal') || 'internal').toLowerCase(),

    ZK_POPULATOR_PATH: knob('ZK_POPULATOR_PATH', '/bnaas-demo/queue-populator'),
    ZK_WORKGROUPS_PATH: knob('ZK_WORKGROUPS_PATH', '/bnaas-demo/delivery-workgroups'),

    S3_ACCESS_KEY: knob('S3_ACCESS_KEY', 'accessKey1'),
    S3_SECRET_KEY: knob('S3_SECRET_KEY', 'verySecretKey1'),

    // which acts to run, e.g. DEMO_ACTS=02,04,06
    ACTS: (knob('DEMO_ACTS', '') || '').split(',').map(s => s.trim())
        .filter(Boolean),

    knob,
    num,
    suffixed,
};

env.S3_ENDPOINT = `http://localhost:${env.CLOUDSERVER_PORT}`;
// the topic the pool consumes, which every drain, freeze and head check on
// the pool side has to look at
env.POOL_TOPIC = env.SOURCE === 'delivery' ? env.DELIVERY_TOPIC
    : env.INTERNAL_TOPIC;
env.POOL_PARTITIONS = env.SOURCE === 'delivery' ? env.DELIVERY_PARTITIONS
    : env.INTERNAL_PARTITIONS;
env.probePort = n => env.PROBE_BASE + n;
env.legacyGroup = dest => `${env.LEGACY_GROUP_PREFIX}-${dest}`;
env.customerTopic = n => env.CUSTOMER_TOPICS[n - 1];

// pace-scaled pause, in milliseconds
env.pause = ms => Math.round(ms * env.PACE_FACTOR);

// A workload duration in seconds, scaled by pace with a floor. The fixed
// driver durations an act runs (how long traffic flows during a cutover or a
// reshard) are not env.pause waits, so they need their own knob: at demo and
// fast pace a shorter run still exercises the same procedure with fewer
// events, which is what keeps the whole suite inside a recording window. The
// floor keeps enough traffic for every consumer group to see load.
env.workSecs = (baseSeconds, floorSeconds) => {
    const factor = { slow: 1.25, normal: 1, demo: 0.55, fast: 0.4 }[env.PACE]
        || 1;
    return Math.max(floorSeconds || 30, Math.round(baseSeconds * factor));
};

env.urls = () => ({
    grafana: `http://localhost:${env.GRAFANA_PORT}`,
    prometheus: `http://localhost:${env.PROMETHEUS_PORT}`,
    kafkaUi: `http://localhost:${env.KAFKA_UI_PORT}`,
    s3: env.S3_ENDPOINT,
});

module.exports = env;
