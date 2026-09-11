'use strict';

/**
 * What happens once, before any act.
 *
 * The stack the acts run against is shared with whoever used it last, so
 * nothing here assumes a clean slate: every topic the demo uses is
 * recreated, every consumer group it uses is deleted, and the workgroups
 * document is removed. Leftover records and a stale committed offset are
 * exactly what makes a healthy consumer look wedged and a Grafana panel lie.
 */

const fs = require('fs');
const path = require('path');
const env = require('./env');
const kafka = require('./kafka');
const zk = require('./zk');
const conf = require('./conf');
const s3lib = require('./s3');
const procs = require('./procs');
const wait = require('./wait');
const { say, note, line, rule } = require('./narrate');
const { run } = require('./sh');

const ctx = {
    s3: null,
    cloudserver: null,
    startedCloudserver: false,
    configs: {},
    customerTopicOf: {},
};

function branchOf(dir) {
    const b = run('git', ['-C', dir, 'rev-parse', '--abbrev-ref', 'HEAD']).out;
    const t = run('git', ['-C', dir, 'rev-parse', '--short', 'HEAD']).out;
    return `${b} @ ${t}`;
}

function header() {
    rule('=');
    line(' BNaaS delivery pool: the demo, as a test suite');
    line('');
    line(` backbeat      ${env.BACKBEAT_DIR}`);
    line(`               ${branchOf(env.BACKBEAT_DIR)}`);
    line(` cloudserver   ${env.CLOUDSERVER_DIR}`);
    line(` node          ${env.NODE}`);
    line(` port offset   ${env.PORT_OFFSET}  (kafka ${env.KAFKA_PORT}, `
        + `zookeeper ${env.ZK_PORT}, mongo ${env.MONGO_PORT})`);
    line(` run id        ${env.RUN_ID}   pace ${env.PACE}`);
    line('');
    line(' open these beside the terminal:');
    line(`   grafana      http://localhost:${env.GRAFANA_PORT}   `
        + 'dashboard "BNaaS delivery pool"');
    line(`   kafka ui     http://localhost:${env.KAFKA_UI_PORT}`);
    line(`   prometheus   http://localhost:${env.PROMETHEUS_PORT}`);
    line(`   zoonavigator http://localhost:${env.num('ZOONAV_PORT', 9000 + env.PORT_OFFSET)}`
        + `   node ${env.ZK_WORKGROUPS_PATH}`);
    line(`   s3           ${env.S3_ENDPOINT}`);
    rule('=');
}

/**
 * Recreate every topic the demo uses, and delete every consumer group it
 * uses, so a run starts from offset 0 with nobody's leftovers.
 *
 * @return {Object} what happened, for the record
 */
function resetKafka() {
    const report = { topics: {}, groups: [] };
    say(`kafka container ${kafka.kafkaContainer()}, `
        + `broker port ${env.KAFKA_PORT}`);

    // groups first: a topic cannot be recreated cleanly under a live group,
    // and deleting offsets from a group with a member triggers a rebalance
    const live = kafka.groups();
    const mine = live.filter(g => g === env.DELIVERY_GROUP
        || g.startsWith(`${env.DELIVERY_GROUP}-`)
        || g.startsWith(`${env.LEGACY_GROUP_PREFIX}-`)
        || g.startsWith('bn-replay-'));
    mine.forEach(g => {
        const r = kafka.deleteGroup(g);
        report.groups.push(`${g}: ${r.ok ? 'deleted' : 'kept'}`);
    });
    if (mine.length) {
        note(`consumer groups cleared: ${report.groups.join(', ')}`);
    } else {
        note('no consumer group of this demo existed yet');
    }
    // a killed consumer keeps its membership until its session expires, and
    // the cutover tool refuses to pre-seed a group that still has members
    const held = kafka.waitForNoMembers(90000);
    if (held.length) {
        note('WARNING: these groups still have live members: '
            + `${held.join(', ')}. A workgroup cutover will refuse to `
            + 'pre-seed them.');
    } else if (mine.length) {
        note('every one of them is now memberless, so a cutover can '
            + 'pre-seed cleanly');
    }

    // The topics the pipeline CONSUMES are recreated, because leftover
    // records there are read by a fresh consumer and counted as drops, and
    // because a stale committed offset on them reads exactly like a wedge.
    // The customer topics are only ever dumped from an offset each act
    // records for itself, so their history is harmless and keeping it saves
    // a minute of a recording. DEMO_RECREATE_TOPICS=all recreates those too.
    // The decided model consumes one topic. The destination-keyed delivery
    // topic belongs to the retired DEMO_SOURCE=delivery path, so it is only
    // created when that path is the one being run: recreating it on every
    // run would put a second topic back on the broker that nothing reads,
    // and the first question in the room is how many topics this needs.
    const pipeline = [
        [env.INTERNAL_TOPIC, env.INTERNAL_PARTITIONS],
        [env.FAILED_TOPIC, 1],
    ];
    if (env.SOURCE === 'delivery') {
        pipeline.push([env.DELIVERY_TOPIC, env.DELIVERY_PARTITIONS]);
    }
    const customer = env.CUSTOMER_TOPICS.map(t => [t, 1]);
    const all = env.knob('DEMO_RECREATE_TOPICS', '') === 'all';
    report.topics = kafka.recreateTopics(all ? pipeline.concat(customer)
        : pipeline);
    if (!all) {
        customer.forEach(([name, partitions]) => {
            if (!kafka.topicExists(name)) {
                kafka.createTopic(name, partitions);
                report.topics[name] = 'created';
            } else {
                report.topics[name] = 'kept, dumped from a recorded offset';
            }
        });
    }
    note(`topics: ${Object.entries(report.topics)
        .map(([k, v]) => `${k} ${v}`).join(', ')}`);
    note('every partition leader-confirmed three times before any consumer '
        + 'joins, which is the wedge mitigation');

    if (Object.values(report.topics).some(v => v.startsWith('kept, the broker'))) {
        note('WARNING: the broker would not delete a topic, so this run '
            + 'starts on top of what was already there. Records from before '
            + 'will be counted as unexpected, not as gaps.');
    }
    return report;
}

/** remove the workgroups document, so act 06 starts from no generation */
function resetWorkgroups() {
    if (zk.workgroupsDoc()) {
        zk.deleteAll(env.ZK_WORKGROUPS_PATH);
        note(`removed the workgroups document at ${env.ZK_WORKGROUPS_PATH}`);
    }
}

/**
 * The S3 endpoint the acts drive.
 *
 * CloudServer is a container of the compose stack, published on
 * CLOUDSERVER_PORT. If that port does not answer but the stack's CloudServer
 * container publishes another host port for 8000, use that and say so: the
 * stack's own preflight moves the port when something else holds it.
 *
 * @return {Promise} resolves with the endpoint, or null
 */
async function resolveS3() {
    const probe = await wait.get(`${env.S3_ENDPOINT}/`, 4000);
    if (probe) {
        return env.S3_ENDPOINT;
    }
    const published = run('docker', ['ps', '--format', '{{.Names}}\t{{.Ports}}'])
        .out.split('\n')
        .filter(l => /cloudserver/i.test(l) && l.includes('->8000/tcp'))
        .map(l => (/:(\d+)->8000\/tcp/.exec(l) || [])[1])
        .filter(Boolean);
    for (const port of published) {
        const url = `http://localhost:${port}`;
         
        if (await wait.get(`${url}/`, 4000)) {
            note(`CloudServer answers on ${url}, not on ${env.S3_ENDPOINT}: `
                + 'the stack published a different port. Using it.');
            env.S3_ENDPOINT = url;
            return url;
        }
    }
    return null;
}

/**
 * Wait for the whole stack to be ready, using the stack's own check, and
 * then for CloudServer to answer.
 *
 * The optional host CloudServer, from a source checkout, is the fallback for
 * a machine where the container will not run. It is off unless
 * DEMO_HOST_CLOUDSERVER=1, because two CloudServers on one machine fight
 * over the same ports.
 *
 * @return {Promise} resolves when the stack is ready
 */
async function ensureStackReady() {
    const script = path.join(env.DEMO, 'bin', 'wait-ready.sh');
    if (fs.existsSync(script)) {
        const r = run('bash', [script, '--timeout', '180'], { timeout: 220000 });
        if (r.ok) {
            say('stack ready, by the stack\'s own check (bin/wait-ready.sh)');
        } else {
            note(`bin/wait-ready.sh is not satisfied yet: ${r.out || r.err}`);
        }
    }
    const endpoint = await resolveS3();
    if (endpoint) {
        say(`S3 endpoint ${endpoint}, CloudServer answering`);
        return;
    }
    if (env.knob('DEMO_HOST_CLOUDSERVER', '') !== '1') {
        throw new Error('CloudServer is not answering on '
            + `${env.S3_ENDPOINT}. Bring the stack up (demo/bin/stack-up.sh), `
            + 'or set DEMO_HOST_CLOUDSERVER=1 to start one from a source '
            + 'checkout instead.');
    }
    const file = conf.write('cloudserver-config.json', conf.cloudserver());
    ctx.configs.cloudserver = file;
    fs.mkdirSync(env.EVIDENCE, { recursive: true });
    const logFile = path.join(env.EVIDENCE, 'cloudserver.log');
    say(`starting a host CloudServer on ${env.S3_ENDPOINT} (log ${logFile})`);
    note('this is the fallback: the demo normally uses the container');
    ctx.cloudserver = new procs.Proc({
        name: 'cloudserver',
        argv: ['index.js'],
        configFile: file,
        logFile,
        readyRe: /server started/,
        cwd: env.CLOUDSERVER_DIR,
        keep: true,
        extraEnv: {
            S3METADATA: 'mongodb',
            S3DATA: 'mem',
            S3VAULT: 'mem',
            REMOTE_MANAGEMENT_DISABLE: '1',
            S3_CONFIG_FILE: file,
        },
    }).spawnOnce();
    ctx.startedCloudserver = true;
    await procs.waitReady(ctx.cloudserver, 120000);
    const ok = await wait.until('cloudserver to answer',
        async () => !!(await wait.get(`${env.S3_ENDPOINT}/`, 3000)), 60000);
    if (!ok) {
        throw new Error(`cloudserver did not answer on ${env.S3_ENDPOINT}`);
    }
    say('host CloudServer answering');
}

// Only one demo run at a time may drive one broker: two runs share the same
// topics and consumer groups, and the reset one does at startup deletes the
// groups the other just seeded. The lock is keyed to the compose project and
// the port offset, so runs against different stacks do not collide.
const LOCK = path.join(env.RUN, `demo-${env.PROJECT}-${env.PORT_OFFSET}.lock`);

/**
 * Take the single-run lock, or throw naming who holds it. A lock whose pid is
 * no longer alive is stale and is taken over.
 *
 * @return {undefined}
 */
function acquireLock() {
    fs.mkdirSync(env.RUN, { recursive: true });
    if (fs.existsSync(LOCK)) {
        const held = (fs.readFileSync(LOCK, 'utf8').trim().split(/\s+/)[0]) || '';
        const pid = Number(held);
        let alive = false;
        try {
            process.kill(pid, 0);
            alive = pid !== process.pid;
        } catch {
            alive = false;
        }
        if (alive) {
            // A run that was interrupted a moment ago is still stopping its
            // processes and releasing this lock; give it a few seconds.
            const deadline = Date.now() + 15000;
            note(`another run (pid ${pid}) holds the lock, waiting up to 15 s`);
            while (Date.now() < deadline) {
                require('./sh').sleepSync(500);
                try {
                    process.kill(pid, 0);
                } catch {
                    alive = false;
                    break;
                }
            }
        }
        if (alive) {
            throw new Error('another demo run is already driving this stack '
                + `(pid ${pid}, lock ${LOCK}). Wait for it to finish, or stop `
                + 'it with yarn demo:stop, then run again. Different '
                + 'PORT_OFFSET values do not collide.');
        }
        note(`taking over a stale lock left by pid ${pid || 'unknown'}`);
    }
    fs.writeFileSync(LOCK, `${process.pid} ${new Date().toISOString()}\n`);
}

/** Release the single-run lock, but only if it is still ours. */
function releaseLock() {
    try {
        const held = Number(fs.readFileSync(LOCK, 'utf8').trim().split(/\s+/)[0]);
        if (held === process.pid) {
            fs.unlinkSync(LOCK);
        }
    } catch {
        // no lock to release
    }
}

/**
 * Everything before the first act.
 *
 * @return {Promise} resolves with the shared context
 */
async function globalSetup() {
    header();
    acquireLock();
    fs.mkdirSync(env.EVIDENCE, { recursive: true });
    fs.mkdirSync(env.RUN, { recursive: true });
    ctx.reset = resetKafka();
    resetWorkgroups();
    // one customer topic per destination the platform knows about
    conf.KNOWN.forEach((id, i) => {
        ctx.customerTopicOf[id] = env.CUSTOMER_TOPICS[i];
    });
    note(`destinations: ${conf.KNOWN.map(id => `${id} -> `
        + `${ctx.customerTopicOf[id]}`).join(', ')}`);
    note('those five are the names the containerised CloudServer validates a '
        + 'notification configuration against');
    await ensureStackReady();
    ctx.s3 = s3lib.client();
    say('setup done');
    line('');
    return ctx;
}

/**
 * Everything after the last act: stop what the suite started, leave the
 * stack itself alone.
 *
 * @return {Promise} resolves when nothing of ours is running
 */
async function globalTeardown() {
    line('');
    say('stopping every process this run started');
    procs.stopAll(true);
    await procs.sleep(1000);
    releaseLock();
    say(`evidence is under ${env.EVIDENCE}`);
}

module.exports = { ctx, globalSetup, globalTeardown, resetKafka, header, releaseLock };
