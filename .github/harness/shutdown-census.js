/*
 * BB-833 shutdown census driver.
 *
 * Terminates a consumer in a given state and measures whether it departs the
 * group cleanly, whether the process exits, how fast a survivor takes over,
 * and whether any message was lost or duplicated in the process.
 *
 * One JSON line per iteration on stdout (prefixed RESULT ), so the workflow's
 * summary job can tabulate across jobs. The harness code is held identical
 * across arms; only lib/ differs.
 *
 *   SCENARIO=idle|rejoining|draining|backlog  ITERATIONS=6  node this.js
 */
'use strict';

const fs = require('fs');
const os = require('os');
const path = require('path');
const { fork } = require('child_process');

const REPO = process.cwd();
const kafka = require(path.join(REPO, 'node_modules/node-rdkafka'));

const HOSTS = process.env.HOSTS || 'localhost:9092';
const SCENARIO = process.env.SCENARIO || 'draining';
const ITERATIONS = Number(process.env.ITERATIONS || 6);
const ARM = process.env.ARM || 'unknown';
const JOB = process.env.JOB_INDEX || '0';
const WORKER = path.join(__dirname, 'shutdown-census-worker.js');
// a retried job must not land on a topic still holding the previous run's
// messages, which would corrupt the offset and reconciliation figures
const RUN_NONCE = `${Date.now().toString(36)}${process.pid.toString(36)}`;

const PARTITIONS = 4;
// a pod's grace period: past this, kubernetes SIGKILLs and the member is left
// registered at the broker, which is the failure BB-833 is about
const GRACE_MS = 30000;
// after close() returns, how long node gets to exit on its own
const EXIT_GRACE_MS = 15000;
// session.timeout.ms is 45s, so a survivor that only takes over after that
// was waiting for eviction rather than being handed a LeaveGroup
const TAKEOVER_TIMEOUT_MS = 70000;
const CATCHUP_TIMEOUT_MS = 90000;
const SETTLE_TIMEOUT_MS = 60000;

const SCENARIOS = {
    // control: nothing in flight, group fully settled
    idle: { messages: 50, slowMs: 0, concurrency: 1 },
    // killed after releasing its partitions, while waiting to rejoin
    rejoining: { messages: 50, slowMs: 0, concurrency: 1 },
    // killed with a revoke outstanding and tasks still running
    draining: { messages: 40, slowMs: 6000, concurrency: 5 },
    // killed while actively chewing a backlog: the condition that exposed
    // the consume-during-shutdown defect, and the realistic rolling update
    backlog: { messages: 3000, slowMs: 5, concurrency: 10 },
};

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

function createTopic(topic) {
    return new Promise((resolve, reject) => {
        const admin = kafka.AdminClient.create({ 'metadata.broker.list': HOSTS });
        admin.createTopic({
            topic,
            num_partitions: PARTITIONS, // eslint-disable-line camelcase
            replication_factor: 1, // eslint-disable-line camelcase
        }, 20000, err => {
            admin.disconnect();
            if (err && !/already exists/i.test(err.message)) {
                return reject(err);
            }
            return resolve();
        });
    });
}

function produce(topic, count) {
    return new Promise((resolve, reject) => {
        const producer = new kafka.Producer(
            { 'metadata.broker.list': HOSTS, 'dr_cb': true });
        let delivered = 0;
        let settled = false;
        const finish = err => {
            if (settled) {
                return;
            }
            settled = true;
            try {
                producer.disconnect();
            } catch (e) { /* best effort */ } // eslint-disable-line no-unused-vars
            return err ? reject(err) : resolve();
        };
        producer.on('delivery-report', err => {
            if (err) {
                return finish(err);
            }
            delivered++;
            return delivered === count ? finish(null) : undefined;
        });
        producer.on('event.error', finish);
        producer.on('ready', () => {
            producer.setPollInterval(50);
            for (let seq = 0; seq < count; seq++) {
                producer.produce(topic, null,
                    Buffer.from(JSON.stringify({ seq })), `k${seq}`);
            }
        });
        producer.connect();
        setTimeout(() => finish(new Error('produce timed out')), 60000);
    });
}

function committedOffsets(topic, groupId) {
    return new Promise(resolve => {
        const consumer = new kafka.KafkaConsumer({
            'metadata.broker.list': HOSTS,
            'group.id': groupId,
            'enable.auto.commit': false,
        }, {});
        const done = value => {
            try {
                consumer.disconnect();
            } catch (e) { /* best effort */ } // eslint-disable-line no-unused-vars
            resolve(value);
        };
        consumer.on('ready', () => {
            const parts = Array.from({ length: PARTITIONS },
                (_, partition) => ({ topic, partition }));
            consumer.committed(parts, 10000, (err, offsets) => {
                if (err) {
                    return done(null);
                }
                return done(offsets.reduce((total, o) =>
                    total + (o.offset >= 0 ? o.offset : 0), 0));
            });
        });
        consumer.on('event.error', () => done(null));
        consumer.connect();
        setTimeout(() => done(null), 20000);
    });
}

/** one pod, with the state the driver needs to decide when to act */
function startPod(role, topic, groupId, cfg, outDir, logPath) {
    const child = fork(WORKER, [], {
        cwd: REPO,
        silent: true,
        env: {
            ...process.env,
            HOSTS,
            TOPIC: topic,
            GROUP_ID: groupId,
            ROLE: role,
            OUT_DIR: outDir,
            SLOW_MS: String(cfg.slowMs),
            DROP_EVERY: process.env.DROP_EVERY || '',
            CONCURRENCY: String(cfg.concurrency),
        },
    });
    const logs = fs.createWriteStream(logPath, { flags: 'a' });
    child.stdout.pipe(logs);
    child.stderr.pipe(logs);

    const pod = {
        role, child,
        subscribed: false,
        held: 0,
        processed: 0,
        started: 0,
        sawRevoke: false,
        sawAssign: false,
        heldAtClose: null,
        closeMs: null,
        closeAnsweredTwice: false,
        errors: [],
        uncaught: null,
        exited: false,
        exitCode: null,
        exitSignal: null,
        exitedAt: null,
    };
    child.on('message', msg => {
        switch (msg.type) {
        case 'subscribed':
            pod.subscribed = true;
            break;
        case 'rebalance':
            if (msg.kind === 'revoke') {
                pod.sawRevoke = true;
            } else if (msg.kind === 'assign') {
                pod.sawAssign = true;
            }
            break;
        case 'progress':
            pod.held = msg.held;
            pod.processed = msg.processed;
            pod.started = msg.started;
            break;
        case 'close-start':
            pod.heldAtClose = msg.held;
            break;
        case 'close-returned':
            pod.closeMs = msg.closeMs;
            break;
        case 'close-answered-again':
            pod.closeAnsweredTwice = true;
            break;
        case 'uncaught':
            pod.uncaught = msg.message;
            break;
        case 'error':
            pod.errors.push(msg.message);
            break;
        default:
            break;
        }
    });
    child.on('exit', (code, signal) => {
        pod.exited = true;
        pod.exitCode = code;
        pod.exitSignal = signal;
        pod.exitedAt = Date.now();
    });
    return pod;
}

/** resolves with elapsed ms, or -1 if the deadline passed */
async function waitFor(predicate, timeoutMs) {
    const start = Date.now();
    while (Date.now() - start < timeoutMs) {
        if (predicate()) {
            return Date.now() - start;
        }
        await sleep(100); // eslint-disable-line no-await-in-loop
    }
    return -1;
}

function readProcessed(outDir, role) {
    const file = path.join(outDir, `processed-${role}.txt`);
    if (!fs.existsSync(file)) {
        return [];
    }
    return fs.readFileSync(file, 'utf8').split('\n')
        .filter(line => line !== '')
        .map(Number)
        .filter(n => Number.isInteger(n));
}

async function runIteration(idx) {
    const cfg = SCENARIOS[SCENARIO];
    const stamp = `${ARM}-${SCENARIO}-${JOB}-${idx}-${RUN_NONCE}`;
    const topic = `bb833-census-${stamp}`;
    const groupId = `bb833-census-group-${stamp}`;
    const outDir = fs.mkdtempSync(path.join(os.tmpdir(), 'bb833-'));
    const keepDir = path.join(REPO, 'census-pod-logs');
    const logPath = path.join(outDir, 'pods.log');
    const record = {
        arm: ARM, scenario: SCENARIO, job: JOB, iteration: idx,
        dropEvery: Number(process.env.DROP_EVERY || 0),
        messages: cfg.messages, concurrency: cfg.concurrency,
    };
    let leaving;
    let survivor;

    try {
        await createTopic(topic);
        await produce(topic, cfg.messages);

        leaving = startPod('leaving', topic, groupId, cfg, outDir, logPath);
        if (await waitFor(() => leaving.held === PARTITIONS,
            SETTLE_TIMEOUT_MS) < 0) {
            record.skipped = 'leaving never took every partition';
            return record;
        }
        // it must be genuinely working before the survivor perturbs anything:
        // terminating a pod that has processed nothing exercises no drain, no
        // commit and no handover, and cannot distinguish a clean departure
        // from a broker that never delivered anything
        const workThreshold = SCENARIO === 'backlog' ? 50 : 5;
        if (await waitFor(() => leaving.processed >= workThreshold,
            SETTLE_TIMEOUT_MS) < 0) {
            record.skipped = 'leaving never processed enough messages';
            record.leavingProcessedIpc = leaving.processed;
            return record;
        }

        survivor = startPod('survivor', topic, groupId, cfg, outDir, logPath);
        if (await waitFor(() => survivor.subscribed, SETTLE_TIMEOUT_MS) < 0) {
            record.skipped = 'survivor never subscribed';
            return record;
        }

        let reached;
        if (SCENARIO === 'idle') {
            reached = await waitFor(() => survivor.held > 0 && leaving.held > 0
                && leaving.processed + survivor.processed >= cfg.messages,
            SETTLE_TIMEOUT_MS);
        } else if (SCENARIO === 'rejoining' || SCENARIO === 'draining') {
            // terminate the moment a revoke lands. With instant tasks the pod
            // has already un-assigned and is waiting to rejoin; with slow ones
            // its deferred un-assign is still outstanding. heldAtClose records
            // which of the two each iteration actually caught.
            reached = await waitFor(() => leaving.sawRevoke, SETTLE_TIMEOUT_MS);
        } else {
            reached = await waitFor(() => survivor.held > 0 && leaving.held > 0,
                SETTLE_TIMEOUT_MS);
        }
        if (reached < 0) {
            record.skipped = `never reached the ${SCENARIO} state`;
            return record;
        }

        record.heldBeforeTerm = leaving.held;
        record.processedBeforeTerm = leaving.processed;

        const termAt = Date.now();
        leaving.child.kill('SIGTERM');

        // 1. did close() come back inside the grace period?
        const closed = await waitFor(() => leaving.closeMs !== null, GRACE_MS);
        record.closeReturned = closed >= 0;
        record.closeMs = leaving.closeMs;
        record.closeAnsweredTwice = leaving.closeAnsweredTwice;
        record.heldAtClose = leaving.heldAtClose;

        // 2. did node then exit on its own, without a process.exit()?
        if (closed >= 0) {
            const exited = await waitFor(() => leaving.exited, EXIT_GRACE_MS);
            record.exitedNaturally = exited >= 0;
            record.exitMs = leaving.exitedAt ? leaving.exitedAt - termAt : null;
        } else {
            record.exitedNaturally = false;
            record.exitMs = null;
        }
        if (!leaving.exited) {
            record.sigkilled = true;
            leaving.child.kill('SIGKILL');
            await waitFor(() => leaving.exited, 10000);
        } else {
            record.sigkilled = false;
        }

        // 3. how long until the survivor owns the whole topic? a clean
        //    LeaveGroup is seconds; waiting out session.timeout.ms is ~45s
        const took = await waitFor(() => survivor.held === PARTITIONS,
            TAKEOVER_TIMEOUT_MS);
        record.takeoverMs = took >= 0 ? Date.now() - termAt : null;
        record.tookOver = took >= 0;

        // 4. let the survivor finish, so the reconciliation is meaningful
        const caughtUp = await waitFor(() => {
            const all = new Set([...readProcessed(outDir, 'leaving'),
                ...readProcessed(outDir, 'survivor')]);
            return all.size >= cfg.messages;
        }, CATCHUP_TIMEOUT_MS);
        record.caughtUp = caughtUp >= 0;

        survivor.child.kill('SIGKILL');
        await waitFor(() => survivor.exited, 10000);

        // 5. reconcile: nothing produced may go unprocessed
        const fromLeaving = readProcessed(outDir, 'leaving');
        const fromSurvivor = readProcessed(outDir, 'survivor');
        const seen = new Map();
        [...fromLeaving, ...fromSurvivor].forEach(seq =>
            seen.set(seq, (seen.get(seq) || 0) + 1));
        const lost = [];
        for (let seq = 0; seq < cfg.messages; seq++) {
            if (!seen.has(seq)) {
                lost.push(seq);
            }
        }
        record.processedTotal = fromLeaving.length + fromSurvivor.length;
        record.distinctProcessed = seen.size;
        record.lost = lost.length;
        record.lostSample = lost.slice(0, 10);
        record.duplicated =
            [...seen.values()].filter(count => count > 1).length;

        // 6. what the broker thinks was committed
        record.committedTotal = await committedOffsets(topic, groupId);

        // 7. anything the pods shouted about
        // the pods' own counters, independent of the files: distinguishes
        // "never consumed it" from "processed it but the record never landed"
        record.leavingProcessedIpc = leaving.processed;
        record.survivorProcessedIpc = survivor.processed;
        record.survivorHeld = survivor.held;
        record.leavingFileCount = fromLeaving.length;
        record.survivorFileCount = fromSurvivor.length;
        record.errors = leaving.errors.length + survivor.errors.length;
        record.uncaught = leaving.uncaught || survivor.uncaught || null;
        const podLogs = fs.existsSync(logPath) ?
            fs.readFileSync(logPath, 'utf8') : '';
        record.erroneousState = /Erroneous state/.test(podLogs);
        return record;
    } catch (err) {
        record.error = err.message;
        return record;
    } finally {
        [leaving, survivor].forEach(pod => {
            if (pod && !pod.exited) {
                try {
                    pod.child.kill('SIGKILL');
                } catch (e) { /* best effort */ } // eslint-disable-line no-unused-vars
            }
        });
        await sleep(4000);
        try {
            fs.mkdirSync(keepDir, { recursive: true });
            if (fs.existsSync(logPath)) {
                fs.copyFileSync(logPath, path.join(keepDir, `${stamp}.log`));
            }
        } catch (e) { /* best effort */ } // eslint-disable-line no-unused-vars
        try {
            fs.rmSync(outDir, { recursive: true, force: true });
        } catch (e) { /* best effort */ } // eslint-disable-line no-unused-vars
    }
}

async function main() {
    if (!SCENARIOS[SCENARIO]) {
        throw new Error(`unknown scenario ${SCENARIO}`);
    }
    process.stdout.write(
        `census arm=${ARM} scenario=${SCENARIO} job=${JOB} ` +
        `iterations=${ITERATIONS}\n`);
    process.stdout.write(
        `node-rdkafka ${require(path.join(REPO, 'node_modules/node-rdkafka/package.json')).version} ` +
        `librdkafka ${kafka.librdkafkaVersion}\n`);

    for (let idx = 1; idx <= ITERATIONS; idx++) {
        const record = await runIteration(idx);
        process.stdout.write(`RESULT ${JSON.stringify(record)}\n`);
    }
    // the harness itself must not be the thing that fails to exit
    process.exit(0);
}

main().catch(err => {
    process.stdout.write(`RESULT ${JSON.stringify({
        arm: ARM, scenario: SCENARIO, job: JOB, fatal: err.message,
    })}\n`);
    process.exit(1);
});
