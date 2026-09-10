'use strict';

/**
 * The steps every act shares: render a config, start a path, drive a
 * workload, dump what arrived and check it.
 */

const fs = require('fs');
const env = require('./env');
const conf = require('./conf');
const kafka = require('./kafka');
const procs = require('./procs');
const wait = require('./wait');
const check = require('./check');
const { say, note } = require('./narrate');

/**
 * A config for the legacy path: no deliveryPool block, so the code takes its
 * shipped 9.3 route.
 *
 * @param {Object} act - the act
 * @param {Object} ctx - shared context
 * @param {Object} [opts] - passed through to conf.backbeat
 * @return {String} config path
 */
function legacyConfig(act, ctx, opts) {
    const doc = conf.backbeat(Object.assign({
        pool: false,
        customerTopics: ctx.customerTopicOf,
    }, opts || {}));
    return conf.write(`backbeat-legacy-${act.id}.json`, doc, act.dir);
}

/**
 * A config for the delivery pool: the same file plus the deliveryPool block.
 *
 * @param {Object} act - the act
 * @param {Object} ctx - shared context
 * @param {Object} [opts] - passed through to conf.backbeat
 * @return {String} config path
 */
function poolConfig(act, ctx, opts) {
    const doc = conf.backbeat(Object.assign({
        pool: true,
        customerTopics: ctx.customerTopicOf,
    }, opts || {}));
    const name = `backbeat-pool-${act.id}`
        + `${opts && opts.tag ? `-${opts.tag}` : ''}.json`;
    return conf.write(name, doc, act.dir);
}

/**
 * Start the populator, narrated.
 *
 * @param {Object} act - the act
 * @param {String} configFile - config
 * @param {String} label - 'legacy' or 'pool'
 * @return {Promise} resolves with the process
 */
async function startPopulator(act, configFile, label) {
    say(`starting the queue populator on the ${label} path`);
    note(`config ${configFile}`);
    note(label === 'pool'
        ? `it will publish addressed records to ${env.DELIVERY_TOPIC}, one per `
          + 'matching destination, keyed by destination'
        : `it will publish to ${env.INTERNAL_TOPIC}, one record per object`);
    const p = procs.populator(act, configFile, label);
    const secs = await procs.waitReady(p, 90000);
    say(`populator up as pid ${p.pid} after ${secs}s`);
    act.timeline(`populator ${label} START pid=${p.pid}`);
    return p;
}

/**
 * Start one legacy per-destination queue processor, narrated.
 *
 * @param {Object} act - the act
 * @param {String} configFile - config
 * @param {String} dest - destination id
 * @param {Object} [opts] - { expectFailure }
 * @return {Promise} resolves with the process, or null when it exited and
 *   expectFailure was set
 */
async function startProcessor(act, configFile, dest, opts) {
    say(`starting the legacy queue processor for ${dest}`);
    note(`group ${env.legacyGroup(dest)}; one process per destination is what`);
    note('today\'s deployment does, twelve of them per destination on a real');
    note('platform, eleven idle standbys among them');
    let p = procs.legacyProcessor(act, configFile, dest);
    try {
        const secs = await procs.waitReady(p, 45000);
        say(`processor ${dest} up as pid ${p.pid} after ${secs}s`);
        act.timeline(`processor ${dest} START pid=${p.pid}`);
        if (!opts || opts.cure !== false) {
            p = await cureProcessorChurn(act, configFile, dest, p);
        }
        return p;
    } catch (err) {
        if (opts && opts.expectFailure) {
            say(`processor ${dest} did not come up, which is the point`);
            return p;
        }
        throw err;
    }
}


/**
 * The same wedge cure for a legacy queue processor. It has no metrics route
 * on this path, so its health signal is its own log: a healthy one issues
 * sends, a wedged one only cycles assign and revoke.
 *
 * @param {Object} act - the act
 * @param {String} configFile - config
 * @param {String} dest - destination id
 * @param {Object} proc - the process
 * @return {Promise} resolves with the running process
 */
async function cureProcessorChurn(act, configFile, dest, proc) {
    let p = proc;
    for (let attempt = 0; attempt < 2; attempt++) {
        const first = p.rebalances().revoke;
         
        await wait.sleep(env.pause(20000));
        const bal = p.rebalances();
        const sends = p.count(/sending message to external destination/g);
        if (bal.revoke - first < 3 || sends > 0) {
            return p;
        }
        note(`processor ${dest} is WEDGED: ${bal.assign} assigns, `
            + `${bal.revoke} revokes, no sends. Restarting that one process,`);
        note('  which is the cure. Up to 45s of it is the wedged member\'s');
        note('  group session expiring.');
        act.timeline(`processor ${dest} WEDGED, restarting`);
        p.stop();
         
        await wait.sleep(env.pause(6000));
        p = procs.legacyProcessor(act, configFile, dest);
         
        await procs.waitReady(p, 60000);
        say(`processor ${dest} restarted as pid ${p.pid}`);
    }
    return p;
}

/**
 * Start a delivery worker, narrated.
 *
 * @param {Object} act - the act
 * @param {String} configFile - config
 * @param {Number} n - worker index
 * @param {Object} [opts] - { workgroupId, autoRestart, quiet }
 * @return {Promise} resolves with the process
 */
async function startWorker(act, configFile, n, opts) {
    const o = opts || {};
    if (!o.quiet) {
        say(`starting delivery worker ${n}`
            + `${o.workgroupId ? ` for workgroup ${o.workgroupId}` : ''}`);
        note(`probe http://localhost:${env.probePort(n)}/metrics, `
            + 'liveness /_/live');
    }
    let p = procs.worker(act, configFile, n, o);
    const secs = await procs.waitReady(p, 90000);
    say(`worker ${n} up as pid ${p.pid} after ${secs}s`);
    act.timeline(`worker${n} START pid=${p.pid} wg=${o.workgroupId || '-'}`);
    if (o.cure === false) {
        return p;
    }
    p = await waitForProbe(act, configFile, n, p, o);
    p = await cureChurn(act, configFile, n, p, o);
    return p;
}

/**
 * Make sure a worker's probe server is answering.
 *
 * A probe that cannot bind is non-fatal by design: the worker keeps
 * delivering, it is just unscrapeable. That is the right call in production
 * and the wrong one for a demo, whose numbers come from that endpoint, and a
 * restart can lose the race for its own port against the process it
 * replaced. So confirm it, and restart once if it never answers.
 *
 * @param {Object} act - the act
 * @param {String} configFile - config
 * @param {Number} n - worker index
 * @param {Object} proc - the process
 * @param {Object} opts - as startWorker takes them
 * @return {Promise} resolves with the running process
 */
async function waitForProbe(act, configFile, n, proc, opts) {
    let p = proc;
    for (let attempt = 0; attempt < 2; attempt++) {
         
        const ok = await wait.until(`worker ${n}'s probe to answer`,
            async () => (await wait.liveness(n)) === 200, 45000, 2000);
        if (ok) {
            return p;
        }
        note(`worker ${n}'s probe on ${env.probePort(n)} never answered. A`);
        note('  probe that cannot bind is non-fatal for delivery and fatal');
        note('  for the demo\'s numbers, so restart it once, giving the port');
        note('  time to be released.');
        p.stop();
         
        await wait.sleep(10000);
        p = procs.worker(act, configFile, n, opts);
         
        await procs.waitReady(p, 90000);
        say(`worker ${n} restarted as pid ${p.pid} for its probe`);
    }
    return p;
}

/**
 * Watch a freshly started consumer for the wedge, and cure it.
 *
 * The signature is the one design/06 records: a live group member holding
 * all its partitions, cycling assign then revoke then "processing queue
 * idle, un-assigning" about once a second, delivering nothing, with a
 * liveness probe that still answers 200. It fired on 5 of 21 consumer starts
 * during the migration round, so a demo that starts consumers has to handle
 * it rather than hope. The cure is a restart of that one process, and up to
 * 45 seconds of it is the wedged member's group session expiring.
 *
 * An idle worker with nothing to read is NOT wedged: the signature needs the
 * revoke count to keep climbing.
 *
 * @param {Object} act - the act
 * @param {String} configFile - the config it runs with
 * @param {Number} n - worker index
 * @param {Object} proc - the process
 * @param {Object} opts - as startWorker takes them
 * @return {Promise} resolves with the running process
 */
async function cureChurn(act, configFile, n, proc, opts) {
    let p = proc;
    for (let attempt = 0; attempt < 2; attempt++) {
        const first = p.rebalances().revoke;
        const headFirst = kafka.headTotal(env.DELIVERY_TOPIC);
         
        await wait.sleep(env.pause(20000));
        const bal = p.rebalances();
         
        const delivered = await wait.counter(n, 'delivered');
         
        const skipped = await wait.counter(n, 'skipped');
        // An idle consumer with nothing to read cycles assign, revoke and
        // "processing queue idle, un-assigning" too: that is the same log
        // signature as the wedge and it is harmless. What separates them is
        // whether there was anything to consume. So only call it a wedge
        // when records arrived and nothing moved.
        const arrived = kafka.headTotal(env.DELIVERY_TOPIC) - headFirst >= 5;
        const churning = arrived && bal.revoke - first >= 3
            && delivered === 0 && skipped === 0;
        if (!churning) {
            if (bal.revoke > 0 || delivered > 0) {
                say(`worker ${n}: ${bal.assign} assign, ${bal.revoke} revoke, `
                    + `${delivered} delivered, ${skipped} skipped`);
            }
            return p;
        }
        note(`worker ${n} is WEDGED: ${bal.assign} assigns and ${bal.revoke}`);
        note('  revokes, nothing delivered, liveness '
            + `${await wait.liveness(n)}. That is the design/06 signature on a`);
        note('  consumer start, and the cure is a restart of that one worker.');
        act.timeline(`worker${n} WEDGED, restarting`);
        p.stop();
        // the replacement needs its own probe port back, and the process it
        // replaces has to release it first
         
        await wait.sleep(10000);
        p = procs.worker(act, configFile, n, opts);
         
        await procs.waitReady(p, 90000);
        say(`worker ${n} restarted as pid ${p.pid}`);
         
        await wait.until(`worker ${n}'s probe to answer`,
            async () => (await wait.liveness(n)) === 200, 45000, 2000);
    }
    return p;
}


/**
 * Warm a legacy consumer group before measuring anything through it.
 *
 * The legacy processor builds its consumer with no fromOffset, so
 * auto.offset.reset stays at librdkafka's `latest`: a group that has not
 * committed yet can skip what is already on the topic, and a first-join
 * revoke can move it past records published in between. A few operations
 * before the measured window, and a wait for committed offsets, removes that
 * from every measurement. It is the rig's own method note.
 *
 * @param {Object} act - the act
 * @param {Object} p - { dest, bucket, count, internalAtLeast }
 * @return {Promise} resolves with the group state
 */
/**
 * Restart one process in place, reusing its Proc so that every handle an act
 * is holding stays valid and its log file keeps its history. This is the
 * operator cure for the design/06 consumer wedge: restart that one consumer,
 * and only that one. Up to 45 seconds of it is the wedged member's group
 * session expiring.
 *
 * @param {String} name - the process name, e.g. 'processor-poc-dest-1'
 * @param {Number} [timeoutMs] - how long to wait for it to be ready again
 * @return {Promise} resolves with the process, or null if there is no such one
 */
async function restartInPlace(name, timeoutMs) {
    const p = procs.ALL.filter(x => x.name === name).pop();
    if (!p) {
        note(`no process called ${name} to restart`);
        return null;
    }
    p.stop();
    await wait.sleep(env.pause(6000));
    p.held = false;
    p.restarts += 1;
    p.spawnOnce();
    await procs.waitReady(p, timeoutMs || 60000);
    say(`${name} restarted as pid ${p.pid}, restart ${p.restarts}`);
    return p;
}

async function warmLegacyGroup(act, p) {
    const count = p.count || 8;
    say(`warming ${env.legacyGroup(p.dest)} with ${count} operations, so the`);
    note('group holds committed offsets before anything is measured');
    await runDriver(act, { bucket: p.bucket, prefix: `warm-${p.dest}`,
        rate: 4, count });
    await wait.frozen(env.INTERNAL_TOPIC, env.pause(10000),
        { atLeast: p.internalAtLeast || 1 });
    const group = env.legacyGroup(p.dest);
    const hasCommitted = () => {
        const g = kafka.groupState(group);
        return g.partitions > 0 && g.committed > 0 && g.unknown === 0;
    };
    let ok = await wait.until('the group to hold committed offsets',
        hasCommitted, 90000, 5000);
    if (!ok) {
        const g = kafka.groupState(group);
        note(`WEDGE SUSPECTED: ${group} holds ${g.partitions} partitions with`);
        note(`  ${g.unknown} of them showing NO committed offset while the`);
        note('  topic has records. That is the design/06 consumer defect, and');
        note('  it can fire well after the process passed its start-up check,');
        note('  so the cure is applied here too: restart that one consumer.');
        act.timeline(`processor ${p.dest} WEDGED during warm-up, restarting`);
        await restartInPlace(`processor-${p.dest}`, 60000);
        ok = await wait.until('the group to hold committed offsets after the '
            + 'restart', hasCommitted, 150000, 5000);
    }
    const st = kafka.groupState(group);
    if (!ok) {
        throw new Error(`${group} never committed an offset: `
            + `${st.unknown} of ${st.partitions} partitions uncommitted, lag `
            + `${st.lag}. The consumer wedge did not clear after one restart, `
            + 'so nothing measured past this point would be honest.');
    }
    say(`warmed: committed ${st.committed} over ${st.partitions} partitions, `
        + `lag ${st.lag}`);
    return st;
}

/**
 * Run the workload driver to completion.
 *
 * @param {Object} act - the act
 * @param {Object} opts - driver options
 * @return {Promise} resolves with the number of operations logged
 */
async function runDriver(act, opts) {
    const log = act.file(`driver${opts.prefix ? `-${opts.prefix}` : ''}.log`);
    const o = Object.assign({ log, endpoint: env.S3_ENDPOINT }, opts);
    say(`workload: ${o.count ? `${o.count} operations` : `${o.duration}s`} at `
        + `${o.rate}/s into ${o.buckets || o.bucket}`
        + `${o.straddle ? `, every ${o['straddle-every'] || 5}th a PUT then `
            + `DELETE on one of ${o.straddle} fixed keys` : ''}`);
    if (o.straddle) {
        note('those fixed keys are the only ones with several operations, so');
        note('they are the only keys that can show a per-key inversion');
    }
    const p = procs.driver(act, o);
    await wait.until('the driver to finish', () => !p.isRunning(),
        ((o.duration || 0) + 120) * 1000, 1000);
    const ops = fs.existsSync(log)
        ? fs.readFileSync(log, 'utf8').split('\n').filter(l => / ok$/.test(l)).length
        : 0;
    say(`driver done, ${ops} operations completed`);
    return { ops, log, proc: p };
}

/**
 * Start the driver and leave it running.
 *
 * @param {Object} act - the act
 * @param {Object} opts - driver options
 * @return {Object} { proc, log }
 */
function startDriver(act, opts) {
    const log = act.file(`driver${opts.prefix ? `-${opts.prefix}` : ''}.log`);
    const o = Object.assign({ log, endpoint: env.S3_ENDPOINT }, opts);
    say(`workload on for the whole procedure: ${o.rate}/s into `
        + `${o.buckets || o.bucket} for ${o.duration}s`);
    return { proc: procs.driver(act, o), log };
}

/**
 * Dump a customer topic slice and check it against the driver log.
 *
 * @param {Object} p - { act, topic, from, driver, keyPrefix, bucket, label,
 *   partition }
 * @return {Object} the check result
 */
function dumpAndCheck(p) {
    const events = p.act.file(`events-${p.label || 'default'}.jsonl`);
    const n = kafka.dump(p.topic, p.from, events, p.partition || 0, 40000);
    say(`dumped ${n} records of ${p.topic} from offset ${p.from}`);
    const result = check.check({
        events,
        driver: p.driver,
        keyPrefix: p.keyPrefix,
        bucket: p.bucket,
        label: p.label || p.topic,
        out: p.act.file(`checker-${p.label || 'default'}.json`),
    });
    say(check.summary(result));
    if (result.latency) {
        note(`latency seconds: ${JSON.stringify(result.latency)}`);
    }
    const t = result.totals;
    if (t.gaps) {
        note(`GAPS (loss): ${JSON.stringify(Object.entries(result.perKey)
            .filter(([, v]) => v.gaps.length)
            .reduce((a, [k, v]) => Object.assign(a, { [k]: v.gaps }), {}))
            .slice(0, 800)}`);
    }
    if (t.inversions) {
        note(`inversion pairs: ${JSON.stringify(Object.entries(result.perKey)
            .filter(([, v]) => v.inversions)
            .reduce((a, [k, v]) => Object.assign(a, { [k]: v.inversion_pairs }), {}))
            .slice(0, 800)}`);
    }
    return result;
}

/**
 * Record the standard rows of an act's verdict table.
 *
 * @param {Object} act - the act
 * @param {Object} result - a check result
 * @return {undefined}
 */
function recordChecker(act, result) {
    const t = result.totals;
    act.measured('expected events', t.expected);
    act.measured('delivered events', t.delivered);
    act.measured('gaps (loss)', t.gaps);
    act.measured('duplicate extras', t.duplicate_extras);
    act.measured('per-key inversions', t.inversions);
}

module.exports = {
    restartInPlace,
    warmLegacyGroup,
    legacyConfig,
    poolConfig,
    startPopulator,
    startProcessor,
    startWorker,
    runDriver,
    startDriver,
    dumpAndCheck,
    recordChecker,
};
