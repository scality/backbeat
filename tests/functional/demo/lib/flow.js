'use strict';

/**
 * The steps every act shares: render a config, start a path, drive a
 * workload, dump what arrived and check it.
 */

const fs = require('fs');
const path = require('path');
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
    if (env.SOURCE === 'delivery' && label !== 'legacy') {
        note(`it will publish addressed records to ${env.DELIVERY_TOPIC}, one `
            + 'per matching destination, keyed by destination');
    } else {
        note(`it will publish to ${env.INTERNAL_TOPIC}, one record per event, `
            + 'exactly as today; the populator does not know which path '
            + 'consumes it');
    }
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
    // A group that has run in production for years has a committed offset on
    // every partition. One this suite just deleted has none, and the
    // processor's consumer then starts at `latest` and skips whatever lands
    // during its first-join rebalance. Put the group in the production state
    // before the process starts, and say so.
    const group = env.legacyGroup(dest);
    const seeded = kafka.seedGroupAtHead(group, env.INTERNAL_TOPIC);
    if (seeded.length) {
        note(`seeded ${group} at the head of ${env.INTERNAL_TOPIC} on `
            + `partition${seeded.length > 1 ? 's' : ''} ${seeded.join(', ')},`);
        note('  which had no committed offset. A production group has one');
        note('  everywhere; a brand-new one starts at latest and would skip');
        note('  records published during its first-join rebalance.');
        act.timeline(`processor ${dest} group seeded on ${seeded.join(',')}`);
    }
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
         
        await wait.sleep(env.pause(20000));
        const bal = p.rebalances();
         
        const delivered = await wait.counter(n, 'delivered');
         
        const skipped = await wait.counter(n, 'skipped');
        // A healthy consumer holds its assignment, idle or not: the
        // "processing queue idle, un-assigning" line only appears on a revoke
        // callback, and revokes do not come once a second on their own. So
        // three or more revokes in this window with nothing delivered or
        // skipped is the wedge, whether or not records have arrived yet. In
        // the timed reference run every worker that showed this at start went
        // on to stall its first drain, and letting the drain gate notice it
        // cost about a minute each time.
        const churning = bal.revoke - first >= 3
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
    // stop() clears autoRestart so that a deliberate stop is not undone by
    // the supervisor. A restart is not a stop: the replacement must keep
    // the supervision the original had, or the next crash or kill leaves
    // the pool with no consumer and nothing to bring one back.
    const supervised = p.autoRestart;
    const assignsBefore = p.rebalances().assign;
    p.stop();
    await wait.sleep(env.pause(6000));
    p.held = false;
    p.autoRestart = supervised;
    p.restarts += 1;
    p.spawnOnce();
    await procs.waitReady(p, timeoutMs || 60000);
    say(`${name} restarted as pid ${p.pid}, restart ${p.restarts}`);
    // A consumer that has just started holds nothing for a while: the dead
    // member's session has to expire and the first join is revoked and
    // re-assigned about forty seconds later. Nothing can progress before
    // that, so a stall clock started here would fire on a healthy
    // consumer. Wait for the first assignment before judging anything.
    const joined = await wait.until(`${name}'s first assignment after the restart`,
        () => p.rebalances().assign > assignsBefore, 150000, 2000);
    if (joined) {
        say(`${name} holds an assignment again, `
            + `${Math.round((Date.now() - p.startedAt) / 1000)}s after it started`);
    }
    return p;
}

/**
 * Wait for a worker to make progress, and cure the wedge once if it does not.
 *
 * cureChurn runs when a worker starts, before an act's workload exists, so a
 * worker that wedges on the first records it sees is never caught by it. This
 * is the same test applied where the symptom shows: if the condition never
 * comes true, and the delivery topic holds records while the worker's own
 * counters are all zero, that is the design/06 wedge rather than an idle
 * consumer, and the cure is a restart of that one worker.
 *
 * @param {Object} act - the act
 * @param {Object} proc - the worker's process
 * @param {String} what - what is being waited for, for the narration
 * @param {Function} condition - async predicate, true when progress happened
 * @param {Object} [opts] - { timeoutMs, retryTimeoutMs, everyMs, index }
 * @return {Promise} resolves true if progress happened, first or second try
 */
async function progressOrCure(act, proc, what, condition, opts) {
    const o = opts || {};
    const n = o.index || Number((/worker(\d+)/.exec(proc.name) || [])[1]) || 1;
    const every = o.everyMs || 10000;
    let timeout = o.timeoutMs || 90000;
    // Two attempts, the same as cureChurn: on this host one restart is often
    // not enough, and a worker that comes back wedged looks identical to one
    // that never cleared.
    for (let attempt = 0; attempt < 3; attempt++) {
        // eslint-disable-next-line no-await-in-loop
        if (await wait.until(attempt ? `${what}, after restart ${attempt}`
            : what, condition, timeout, every)) {
            if (attempt) {
                say(`${what}: progress after restart ${attempt}. That is the `
                    + 'operator cure working, and it is the reliability '
                    + 'ceiling this defect sets.');
            }
            return true;
        }
        if (attempt === 2) {
            break;
        }
        const onTopic = kafka.headTotal(env.POOL_TOPIC);
        // eslint-disable-next-line no-await-in-loop
        const moved = (await wait.counter(n, 'delivered'))
            // eslint-disable-next-line no-await-in-loop
            + (await wait.counter(n, 'dropped'))
            // eslint-disable-next-line no-await-in-loop
            + (await wait.counter(n, 'skipped'));
        const bal = proc.rebalances();
        if (onTopic === 0 || moved > 0) {
            note(`no progress on ${what}, and this is NOT the wedge: `
                + `${onTopic} records on the pool's topic and ${moved} of `
                + 'them accounted for by this worker. Something else is '
                + 'wrong, so nothing is being restarted.');
            return false;
        }
        note(`WEDGE SUSPECTED: ${onTopic} records on the pool's topic, `
            + `${bal.assign} assigns and ${bal.revoke} revokes on worker `
            + `${n}, and`);
        // eslint-disable-next-line no-await-in-loop
        note('  nothing delivered, dropped or skipped. Liveness is still '
            + `${await wait.liveness(n)}, which is what makes this defect `
            + 'hard to');
        note('  see in production. The cure is a restart of that one worker.');
        act.timeline(`worker${n} WEDGED during ${what}, restart ${attempt + 1}`);
        // eslint-disable-next-line no-await-in-loop
        await restartInPlace(proc.name, 90000);
        // eslint-disable-next-line no-await-in-loop
        await wait.until(`worker ${n}'s probe to answer`,
            async () => (await wait.liveness(n)) === 200, 60000, 2000);
        timeout = o.retryTimeoutMs || 150000;
    }
    return false;
}

/**
 * The consumers a drain is waiting on, worked out from what the caller
 * already passes. Worker indices name pool workers; a legacy group name
 * carries its destination, and the processor for it is named after that.
 *
 * @param {Object} p - the drain parameters
 * @return {Array} the processes, most recent first spawn last
 */
function consumersOf(p) {
    const out = [];
    (p.workers || []).forEach(n => {
        const m = procs.ALL.filter(x => x.name === `worker${n}`
            || x.name.startsWith(`worker${n}-`)).pop();
        if (m) {
            out.push(m);
        }
    });
    const pre = `${env.LEGACY_GROUP_PREFIX}-`;
    if (!out.length && p.group && p.group.startsWith(pre)) {
        const m = procs.ALL.filter(
            x => x.name === `processor-${p.group.slice(pre.length)}`).pop();
        if (m) {
            out.push(m);
        }
    }
    return out;
}

/**
 * Drain a consumer group, and if it stalls on the wedge, apply the cure the
 * stall message names: restart exactly the consumers that stalled, and
 * nothing else, then drain again.
 *
 * wait.drain recognises the wedge but only reports it, so without this the
 * suite prints the cure and never applies it, and every act that asserts on
 * a completed drain fails on a defect it already diagnosed correctly.
 *
 * @param {Object} p - as wait.drain takes it, plus `cure` to name the
 *   processes explicitly and `retryTimeoutMs` for the second drain
 * @return {Promise} resolves with the drain result
 */
async function drainOrCure(p) {
    const first = await wait.drain(p);
    if (first.drained || !first.stalled) {
        return first;
    }
    const targets = p.cure || consumersOf(p);
    if (!targets.length) {
        note('there is no consumer of this drain that the suite started, so');
        note('  there is nothing it can restart. The stall stands as measured.');
        return first;
    }
    note(`applying that cure now to ${targets.map(t => t.name).join(', ')}.`);
    for (const t of targets) {
         
        await restartInPlace(t.name, 90000);
        const n = Number((/worker(\d+)/.exec(t.name) || [])[1]);
        if (n) {
             
            await wait.until(`worker ${n}'s probe to answer`,
                async () => (await wait.liveness(n)) === 200, 60000, 2000);
        }
    }
    // The restarted consumer has an assignment now, but its first records
    // still take a while to flow, so the second drain gets a longer stall
    // limit than the first: sixty seconds fired on a healthy consumer that
    // had joined a second earlier.
    const again = await wait.drain(Object.assign({}, p, {
        timeoutMs: p.retryTimeoutMs || p.timeoutMs || 300000,
        stallSeconds: Math.max(p.stallSeconds || 60, 120),
    }));
    if (again.drained) {
        say(`${p.label || p.group}: drained after the restart, in `
            + `${again.seconds}s. That is the operator cure working, and it is `
            + 'the reliability ceiling this defect sets.');
    } else {
        note(`${p.label || p.group}: still not drained after the restart.`);
    }
    again.cured = true;
    again.restarts = targets.map(t => t.name);
    return again;
}

/**
 * Watch for the evidence that a generation seeded itself.
 *
 * A worker whose consumer group has no committed offsets takes a zookeeper
 * lock, seeds every group of the document from the groups it inherits from,
 * and writes the per-destination watermarks last, after the groups are
 * committed and read back. So the watermarks node appearing is the proof
 * that the whole seeding ran, and it is durable: the group's own offsets
 * move as soon as the worker starts committing, the watermarks do not.
 *
 * Call it without awaiting, start the workers, then await it.
 *
 * @param {Object} act - the act
 * @param {Object} p - { generation, budgetMs }
 * @return {Promise} resolves with { watermarks, waitedS, start, seeders },
 *   where start is the lowest watermark per partition, which is where the
 *   seeding put the group of a workgroup owning every destination, and
 *   seeders names the workers whose log says they did it
 */
async function captureSelfSeed(act, p) {
    const zk = require('./zk');
    const started = Date.now();
    let marks = null;
    await wait.until(`generation ${p.generation} to seed itself`, () => {
        marks = zk.watermarks(p.generation);
        return marks !== null;
    }, p.budgetMs || 240000, 2000);
    const waitedS = Math.round((Date.now() - started) / 1000);
    // the watermarks are written inside the seeding, the log line just
    // after it returns, so give the line a moment to land
    if (marks) {
        await wait.until('the seeding worker to say so in its log',
            () => selfSeedersIn(act, p.generation).length > 0, 60000, 2000);
    }
    const seeders = selfSeedersIn(act, p.generation);
    const start = {};
    Object.values(marks || {}).forEach(byPartition => {
        Object.entries(byPartition).forEach(([partition, offset]) => {
            if (start[partition] === undefined || offset < start[partition]) {
                start[partition] = offset;
            }
        });
    });
    if (marks) {
        act.timeline(`generation ${p.generation} SELF-SEEDED after ${waitedS}s `
            + `by ${seeders.join(',') || 'a worker that did not say so'}`);
    }
    return { watermarks: marks, waitedS, start, seeders };
}

/**
 * Which of a generation's workers said in its log that it did the seeding.
 * Exactly one of them takes the lock, so exactly one says it.
 *
 * Read from the log files rather than from the processes, so it can be
 * called while a start is still in flight, and matched on the generation
 * the line carries, so an earlier generation's seeding is not counted.
 *
 * @param {Object} act - the act
 * @param {Number} generation - generation number
 * @return {Array} the worker log files that carry the line
 */
function selfSeedersIn(act, generation) {
    const seen = [];
    fs.readdirSync(act.dir)
        .filter(f => /^worker.*\.log$/.test(f))
        .forEach(f => {
            const text = fs.readFileSync(path.join(act.dir, f), 'utf8');
            const said = text.split('\n').some(l => l.includes('seeded itself')
                && l.includes(`"generation":${generation}`));
            if (said) {
                seen.push(f.replace(/\.log$/, ''));
            }
        });
    return seen;
}

const SEED_TOOL = 'bin/notificationDeliverySeed.js';

/**
 * Run the seed tool and show its table.
 *
 * @param {Object} act - the act
 * @param {String} configFile - the config the workers will run with
 * @param {Array} args - the tool's arguments
 * @return {Object} { code, out }
 */
function runSeedTool(act, configFile, args) {
    const r = procs.runTool({ act, name: `seed-${args[0]}`,
        argv: [SEED_TOOL].concat(args), configFile, timeoutMs: 180000 });
    const { line } = require('./narrate');
    line(r.out.split('\n').filter(Boolean).map(l => `      | ${l}`).join('\n'));
    if (r.code !== 0) {
        note(`the seed tool exited ${r.code}`);
    }
    return r;
}

/**
 * Seed a generation's worker groups from the legacy processors they replace:
 * per partition the lowest committed offset across the processor groups of
 * the destinations each workgroup owns, plus a per-destination watermark at
 * each destination's own processor offset, so nothing is delivered twice and
 * a stalled destination gets its whole backlog. This is the one step an
 * Ansible run has to do between deleting the processor containers and
 * starting the worker containers.
 *
 * @param {Object} act - the act
 * @param {String} configFile - the config the workers will run with
 * @param {Number} generation - the generation being started
 * @return {Object} { code, out }
 */
function seedFromProcessors(act, configFile, generation) {
    say(`seeding generation ${generation}'s groups from the processors' `
        + 'committed offsets');
    note('the lowest offset per partition, so nothing is skipped, and a');
    note('watermark per destination, so nothing already delivered is sent');
    note('again. A group with no committed offset would otherwise start at');
    note('the oldest retained record and replay hours to every customer.');
    const r = runSeedTool(act, configFile,
        ['seed-from-processors', '--generation', String(generation)]);
    act.timeline(`seed-from-processors generation ${generation} exit ${r.code}`);
    return r;
}

/**
 * Seed a generation's worker groups from the previous generation's groups,
 * the same way, for a layout change.
 *
 * @param {Object} act - the act
 * @param {String} configFile - the config the workers will run with
 * @param {Number} from - the generation being stopped
 * @param {Number} to - the generation being started
 * @return {Object} { code, out }
 */
function seedFromGeneration(act, configFile, from, to) {
    say(`seeding generation ${to}'s groups from generation ${from}'s `
        + 'committed offsets');
    const r = runSeedTool(act, configFile,
        ['seed-from-generation', '--from', String(from), '--to', String(to)]);
    act.timeline(`seed-from-generation ${from} to ${to} exit ${r.code}`);
    return r;
}

/**
 * Give the plain pool group (no workgroups) a committed offset at the head
 * of the pool's topic before its first worker starts, the way an Ansible run
 * would seed it. A fresh group with no committed offset starts at the oldest
 * retained record, and on today's topic that is every event of every act so
 * far.
 *
 * @param {Object} act - the act
 * @param {String} [group] - the group, default the base pool group
 * @return {Array} the partitions that were seeded
 */
function seedPoolGroupAtHead(act, group) {
    const g = group || env.DELIVERY_GROUP;
    const seeded = kafka.seedGroupAtHead(g, env.POOL_TOPIC);
    if (seeded.length) {
        note(`seeded ${g} at the head of ${env.POOL_TOPIC} on partition`
            + `${seeded.length > 1 ? 's' : ''} ${seeded.join(', ')}: a group`);
        note('  with no committed offset would start at the oldest retained');
        note('  record, which on today\'s topic is every event so far.');
        act.timeline(`pool group ${g} seeded at head on ${seeded.join(',')}`);
    }
    return seeded;
}

/**
 * Save a worker's counters into the act's evidence, and return the skipped
 * and dropped totals by reason, so a record that was consumed and not
 * delivered can be told from one that was never consumed.
 *
 * @param {Object} act - the act
 * @param {Number} n - worker index
 * @param {String} label - file label
 * @return {Promise} resolves with { delivered, skipped: {reason: n},
 *   dropped: {reason: n}, watermark }
 */
async function snapshotMetrics(act, n, label) {
    const rows = await wait.metrics(n);
    fs.writeFileSync(act.file(`metrics-${label || `worker${n}`}.txt`),
        rows.map(r => `${r.name}${JSON.stringify(r.labels)} ${r.value}`)
            .join('\n').concat('\n'));
    const out = { delivered: 0, skipped: {}, dropped: {}, watermark: 0 };
    rows.forEach(r => {
        if (r.name === wait.COUNTER.delivered) {
            out.delivered += r.value;
        } else if (r.name === wait.COUNTER.skipped) {
            const k = r.labels.reason || '(none)';
            out.skipped[k] = (out.skipped[k] || 0) + r.value;
        } else if (r.name === wait.COUNTER.dropped) {
            const k = r.labels.reason || '(none)';
            out.dropped[k] = (out.dropped[k] || 0) + r.value;
        } else if (r.name === wait.COUNTER.watermark) {
            out.watermark += r.value;
        }
    });
    say(`worker ${n} counters: delivered ${out.delivered}, skipped `
        + `${JSON.stringify(out.skipped)}, dropped ${JSON.stringify(out.dropped)}, `
        + `watermark ${out.watermark}`);
    return out;
}

/**
 * Start an act with no pool groups and no workgroups document left over by
 * an earlier act, so its own generation 1 starts where the act seeds it and
 * not where a previous act's group of the same name had got to.
 *
 * @param {Object} act - the act
 * @return {Array} the groups deleted
 */
function resetPoolGroups(act) {
    const zk = require('./zk');
    const mine = kafka.groups().filter(g => g === env.DELIVERY_GROUP
        || g.startsWith(`${env.DELIVERY_GROUP}-`));
    const held = kafka.waitForNoMembers(60000);
    if (held.length) {
        note(`groups still holding members before the reset: ${held.join(', ')}`);
    }
    const deleted = [];
    mine.forEach(g => {
        if (kafka.deleteGroup(g).ok) {
            deleted.push(g);
        }
    });
    if (zk.workgroupsDoc()) {
        zk.deleteAll(env.ZK_WORKGROUPS_PATH);
    }
    if (deleted.length) {
        note(`pool groups from earlier acts deleted: ${deleted.join(', ')}`);
        act.timeline(`pool groups reset: ${deleted.join(',')}`);
    }
    return deleted;
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
    // Two cure attempts, the same as cureChurn and progressOrCure. On this
    // host one restart is often not enough, and a consumer that comes back
    // wedged looks identical to one that never cleared.
    let ok = false;
    let timeout = 90000;
    for (let attempt = 0; attempt < 3; attempt++) {
        // eslint-disable-next-line no-await-in-loop
        ok = await wait.until(attempt
            ? `the group to hold committed offsets, after restart ${attempt}`
            : 'the group to hold committed offsets',
        hasCommitted, timeout, 5000);
        if (ok || attempt === 2) {
            break;
        }
        const g = kafka.groupState(group);
        note(`WEDGE SUSPECTED: ${group} holds ${g.partitions} partitions with`);
        note(`  ${g.unknown} of them showing NO committed offset while the`);
        note('  topic has records. That is the design/06 consumer defect, and');
        note('  it can fire well after the process passed its start-up check,');
        note('  so the cure is applied here too: restart that one consumer.');
        act.timeline(`processor ${p.dest} WEDGED during warm-up, `
            + `restart ${attempt + 1}`);
        // eslint-disable-next-line no-await-in-loop
        await restartInPlace(`processor-${p.dest}`, 60000);
        timeout = 150000;
    }
    const st = kafka.groupState(group);
    if (!ok) {
        throw new Error(`${group} never committed an offset: `
            + `${st.unknown} of ${st.partitions} partitions uncommitted, lag `
            + `${st.lag}. The consumer wedge did not clear after two `
            + 'restarts, so nothing measured past this point would be '
            + 'honest.');
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
    const log = act.file(`driver-${procs.fileTag(opts.prefix)}.log`);
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
    const log = act.file(`driver-${procs.fileTag(opts.prefix)}.log`);
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
    SEED_TOOL,
    runSeedTool,
    captureSelfSeed,
    selfSeedersIn,
    seedFromProcessors,
    seedFromGeneration,
    seedPoolGroupAtHead,
    resetPoolGroups,
    snapshotMetrics,
    restartInPlace,
    drainOrCure,
    progressOrCure,
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
