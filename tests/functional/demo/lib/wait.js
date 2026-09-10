'use strict';

/**
 * Waiting, done the way the rig had to learn to do it.
 *
 * The rule that matters: never gate a procedure on consumer lag alone. A
 * wedged consumer is a live group member holding all its partitions with a
 * lag that simply stops falling, a liveness probe that still answers 200,
 * and no delivery counters on /metrics at all. It fired on 5 of 21 consumer
 * starts during the migration round. So every gate here watches progress
 * (the committed offset, and where there is one the delivery counter) as
 * well as the lag, and calls a stall a stall.
 */

const http = require('http');
const env = require('./env');
const kafka = require('./kafka');
const { say, note } = require('./narrate');

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, Math.max(0, ms)));
}

/**
 * GET a small text body, resolving to null on any failure.
 *
 * @param {String} url - the url
 * @param {Number} [timeoutMs] - request timeout
 * @return {Promise} resolves with the body or null
 */
function get(url, timeoutMs) {
    return new Promise(resolve => {
        const req = http.get(url, { timeout: timeoutMs || 3000 }, res => {
            let body = '';
            res.on('data', c => { body += c; });
            res.on('end', () => resolve({ status: res.statusCode, body }));
        });
        req.on('timeout', () => { req.destroy(); resolve(null); });
        req.on('error', () => resolve(null));
    });
}

/**
 * A worker's prometheus metrics, parsed into { name, labels, value } rows.
 *
 * @param {Number} n - worker index
 * @return {Promise} resolves with the rows, empty if unreachable
 */
async function metrics(n) {
    const r = await get(`http://localhost:${env.probePort(n)}/metrics`);
    if (!r || r.status !== 200) {
        return [];
    }
    return r.body.split('\n').filter(l => l && !l.startsWith('#')).map(l => {
        const m = /^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[^}]*\})?\s+(-?[\d.eE+]+)$/.exec(l.trim());
        if (!m) {
            return null;
        }
        const labels = {};
        if (m[2]) {
            m[2].slice(1, -1).split(',').forEach(kv => {
                const p = /^([^=]+)="(.*)"$/.exec(kv.trim());
                if (p) {
                    labels[p[1]] = p[2];
                }
            });
        }
        return { name: m[1], labels, value: Number(m[3]) };
    }).filter(Boolean);
}

const COUNTER = {
    delivered: 's3_notification_delivery_worker_delivered_total',
    dropped: 's3_notification_delivery_worker_dropped_total',
    skipped: 's3_notification_delivery_worker_skipped_total',
    barrier: 's3_notification_delivery_worker_barrier_seen_total',
};

/**
 * Sum one worker counter, optionally filtered by label.
 *
 * @param {Number} n - worker index
 * @param {String} which - delivered, dropped, skipped or barrier
 * @param {Object} [filter] - label values to match
 * @return {Promise} resolves with the total
 */
async function counter(n, which, filter) {
    const rows = await metrics(n);
    return rows.filter(r => r.name === COUNTER[which])
        .filter(r => !filter || Object.keys(filter)
            .every(k => r.labels[k] === filter[k]))
        .reduce((a, r) => a + r.value, 0);
}

/**
 * Every value of one counter, keyed by a label, for printing.
 *
 * @param {Number} n - worker index
 * @param {String} which - counter
 * @param {String} label - label to group by
 * @return {Promise} resolves with { labelValue: total }
 */
async function counterBy(n, which, label) {
    const rows = await metrics(n);
    const out = {};
    rows.filter(r => r.name === COUNTER[which]).forEach(r => {
        const k = r.labels[label] === undefined ? '(none)' : r.labels[label];
        out[k] = (out[k] || 0) + r.value;
    });
    return out;
}

async function liveness(n) {
    const r = await get(`http://localhost:${env.probePort(n)}/_/live`);
    return r ? r.status : 0;
}

/**
 * Wait for a consumer group to reach lag 0 on its topic, gating on progress
 * as well as on the lag.
 *
 * A lag of 0 also means "nothing has been produced yet", which is a trap
 * right after a workload: the populator's batch cadence is several seconds,
 * so the topic can still be empty when the driver has finished. Pass
 * topicAtLeast to say how many records must be on the topic before a lag of
 * 0 is allowed to mean drained.
 *
 * @param {Object} p - group, label, timeoutMs, topic, workers (indices whose
 *   delivery counters count as progress), stallSeconds,
 *   topicAtLeast: { topic, count }
 * @return {Promise} resolves { drained, stalled, seconds, lag }
 */
async function drain(p) {
    const label = p.label || p.group;
    const timeout = p.timeoutMs || 300000;
    const stallLimit = (p.stallSeconds || 60) * 1000;
    const started = Date.now();
    const topic = p.topic || kafka.topicOfGroup(p.group);
    let lastProgress = Date.now();
    let prev = null;
    let lag = kafka.groupLag(p.group, p.topic);
    while (Date.now() - started < timeout) {
        const state = kafka.groupState(p.group, p.topic);
        lag = state.lag;
        let delivered = 0;
        if (p.workers && p.workers.length) {
             
            const each = await Promise.all(p.workers.map(n => counter(n, 'delivered')));
            delivered = each.reduce((a, b) => a + b, 0);
        }
        const now = `${state.committed}/${delivered}`;
        if (prev !== null && now !== prev) {
            lastProgress = Date.now();
        }
        prev = now;
        let produced = true;
        if (p.topicAtLeast) {
            const h = kafka.headTotal(p.topicAtLeast.topic);
            produced = h >= p.topicAtLeast.count;
            if (!produced) {
                say(`${label}: waiting for ${p.topicAtLeast.topic} to reach `
                    + `${p.topicAtLeast.count} records, now ${h}`);
                lastProgress = Date.now();
            }
        } else if (topic && kafka.headTotal(topic) === 0 && lag === 0) {
            // Nothing published and nothing expected: there is nothing for
            // anybody to drain, and calling that a stall would be wrong.
            say(`${label}: ${topic} is still empty, nothing to drain`);
            return { drained: true, stalled: false, empty: true,
                seconds: Math.round((Date.now() - started) / 1000), lag: 0 };
        }
        // A group with no rows at all is not drained: it has not been
        // assigned anything yet. Neither is one holding a partition that has
        // records and no committed offset, which is the wedge signature.
        if (produced && state.partitions > 0 && lag === 0
            && state.unknown === 0 && Date.now() - started > 3000) {
            const secs = Math.round((Date.now() - started) / 1000);
            say(`${label}: lag 0 after ${secs}s`);
            return { drained: true, stalled: false, seconds: secs, lag: 0 };
        }
        if (Date.now() - lastProgress > stallLimit) {
            const secs = Math.round((Date.now() - started) / 1000);
            note(`${label}: WEDGE SUSPECTED. lag ${lag} has not fallen and`);
            note('  nothing has progressed for '
                + `${Math.round(stallLimit / 1000)}s. Signature: partitions`);
            note('  held, liveness 200, no delivery counters moving.');
            note('  Cure: restart that one consumer. Up to 45s of it is the');
            note('  wedged member\'s group session expiring.');
            return { drained: false, stalled: true, seconds: secs, lag };
        }
        say(`${label}: lag ${lag} committed ${state.committed}`
            + `${p.workers ? ` delivered ${delivered}` : ''}`);
         
        await sleep(5000);
    }
    return { drained: false, stalled: false,
        seconds: Math.round((Date.now() - started) / 1000), lag };
}

/**
 * Wait for a predicate, polling.
 *
 * @param {String} what - what is being waited for, for the message
 * @param {Function} fn - predicate, may be async
 * @param {Number} timeoutMs - how long
 * @param {Number} [everyMs] - poll interval
 * @return {Promise} resolves true, or false on timeout
 */
async function until(what, fn, timeoutMs, everyMs) {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
         
        if (await fn()) {
            return true;
        }
         
        await sleep(everyMs || 1000);
    }
    note(`timed out waiting for ${what}`);
    return false;
}

/**
 * Wait for a topic to stop growing, which is how the suite knows the
 * populator has caught up or that a switch has taken effect.
 *
 * A topic that has never moved is NOT settled: right after a workload the
 * populator's batch cadence means the topic can still be empty, and calling
 * that "settled" is how a check ends up dumping nothing. So this waits for
 * the head to reach a minimum first.
 *
 * @param {String} topic - topic
 * @param {Number} [quietMs] - how long it must stay still
 * @param {Object} [opts] - { atLeast, timeoutMs }
 * @return {Promise} resolves with the frozen head total
 */
async function frozen(topic, quietMs, opts) {
    const o = opts || {};
    const atLeast = o.atLeast === undefined ? 1 : o.atLeast;
    const timeout = o.timeoutMs || 240000;
    const started = Date.now();
    while (kafka.headTotal(topic) < atLeast && Date.now() - started < timeout) {
        say(`${topic}: ${kafka.headTotal(topic)} records, waiting for `
            + `at least ${atLeast}`);
        await sleep(5000);
    }
    if (kafka.headTotal(topic) < atLeast) {
        note(`${topic} never reached ${atLeast} records; carrying on with `
            + `${kafka.headTotal(topic)}`);
    }
    let last = kafka.headTotal(topic);
    const quiet = quietMs || 10000;
    let since = Date.now();
    while (Date.now() - since < quiet) {
        await sleep(2000);
        const now = kafka.headTotal(topic);
        if (now !== last) {
            last = now;
            since = Date.now();
        }
    }
    return last;
}

module.exports = {
    sleep,
    get,
    metrics,
    counter,
    counterBy,
    liveness,
    drain,
    until,
    frozen,
    COUNTER,
};
