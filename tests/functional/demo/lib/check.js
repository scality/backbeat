'use strict';

/* eslint-disable camelcase -- the checker's report keys are
 * snake_case because that is how they appear in the evidence JSON
 * and in RESULTS.md, and renaming them would break both. */

/**
 * The delivery checker, in JS, so that `yarn` is the only entry point the
 * demo needs. Same contract as the rig's scripts/check.py, and verified
 * against its output on the rig's own evidence: an event is identified by
 * (object key, Put or Delete, size), the workload driver carries a monotonic
 * sequence in the object size, and the report is per key:
 *
 *   expected    the ordered operations the driver completed
 *   delivered   the ordered operations that arrived, in kafka offset order
 *   gaps        expected operations that never arrived, which is LOSS
 *   duplicates  extra copies of an operation
 *   inversions  pairs of operations on one key delivered out of driver order
 *   unexpected  delivered operations no driver log accounts for
 */

const fs = require('fs');
const path = require('path');

const OP_OF_EVENT = {
    's3:ObjectCreated:Put': 'Put',
    's3:ObjectCreated:Post': 'Put',
    's3:ObjectCreated:Copy': 'Put',
    's3:ObjectCreated:CompleteMultipartUpload': 'Put',
    's3:ObjectRemoved:Delete': 'Delete',
    's3:ObjectRemoved:DeleteMarkerCreated': 'Delete',
};
const OP_OF_DRIVER = { PUT: 'Put', DELETE: 'Delete' };

/**
 * Read a kafka-console-consumer dump with print.key, print.timestamp,
 * print.partition and print.offset.
 *
 * @param {String} file - dump path
 * @return {Array} one entry per delivered record, in file order
 */
function parseEvents(file) {
    if (!fs.existsSync(file)) {
        return [];
    }
    const recs = [];
    fs.readFileSync(file, 'utf8').split('\n').forEach((raw, i) => {
        const lineNo = i + 1;
        if (!raw.trim()) {
            return;
        }
        const meta = { partition: null, offset: null, ts: null, lineNo };
        const body = [];
        raw.split('\t').forEach(part => {
            let m = /^(?:CreateTime|LogAppendTime):(-?\d+)$/.exec(part);
            if (m) {
                meta.ts = Number(m[1]);
                return;
            }
            m = /^Partition:(\d+)$/.exec(part);
            if (m) {
                meta.partition = Number(m[1]);
                return;
            }
            m = /^Offset:(\d+)$/.exec(part);
            if (m) {
                meta.offset = Number(m[1]);
                return;
            }
            body.push(part);
        });
        if (!body.length) {
            return;
        }
        const value = body[body.length - 1];
        const kafkaKey = body.length >= 2 ? body[body.length - 2] : null;
        let doc = null;
        try {
            doc = JSON.parse(value);
        } catch {
            recs.push(Object.assign({}, meta, { kafkaKey, parseError: true,
                raw: value.slice(0, 200) }));
            return;
        }
        const records = (doc && doc.Records) || [];
        if (!records.length) {
            recs.push(Object.assign({}, meta, { kafkaKey, noRecords: true }));
            return;
        }
        records.forEach(r => {
            const s3 = r.s3 || {};
            const obj = s3.object || {};
            const size = Number(obj.size);
            recs.push(Object.assign({}, meta, {
                kafkaKey,
                bucket: (s3.bucket || {}).name,
                objkey: obj.key,
                event: r.eventName,
                op: OP_OF_EVENT[r.eventName],
                size: Number.isFinite(size) ? size : null,
                configurationId: s3.configurationId,
                eventTime: r.eventTime,
            }));
        });
    });
    return recs;
}

/**
 * Read one or more workload driver logs.
 *
 * @param {Array|String} files - driver log paths
 * @return {Array} completed operations, in completion order
 */
function parseDriver(files) {
    const list = Array.isArray(files) ? files : [files];
    const ops = [];
    list.forEach(file => {
        if (!fs.existsSync(file)) {
            return;
        }
        fs.readFileSync(file, 'utf8').split('\n').forEach(raw => {
            const f = raw.trim().split(/\s+/);
            if (f.length < 6 || !OP_OF_DRIVER[f[1]]) {
                return;
            }
            ops.push({ ts: f[0], op: OP_OF_DRIVER[f[1]], bucket: f[2],
                key: f[3], size: Number(f[4]), rc: f[5] });
        });
    });
    return ops;
}

function opid(op, size) {
    return `${op}:${size}`;
}

function latency(delivered, filter) {
    const vals = [];
    delivered.forEach(r => {
        if (!r.eventTime || !r.ts) {
            return;
        }
        if (filter.keyPrefix && !String(r.objkey || '').startsWith(filter.keyPrefix)) {
            return;
        }
        if (filter.bucket && r.bucket !== filter.bucket) {
            return;
        }
        const t = Date.parse(r.eventTime);
        if (Number.isNaN(t)) {
            return;
        }
        vals.push((r.ts - t) / 1000);
    });
    if (!vals.length) {
        return null;
    }
    vals.sort((a, b) => a - b);
    const q = f => Math.round(vals[Math.min(vals.length - 1,
        Math.floor(f * vals.length))] * 10) / 10;
    return {
        count: vals.length,
        min: Math.round(vals[0] * 10) / 10,
        p50: q(0.5),
        p95: q(0.95),
        p99: q(0.99),
        max: Math.round(vals[vals.length - 1] * 10) / 10,
        over_10s: vals.filter(v => v > 10).length,
        over_30s: vals.filter(v => v > 30).length,
    };
}

/**
 * Compare what the driver did with what arrived.
 *
 * @param {Array} driverOps - from parseDriver
 * @param {Array} delivered - from parseEvents
 * @param {Object} [filter] - { keyPrefix, bucket }
 * @return {Object} { totals, perKey, latency }
 */
function analyse(driverOps, delivered, filter) {
    const f = filter || {};
    const expected = new Map();
    driverOps.forEach((o, i) => {
        if (!String(o.rc).startsWith('ok')) {
            return;
        }
        if (f.keyPrefix && !o.key.startsWith(f.keyPrefix)) {
            return;
        }
        if (f.bucket && o.bucket !== f.bucket) {
            return;
        }
        if (!expected.has(o.key)) {
            expected.set(o.key, []);
        }
        expected.get(o.key).push({ id: opid(o.op, o.size), order: i, ts: o.ts });
    });

    const got = new Map();
    delivered.forEach((r, i) => {
        if (r.parseError || r.noRecords || r.objkey === undefined
            || r.objkey === null) {
            return;
        }
        if (f.keyPrefix && !String(r.objkey).startsWith(f.keyPrefix)) {
            return;
        }
        if (f.bucket && r.bucket !== f.bucket) {
            return;
        }
        if (!got.has(r.objkey)) {
            got.set(r.objkey, []);
        }
        got.get(r.objkey).push({ id: opid(r.op, r.size), seen: i,
            partition: r.partition, offset: r.offset, event: r.event,
            configurationId: r.configurationId });
    });

    const totals = { keys: 0, expected: 0, delivered: 0, unique_delivered: 0,
        gaps: 0, duplicate_extras: 0, inversions: 0, unexpected: 0,
        keys_with_gaps: 0, keys_with_dups: 0, keys_with_inversions: 0 };
    const perKey = {};
    const keys = Array.from(new Set([...expected.keys(), ...got.keys()])).sort();
    keys.forEach(k => {
        const exp = expected.get(k) || [];
        const dlv = got.get(k) || [];
        const expIds = exp.map(e => e.id);
        const dlvIds = dlv.map(d => d.id);
        const counts = new Map();
        dlvIds.forEach(id => counts.set(id, (counts.get(id) || 0) + 1));
        const gaps = expIds.filter(id => !counts.has(id));
        const dups = {};
        let dupExtras = 0;
        counts.forEach((c, id) => {
            if (c > 1) {
                dups[id] = c;
                dupExtras += c - 1;
            }
        });
        const expSet = new Set(expIds);
        const unexpected = dlvIds.filter(id => !expSet.has(id));
        const first = new Map();
        dlv.forEach(d => {
            if (!first.has(d.id)) {
                first.set(d.id, d.seen);
            }
        });
        const present = expIds.filter(id => first.has(id));
        let inv = 0;
        const invPairs = [];
        for (let a = 0; a < present.length; a++) {
            for (let b = a + 1; b < present.length; b++) {
                if (first.get(present[a]) > first.get(present[b])) {
                    inv += 1;
                    if (invPairs.length < 5) {
                        invPairs.push([present[a], present[b]]);
                    }
                }
            }
        }
        perKey[k] = { expected: expIds, delivered: dlvIds,
            delivered_offsets: dlv.map(d => d.offset), gaps, duplicates: dups,
            duplicate_extras: dupExtras, unexpected, inversions: inv,
            inversion_pairs: invPairs };
        totals.keys += 1;
        totals.expected += expIds.length;
        totals.delivered += dlvIds.length;
        totals.unique_delivered += counts.size;
        totals.gaps += gaps.length;
        totals.duplicate_extras += dupExtras;
        totals.inversions += inv;
        totals.unexpected += unexpected.length;
        totals.keys_with_gaps += gaps.length ? 1 : 0;
        totals.keys_with_dups += dupExtras ? 1 : 0;
        totals.keys_with_inversions += inv ? 1 : 0;
    });

    return { totals, perKey, latency: latency(delivered, f) };
}

/**
 * The whole job in one call: read the files, compare, write the report.
 *
 * @param {Object} params - { events, driver, keyPrefix, bucket, out, label }
 * @return {Object} { totals, perKey, latency, byEvent }
 */
/**
 * Decompose the duplicate deliveries in a set of customer-topic dumps by the
 * moment they happened. The first copy of an operation is the delivery; every
 * later copy is a duplicate, and the question is who made it: a replacement
 * after a kill (copy 1 before the kill, copy 2 after), the next generation
 * after a swap (copy 1 before the stop, copy 2 after), or a restart within
 * the same generation (both copies between the same two boundaries).
 *
 * @param {Array} files - events files
 * @param {Array} boundaries - [{ label, at }] in time order, `at` in ms epoch;
 *   labels name what happened at that instant (a kill, a generation stop)
 * @return {Object} { total, across: { label: n }, within: { segment: n },
 *   perFile: { file: { total, across, within } } }
 */
function decomposeDuplicates(files, boundaries) {
    const sorted = boundaries.slice().sort((a, b) => a.at - b.at);
    const segment = t => {
        let i = 0;
        while (i < sorted.length && t >= sorted[i].at) {
            i += 1;
        }
        return i;
    };
    const out = { total: 0, across: {}, within: {}, perFile: {} };
    files.forEach(file => {
        const byOp = new Map();
        parseEvents(file).filter(r => r.objkey && r.op).forEach(r => {
            const id = `${r.objkey}|${opid(r.op, r.size)}`;
            if (!byOp.has(id)) {
                byOp.set(id, []);
            }
            byOp.get(id).push(r.ts);
        });
        const mine = { total: 0, across: {}, within: {} };
        byOp.forEach(times => {
            times.sort((a, b) => a - b);
            const first = segment(times[0]);
            times.slice(1).forEach(t => {
                const seg = segment(t);
                mine.total += 1;
                if (seg === first) {
                    const k = seg === 0 ? 'before any boundary'
                        : `after ${sorted[seg - 1].label}`;
                    mine.within[k] = (mine.within[k] || 0) + 1;
                } else {
                    const k = sorted[seg - 1].label;
                    mine.across[k] = (mine.across[k] || 0) + 1;
                }
            });
        });
        out.perFile[path.basename(file)] = mine;
        out.total += mine.total;
        Object.entries(mine.across).forEach(([k, n]) => {
            out.across[k] = (out.across[k] || 0) + n;
        });
        Object.entries(mine.within).forEach(([k, n]) => {
            out.within[k] = (out.within[k] || 0) + n;
        });
    });
    return out;
}

function check(params) {
    const delivered = parseEvents(params.events);
    const driverOps = parseDriver(params.driver);
    const result = analyse(driverOps, delivered,
        { keyPrefix: params.keyPrefix, bucket: params.bucket });
    const byEvent = {};
    delivered.forEach(r => {
        const name = r.event || (r.parseError ? 'UNPARSED' : 'NO_RECORDS');
        byEvent[name] = (byEvent[name] || 0) + 1;
    });
    result.byEvent = byEvent;
    result.records_in_file = delivered.length;
    result.driver_ops = driverOps.length;
    result.label = params.label || 'default';
    result.files = { events: params.events, driver: params.driver };
    if (params.out) {
        fs.writeFileSync(params.out, `${JSON.stringify(result, null, 1)}\n`);
    }
    return result;
}

/**
 * One line, the way the rig's checker prints it.
 *
 * @param {Object} r - a check() result
 * @return {String} the summary line
 */
function summary(r) {
    const t = r.totals;
    return `${String(r.label).padEnd(14)} keys=${t.keys} expected=${t.expected} `
        + `delivered=${t.delivered} unique=${t.unique_delivered} `
        + `gaps=${t.gaps} dup_extras=${t.duplicate_extras} `
        + `inversions=${t.inversions} unexpected=${t.unexpected}`;
}

module.exports = {
    decomposeDuplicates, parseEvents, parseDriver, analyse, check, summary };
