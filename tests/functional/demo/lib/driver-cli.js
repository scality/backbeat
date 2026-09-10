#!/usr/bin/env node
'use strict';

/**
 * The workload driver, as its own process.
 *
 * It carries a monotonic sequence number in the object SIZE (size = 1000 +
 * seq) and appends every completed operation to a log in completion order,
 * which is also submission order because it keeps one operation in flight.
 * That is what lets the checker rebuild the expected per-key order from the
 * delivered events alone.
 *
 * Same contract and same log format as the rig's scripts/driver.js, so
 * evidence from either is readable by either checker.
 *
 *   --bucket <b> | --buckets b1,b2,b3   where to write
 *   --prefix <p>                        object key prefix
 *   --rate <n>                          operations per second
 *   --duration <s> | --count <n>        how long, or how many
 *   --straddle <N> --straddle-every <K>
 *       every Kth operation is a PUT then DELETE on one of N fixed keys, so
 *       those keys have operations on both sides of any boundary in a
 *       procedure. They are the only keys that can show a per-key inversion.
 *   --seq-start <n>                     continue a sequence
 *   --log <file>                        the driver log
 *   --endpoint <url>                    defaults to the demo CloudServer
 */

const fs = require('fs');
const s3lib = require('./s3');
const env = require('./env');

function arg(name, def) {
    const i = process.argv.indexOf(`--${name}`);
    if (i === -1) {
        return def;
    }
    const v = process.argv[i + 1];
    if (v === undefined || v.startsWith('--')) {
        return true;
    }
    return v;
}

const bucket = arg('bucket', 'demo-bucket');
const bucketsArg = arg('buckets', '');
const buckets = (bucketsArg === '' || bucketsArg === true)
    ? [bucket] : String(bucketsArg).split(',').filter(Boolean);
const prefix = arg('prefix', 'x');
const rate = Number(arg('rate', 2));
const duration = Number(arg('duration', 0));
const count = Number(arg('count', 0));
const logPath = arg('log', '/dev/stdout');
const seqStart = Number(arg('seq-start', 0));
const straddle = Number(arg('straddle', 0));
const straddleEvery = Number(arg('straddle-every', 5));
const noDelete = arg('no-delete', false) === true;

const endpoint = arg('endpoint', env.S3_ENDPOINT);
const s3 = s3lib.client(endpoint === true ? undefined : endpoint);
const out = fs.createWriteStream(logPath, { flags: 'a' });

let seq = seqStart;
let done = 0;
let stopping = false;
const started = Date.now();

process.on('SIGTERM', () => { stopping = true; });
process.on('SIGINT', () => { stopping = true; });

function record(op, bkt, key, size, rc) {
    out.write(`${new Date().toISOString()} ${op} ${bkt} ${key} ${size} ${rc}\n`);
}

function finish() {
    out.end(() => {
        process.stderr.write(`driver done: ${done} iterations, last seq ${seq}\n`);
        process.exit(0);
    });
}

async function tick() {
    if (stopping
        || (count && done >= count)
        || (duration && (Date.now() - started) / 1000 >= duration)) {
        return finish();
    }
    seq += 1;
    const size = 1000 + seq;
    const bkt = buckets[(seq - 1) % buckets.length];
    const slot = buckets.length > 1 ? `b${(seq - 1) % buckets.length}-` : '';
    const useStraddle = straddle > 0 && seq % straddleEvery === 0;
    const key = useStraddle
        ? `${prefix}-${slot}strad-${Math.floor(seq / straddleEvery) % straddle}`
        : `${prefix}-${String(seq).padStart(5, '0')}`;
    try {
        await s3lib.put(s3, bkt, key, size);
        record('PUT', bkt, key, size, 'ok');
    } catch (err) {
        record('PUT', bkt, key, size, `ERR:${s3lib.codeOf(err)}`);
    }
    if (useStraddle && !noDelete) {
        try {
            await s3lib.del(s3, bkt, key);
            record('DELETE', bkt, key, size, 'ok');
        } catch (err) {
            record('DELETE', bkt, key, size, `ERR:${s3lib.codeOf(err)}`);
        }
    }
    done += 1;
    return setTimeout(tick, Math.max(0, Math.round(1000 / rate)));
}

process.stderr.write(`driver start: endpoint=${endpoint} `
    + `buckets=${buckets.join(',')} prefix=${prefix} rate=${rate} `
    + `duration=${duration} count=${count} straddle=${straddle}\n`);
tick();
