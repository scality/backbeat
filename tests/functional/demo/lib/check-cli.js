#!/usr/bin/env node
'use strict';

/**
 * The delivery checker as a command, for looking at evidence by hand.
 * Same numbers as the suite uses, and the same contract as the rig's
 * scripts/check.py, which it was verified against.
 *
 * Usage:
 *   check-cli.js --events <dump> --driver <log> [--key-prefix p]
 *                [--bucket b] [--out checker.json] [--label name]
 *
 *   gaps        expected operations that never arrived, which is LOSS
 *   dup_extras  extra copies of an operation
 *   inversions  pairs of operations on one key delivered out of order
 *   unexpected  delivered operations no driver log accounts for
 */

const check = require('./check');

function arg(name, def) {
    const i = process.argv.indexOf(`--${name}`);
    if (i === -1) {
        return def;
    }
    const v = process.argv[i + 1];
    return v === undefined || v.startsWith('--') ? true : v;
}

function all(name) {
    const out = [];
    process.argv.forEach((a, i) => {
        if (a === `--${name}` && process.argv[i + 1]) {
            out.push(process.argv[i + 1]);
        }
    });
    return out;
}

const events = arg('events');
if (!events || events === true) {
    process.stderr.write('need --events <dump> and --driver <log>\n');
    process.exit(2);
}

const result = check.check({
    events,
    driver: all('driver'),
    keyPrefix: arg('key-prefix', undefined),
    bucket: arg('bucket', undefined),
    label: arg('label', 'default'),
    out: arg('out', undefined) === true ? undefined : arg('out', undefined),
});

process.stdout.write(`${check.summary(result)}\n`);
process.stdout.write(`               by_event=${JSON.stringify(result.byEvent)}\n`);
if (result.latency) {
    process.stdout.write(`               latency_s=${JSON.stringify(result.latency)}\n`);
}
const t = result.totals;
if (t.gaps) {
    const miss = {};
    Object.entries(result.perKey).forEach(([k, v]) => {
        if (v.gaps.length) {
            miss[k] = v.gaps;
        }
    });
    process.stdout.write(`               GAPS: ${JSON.stringify(miss).slice(0, 2000)}\n`);
}
if (t.duplicate_extras) {
    const dd = {};
    Object.entries(result.perKey).forEach(([k, v]) => {
        if (v.duplicate_extras) {
            dd[k] = v.duplicates;
        }
    });
    process.stdout.write(`               DUPS: ${JSON.stringify(dd).slice(0, 2000)}\n`);
}
if (t.inversions) {
    const iv = {};
    Object.entries(result.perKey).forEach(([k, v]) => {
        if (v.inversions) {
            iv[k] = v.inversion_pairs;
        }
    });
    process.stdout.write(`               INV: ${JSON.stringify(iv).slice(0, 2000)}\n`);
}
process.exit(0);
