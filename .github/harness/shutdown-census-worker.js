/*
 * One "pod" for the BB-833 shutdown census.
 *
 * Consumes a topic, records every sequence number it processes to a file, and
 * on SIGTERM calls BackbeatConsumer.close() and reports how long that took.
 * It deliberately does NOT call process.exit() afterwards: whether node exits
 * on its own is one of the things being measured.
 *
 * The driver reads the processed-sequence files after the pods are gone and
 * reconciles them against what was produced.
 */
'use strict';

const fs = require('fs');
const path = require('path');

const REPO = process.cwd();

require(path.join(REPO, 'node_modules/werelogs'))
    .configure({ level: 'error', dump: 'error' });

const kafka = require(path.join(REPO, 'node_modules/node-rdkafka'));
const BackbeatConsumer = require(path.join(REPO, 'lib/BackbeatConsumer'));

const {
    HOSTS, TOPIC, GROUP_ID, ROLE, OUT_DIR,
    SLOW_MS, CONCURRENCY,
} = process.env;

const slowMs = Number(SLOW_MS || 0);
const concurrency = Number(CONCURRENCY || 1);
const processedFile = path.join(OUT_DIR, `processed-${ROLE}.txt`);

const send = msg => process.send && process.send({ role: ROLE, ...msg });

// buffered so the queue processor stays off the syscall path; flushed on a
// short timer and again at every point that matters for the reconciliation
let buffer = [];
function flush() {
    if (buffer.length === 0) {
        return;
    }
    const chunk = `${buffer.join('\n')}\n`;
    buffer = [];
    try {
        fs.appendFileSync(processedFile, chunk);
    } catch (err) {
        send({ type: 'error', message: `flush failed: ${err.message}` });
    }
}
const flushTimer = setInterval(flush, 50);

let processed = 0;
let started = 0;

// report every rebalance before the real handler runs. Polling for the
// assignment to change is too coarse: the window between releasing the
// partitions and rejoining can close inside a single poll interval.
const originalOnRebalance = BackbeatConsumer.prototype._onRebalance;
BackbeatConsumer.prototype._onRebalance = function onRebalance(err, assignment) {
    const code = err && err.code;
    let kind = 'other';
    if (code === kafka.CODES.ERRORS.ERR__REVOKE_PARTITIONS) {
        kind = 'revoke';
    } else if (code === kafka.CODES.ERRORS.ERR__ASSIGN_PARTITIONS) {
        kind = 'assign';
    }
    send({ type: 'rebalance', kind, count: assignment ? assignment.length : 0 });
    return originalOnRebalance.call(this, err, assignment);
};

const consumer = new BackbeatConsumer({
    clientId: `census-${ROLE}`,
    kafka: { hosts: HOSTS },
    topic: TOPIC,
    groupId: GROUP_ID,
    concurrency,
    // the group is new every iteration, so without this the pods can resolve
    // their fetch position past everything the driver produced
    fromOffset: 'earliest',
    queueProcessor: (entry, cb) => {
        started++;
        let seq;
        try {
            seq = JSON.parse(entry.value.toString()).seq;
        } catch (err) {
            send({ type: 'error', message: `bad entry: ${err.message}` });
            return cb();
        }
        // recorded on completion, so a task cut short by the shutdown is not
        // counted as processed
        const done = () => {
            buffer.push(seq);
            processed++;
            cb();
        };
        return slowMs > 0 ? setTimeout(done, slowMs) : setImmediate(done);
    },
});

function heldPartitions() {
    try {
        return consumer._consumer.isConnected() ?
            consumer._consumer.assignments().length : 0;
    } catch (e) { // eslint-disable-line no-unused-vars
        return -1;
    }
}

consumer.on('error', err =>
    send({ type: 'error', message: err.message }));

consumer.on('ready', () => {
    consumer.subscribe();
    send({ type: 'subscribed' });
    const report = setInterval(() => send({
        type: 'progress',
        held: heldPartitions(),
        processed,
        started,
    }), 100);
    report.unref();
});

process.on('uncaughtException', err => {
    send({ type: 'uncaught', message: err.message, stack: err.stack });
    flush();
});

process.on('SIGTERM', () => {
    const held = heldPartitions();
    send({ type: 'close-start', held, processed, started });
    const closeCalledAt = Date.now();

    let answered = 0;
    consumer.close(() => {
        answered++;
        flush();
        if (answered > 1) {
            // close() answering twice would corrupt every timing below
            send({ type: 'close-answered-again', count: answered });
            return;
        }
        send({
            type: 'close-returned',
            closeMs: Date.now() - closeCalledAt,
            processedAtClose: processed,
        });
        clearInterval(flushTimer);
        flush();
        // no process.exit(): whether node now exits on its own is the
        // measurement (the twice-reproduced 6 hour CI hang)
    });
});
