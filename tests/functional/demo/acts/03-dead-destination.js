'use strict';

/**
 * Act 03, rig scenario M10: a destination is down. Both halves, because the
 * comparison is the whole point.
 *
 *   legacy   a per-destination processor whose destination is unreachable
 *            cannot start at all; one that is already running attempts only
 *            its concurrency worth of records, waits out librdkafka's five
 *            minute default, never advances its consumer offset on any
 *            partition, writes nothing to the failed topic the config names,
 *            and exposes no counter.
 *   pool     the worker starts healthy with every dead destination
 *            configured, drops each record with a reason label, commits past
 *            the drops, and keeps delivering to a healthy destination.
 *
 * Three failure classes, as on the rig: a refused connection, a blackholed
 * address, and a reachable broker whose topic has no leader.
 */

const assert = require('assert');
const env = require('../lib/env');
const kafka = require('../lib/kafka');
const procs = require('../lib/procs');
const wait = require('../lib/wait');
const flow = require('../lib/flow');
const s3lib = require('../lib/s3');
const { Act, say, note, watch, step } = require('../lib/narrate');

const LEADERLESS = 'customer-topic-leaderless';
// the three failure classes, on three destinations the platform already
// validates: a refused connection, an unroutable address, and a reachable
// broker whose topic has no leader
const REFUSED = 'poc-dest-3';
const BLACKHOLE = 'krb-dest-a';
const NO_LEADER = 'krb-dest-b';
const HEALTHY = 'poc-dest-1';
const DEAD = [REFUSED, BLACKHOLE, NO_LEADER];
const STALL_WATCH_S = Number(env.knob('DEMO_STALL_WATCH_S',
    env.PACE === 'fast' ? 90 : 300));

function register(ctx) {
    describe('Act 03: a destination is down', () => {
        const act = new Act('03', 'dead-destination', 'M10',
            'today a dead destination stalls silently with no counter; the '
            + 'pool turns it into a counted drop in about 30 seconds');
        let topics;

        before(() => {
            act.open();
            topics = Object.assign({}, ctx.customerTopicOf);
            act.expect('legacy processor, refused destination',
                'cannot start, then exits');
            act.expect('legacy offset advance', 'none, on any partition');
            act.expect('legacy dead-letter records', 0);
            act.expect('legacy counter for the failure', 'none');
            act.expect('pool worker start',
                'healthy with every dead destination configured');
            act.expect(`pool dropped ${REFUSED}`, '20, producer_error');
            act.expect(`pool dropped ${BLACKHOLE}`, '20, producer_error');
            act.expect(`pool dropped ${NO_LEADER}`, '20, delivery_error');
            act.expect('pool delivery group lag', '0, it commits past the drops');
            act.expect('healthy destination meanwhile', '5 of 5 delivered');
        });

        after(() => {
            procs.stopAll();
            act.close();
        });

        it('stalls on the legacy path and counts the drop on the pool',
            async () => {
                step(1, 'build the third failure class: a leaderless topic');
                note('a reachable broker with an unwritable target. Created');
                note('with --replica-assignment 99, a broker that does not');
                note('exist, so the partition has no leader and never will.');
                if (!kafka.topicExists(LEADERLESS)) {
                    const r = kafka.tool('kafka-topics.sh',
                        ['--create', '--topic', LEADERLESS,
                            '--replica-assignment', '99']);
                    say(`${LEADERLESS}: ${r.ok ? 'created' : 'already there or refused'}`);
                }

                step(2, 'a bucket per dead destination, and a healthy control');
                const buckets = {};
                for (const id of DEAD) {
                    buckets[id] = `demo-bucket-dead-${id}`;
                     
                    const put = await s3lib.bucketWith(ctx.s3, buckets[id], [id]);
                    say(`${buckets[id]} -> ${id}: `
                        + `${put.ok ? 'configured' : `refused, ${put.message}`}`);
                }
                const alive = 'demo-bucket-alive';
                await s3lib.bucketWith(ctx.s3, alive, [HEALTHY]);
                say(`${alive} -> ${HEALTHY}, the control`);

                // ------------------------------------------------ legacy ---
                step(3, 'legacy half: a processor for the REFUSED destination');
                note('expected: it does not start. The destination setup');
                note('treats the first producer error as fatal, so the process');
                note('exits, and under a supervisor that is a crash loop that');
                note('consumes nothing.');
                const legacy = flow.legacyConfig(act, ctx, {
                    customerTopics: topics,
                    only: [HEALTHY].concat(DEAD),
                    dead: { [REFUSED]: 'refused', [BLACKHOLE]: 'blackhole' },
                    leaderless: { [NO_LEADER]: LEADERLESS },
                });
                await flow.startPopulator(act, legacy, 'legacy');
                const p3 = await flow.startProcessor(act, legacy, REFUSED,
                    { expectFailure: true });
                await procs.sleep(env.pause(8000));
                const exited = !p3.isRunning();
                const why = (p3.logText().match(
                    /Client is disconnected|error setting up kafka notif destination/g)
                    || []).length;
                say(`processor ${REFUSED} ${exited ? 'exited' : 'is still up'}`
                    + `, ${why} setup-failure lines in its log`);
                act.measured('legacy processor, refused destination',
                    exited ? 'cannot start, then exits' : 'started (differs)');

                step(4, `legacy half: the leaderless destination, ${STALL_WATCH_S}s`);
                note('this one DOES start: the broker is reachable, only the');
                note('topic is unwritable. Watch the group offsets stand still.');
                const p6 = await flow.startProcessor(act, legacy, NO_LEADER);
                const group6 = env.legacyGroup(NO_LEADER);
                await flow.runDriver(act, { bucket: buckets[NO_LEADER],
                    prefix: 'deadnl', rate: 6, count: 20 });
                const before6 = kafka.groupState(group6).committed;
                const failedBefore = kafka.headTotal(env.FAILED_TOPIC);
                watch('grafana', 'nothing moves: there is no counter for this');
                const started = Date.now();
                while ((Date.now() - started) / 1000 < STALL_WATCH_S) {
                    const st = kafka.groupState(group6);
                    say(`t+${Math.round((Date.now() - started) / 1000)}s `
                        + `committed ${st.committed}, ${st.unknown} of `
                        + `${st.partitions} partitions held with NO committed `
                        + 'offset, failed topic '
                        + `${kafka.headTotal(env.FAILED_TOPIC)}`);
                     
                    await procs.sleep(30000);
                }
                const after6 = kafka.groupState(group6).committed;
                act.measured('legacy offset advance', before6 === after6
                    ? 'none, on any partition' : `moved ${before6} to ${after6}`);
                act.measured('legacy dead-letter records',
                    kafka.headTotal(env.FAILED_TOPIC) - failedBefore);
                act.measured('legacy counter for the failure',
                    'none (the processor has no metrics route on this path)');
                const timedOut = /message timed out/.test(p6.logText());
                const lostAssignment
                    = /Group partition assignment lost|-142/.test(p6.logText());
                if (timedOut || lostAssignment) {
                    note('the log now carries "message timed out" and the commit');
                    note('failing with the group assignment lost: the five minute');
                    note('stall blew max.poll.interval.ms and the broker had');
                    note('already evicted the member. That is the same');
                    note('self-eviction chain replication hits, reached here by');
                    note('a dead destination.');
                } else if (STALL_WATCH_S < 300) {
                    note(`this run watched only ${STALL_WATCH_S}s: the timeout at`);
                    note('about 300s needs DEMO_PACE=normal or DEMO_STALL_WATCH_S=330');
                }
                p6.stop();
                procs.stopAll();
                await procs.sleep(env.pause(4000));

                // -------------------------------------------------- pool ---
                step(5, 'pool half: the same three destinations, one worker');
                const pool = flow.poolConfig(act, ctx, {
                    customerTopics: topics,
                    only: [HEALTHY].concat(DEAD),
                    dead: { [REFUSED]: 'refused', [BLACKHOLE]: 'blackhole' },
                    leaderless: { [NO_LEADER]: LEADERLESS },
                    tag: 'dead',
                });
                await flow.startPopulator(act, pool, 'pool');
                await flow.startWorker(act, pool, 1);
                const live = await wait.liveness(1);
                say(`worker liveness ${live}, with four unreachable `
                    + 'destinations configured');
                act.measured('pool worker start', live === 200
                    ? 'healthy with every dead destination configured'
                    : `liveness ${live}`);
                note('producers are created lazily, per endpoint, so an');
                note('unreachable destination costs nothing at startup');

                step(6, '20 PUTs into each dead bucket, 5 into the healthy one');
                const t0 = Date.now();
                for (const id of DEAD) {
                     
                    await flow.runDriver(act, { bucket: buckets[id],
                        prefix: `pool-${id}`, rate: 6, count: 20 });
                }
                await flow.runDriver(act, { bucket: alive, prefix: 'pool-alive',
                    rate: 2, count: 5 });
                watch('grafana', 'row "Failure", drops per second by reason');

                step(7, 'watch dropped_total{target,reason}');
                const firstDrop = {};
                await wait.until('every dead destination to be counted',
                    async () => {
                        const byTarget = await wait.counterBy(1, 'dropped', 'target');
                        const byReason = await wait.counterBy(1, 'dropped', 'reason');
                        Object.keys(byTarget).forEach(t => {
                            if (byTarget[t] > 0 && !firstDrop[t]) {
                                firstDrop[t] = Math.round((Date.now() - t0) / 1000);
                                say(`first drop for ${t} at t+${firstDrop[t]}s`);
                            }
                        });
                        say(`dropped by target ${JSON.stringify(byTarget)} `
                            + `by reason ${JSON.stringify(byReason)}`);
                        return DEAD.every(id => (byTarget[id] || 0) >= 20);
                    }, 180000, 10000);

                const rows = await wait.metrics(1);
                for (const id of DEAD) {
                    const mine = rows.filter(r => r.name === wait.COUNTER.dropped
                        && r.labels.target === id);
                    const total = mine.reduce((a, r) => a + r.value, 0);
                    const reasons = Array.from(new Set(mine.map(r => r.labels.reason)));
                    act.measured(`pool dropped ${id}`,
                        `${total}, ${reasons.join(',')}`);
                }
                const delivered = await wait.counter(1, 'delivered',
                    { target: HEALTHY });
                act.measured('healthy destination meanwhile',
                    `${delivered} of 5 delivered`);
                const drain = await wait.drain({ group: env.DELIVERY_GROUP,
                    label: 'pool', timeoutMs: 120000, workers: [1] });
                act.measured('pool delivery group lag', drain.drained
                    ? '0, it commits past the drops' : `${drain.lag}`);

                note('carried from the rig: dropped_total{reason} cannot yet');
                note('tell a timeout from a rejection, because the delivery');
                note('report carries a different error code than the timeout');
                note('branch tests for. Fix it before the label is promised');
                note('to operators.');

                assert.ok(delivered >= 5,
                    'the healthy destination did not keep delivering');
                assert.ok(drain.drained,
                    'the pool did not commit past the drops');
                for (const id of DEAD) {
                    const total = rows.filter(r => r.name === wait.COUNTER.dropped
                        && r.labels.target === id)
                        .reduce((a, r) => a + r.value, 0);
                    assert.ok(total >= 20,
                        `${id} was not counted as dropped (${total})`);
                }
            });
    });
}

module.exports = { register };
