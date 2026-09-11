'use strict';

/**
 * Act 08, rig scenarios M9, M8 and M6b: the semantics questions, each
 * short, each ending in a decision somebody has to make. The mixed-window
 * case is gone with the second topic: there is one path, and every change
 * is a container swap, so no two consumers are ever alive together.
 *
 *   detach             queued events for a detached destination: what
 *                      today's processor does with them, and what the pool
 *                      does, both matching at delivery time on today's topic
 *   overlapping rules  one object matching two rules goes to both
 *                      destinations, on both paths
 *   name collision     an account-scoped ARN naming a global destination is
 *                      accepted and delivered to the global one
 */

const assert = require('assert');
const env = require('../lib/env');
const kafka = require('../lib/kafka');
const procs = require('../lib/procs');
const wait = require('../lib/wait');
const flow = require('../lib/flow');
const s3lib = require('../lib/s3');
const check = require('../lib/check');
const { Act, say, note, step } = require('../lib/narrate');

const D1 = 'poc-dest-1';
const D2 = 'poc-dest-2';
const ACCOUNT = '123456789012';

function countKey(file, needle) {
    return check.parseEvents(file).filter(r => String(r.objkey || '')
        .includes(needle)).length;
}

function register(ctx) {
    describe('Act 08: the semantics that change', () => {
        const act = new Act('08', 'semantics', 'M9, M8, M6b',
            'what the migration changes for a customer, and what it keeps');
        let legacy;
        let pool;
        let t1;
        let t2;

        before(async () => {
            act.open();
            t1 = ctx.customerTopicOf[D1];
            t2 = ctx.customerTopicOf[D2];
            legacy = flow.legacyConfig(act, ctx);
            pool = flow.poolConfig(act, ctx);
            act.expect('legacy: delivered to the detached destination', '0 of 20');
            act.expect('pool: delivered to the detached destination',
                '(measured: the worker matches at delivery time too)');
            act.expect('legacy: prefixed object to both destinations', 'yes');
            act.expect('pool: prefixed object to both destinations', 'yes');
            act.expect('unknown account-scoped ARN', 'refused');
            act.expect('colliding account-scoped ARN', 'accepted');
            act.expect('collision delivered to the global destination', '12 of 12');
        });

        after(() => {
            procs.stopAll();
            act.close();
        });

        it('drops queued events on detach today and delivers them on the pool',
            async () => {
                const bucket = 'demo-bucket-detach';
                step(4, 'a bucket with two rules, one of which will be detached');
                await s3lib.bucketWith(ctx.s3, bucket, [D1, D2]);
                await flow.startPopulator(act, legacy, 'legacy');
                const keep = await flow.startProcessor(act, legacy, D1);
                const go = await flow.startProcessor(act, legacy, D2);
                note('the second destination\'s group is warmed first, so it');
                note('holds a committed offset on every partition. Without');
                note('that, auto.offset.reset latest would skip the backlog');
                note('for an unrelated reason.');
                await flow.runDriver(act, { bucket, prefix: 'warm', rate: 6,
                    count: 30 });
                await wait.frozen(env.INTERNAL_TOPIC, env.pause(12000));
                await flow.drainOrCure({ group: env.legacyGroup(D2),
                    label: `legacy ${D2} warm-up`, timeoutMs: 240000 });

                step(5, 'stop its processor, build a backlog, then detach it');
                go.stop();
                await procs.sleep(env.pause(4000));
                const from2 = kafka.head(t2, 0);
                const from1 = kafka.head(t1, 0);
                await flow.runDriver(act, { bucket, prefix: 'legacy-detach',
                    rate: 6, count: 20 });
                await wait.frozen(env.INTERNAL_TOPIC, env.pause(12000));
                say(`backlog for ${D2}: lag `
                    + `${kafka.groupLag(env.legacyGroup(D2))}`);
                const off = await s3lib.putNotification(ctx.s3, bucket, [D1]);
                assert.ok(off.ok, 'the detach was refused');
                say(`${D2} detached: the configuration now has `
                    + `${off.readBack.QueueConfigurations.length} rule`);

                step(6, 'restart its processor and see what it does');
                await flow.startProcessor(act, legacy, D2);
                await flow.drainOrCure({ group: env.legacyGroup(D2),
                    label: `legacy ${D2} after the detach`, timeoutMs: 240000 });
                kafka.dump(t2, from2, act.file('events-legacy-detached.jsonl'), 0);
                kafka.dump(t1, from1, act.file('events-legacy-control.jsonl'), 0);
                const legacyGone = countKey(
                    act.file('events-legacy-detached.jsonl'), 'legacy-detach');
                const legacyKeep = countKey(
                    act.file('events-legacy-control.jsonl'), 'legacy-detach');
                say(`delivered to the detached ${D2}: ${legacyGone} of 20`);
                say(`delivered to the control ${D1}: ${legacyKeep} of 20`);
                note('the processor consumed all 20 and committed. It re-reads');
                note('the bucket configuration at delivery time, no longer');
                note('finds itself, and returns done(). Nothing above debug is');
                note('logged and no metric moves, so the drop is invisible.');
                act.measured('legacy: delivered to the detached destination',
                    `${legacyGone} of 20`);

                step(7, 'the same thing on the pool path');
                keep.stop();
                procs.stopAll();
                await procs.sleep(env.pause(5000));
                await s3lib.putNotification(ctx.s3, bucket, [D1, D2]);
                await flow.startPopulator(act, pool, 'pool');
                flow.seedPoolGroupAtHead(act);
                const poolFrom2 = kafka.head(t2, 0);
                await flow.runDriver(act, { bucket, prefix: 'pool-detach',
                    rate: 6, count: 20 });
                await wait.frozen(env.POOL_TOPIC, env.pause(12000));
                say('the populator wrote one record per event to today\'s '
                    + `topic, as always: ${kafka.headTotal(env.POOL_TOPIC)} records`);
                await s3lib.putNotification(ctx.s3, bucket, [D1]);
                say(`${D2} detached again, and only now is a worker started`);
                const worker = await flow.startWorker(act, pool, 1);
                await flow.drainOrCure({ group: env.DELIVERY_GROUP, label: 'pool',
                    timeoutMs: 240000, workers: [1] });
                kafka.dump(t2, poolFrom2, act.file('events-pool-detached.jsonl'), 0);
                const poolGone = countKey(
                    act.file('events-pool-detached.jsonl'), 'pool-detach');
                say(`delivered to the detached ${D2}: ${poolGone} of 20`);
                if (poolGone === 0) {
                    note('the worker reads today\'s topic and matches each event');
                    note('against the bucket\'s rules at delivery time, exactly');
                    note('as the processor does, so a detached destination\'s');
                    note('queued events are dropped on both paths. Detach stays');
                    note('a revocation. The previous model, which resolved the');
                    note('destination at publish time, delivered them (20 of 20');
                    note('measured on 2026-09-10); that difference is gone with');
                    note('the second topic. Product question 3 becomes: should');
                    note('the drop be counted and visible, which the pool can do');
                    note('and the processor cannot.');
                } else {
                    note('the pool delivered queued events after the detach,');
                    note('which means the worker resolved the destination before');
                    note('the configuration changed. Detach is not a revocation');
                    note('on this path. Release-note material, product question 3.');
                }
                act.measured('pool: delivered to the detached destination',
                    `${poolGone} of 20`);
                worker.stop();
                procs.stopAll();
                await procs.sleep(env.pause(4000));
                assert.strictEqual(legacyGone, 0,
                    'the legacy path delivered to a detached destination');
            });

        it('fans out to both destinations when two rules match', async () => {
            const bucket = 'demo-bucket-overlap';
            step(8, 'a catch-all rule to one destination, a prefix rule to another');
            note('CloudServer\'s filter rule names are case sensitive, so it');
            note('is Prefix, while the matcher compares case-insensitively');
            const put = await s3lib.bucketWith(ctx.s3, bucket, [D1, `${D2}:logs/`]);
            assert.ok(put.ok, `the configuration was refused: ${put.message}`);
            const both = {};
            for (const [pathName, cfg, label] of [
                ['legacy', legacy, 'legacy'], ['pool', pool, 'pool']]) {
                 
                await flow.startPopulator(act, cfg, `${label}-overlap`);
                if (pathName === 'legacy') {
                     
                    await flow.startProcessor(act, cfg, D1);
                     
                    await flow.startProcessor(act, cfg, D2);
                     
                    await flow.warmLegacyGroup(act, { dest: D1, bucket, count: 6 });
                     
                    await flow.warmLegacyGroup(act, { dest: D2, bucket, count: 6 });
                } else {
                    flow.seedPoolGroupAtHead(act);

                    await flow.startWorker(act, cfg, 1);
                }
                const from1 = kafka.head(t1, 0);
                const from2 = kafka.head(t2, 0);
                 
                await flow.runDriver(act, { bucket, prefix: `logs/${label}-x`,
                    rate: 2, count: 2 });
                 
                await flow.runDriver(act, { bucket, prefix: `other/${label}-y`,
                    rate: 2, count: 2 });
                 
                await wait.frozen(env.INTERNAL_TOPIC, env.pause(12000));
                if (pathName === 'legacy') {
                     
                    await flow.drainOrCure({ group: env.legacyGroup(D1),
                        label: `legacy ${D1}`, timeoutMs: 180000 });
                     
                    await flow.drainOrCure({ group: env.legacyGroup(D2),
                        label: `legacy ${D2}`, timeoutMs: 180000 });
                } else {
                     
                    await flow.drainOrCure({ group: env.DELIVERY_GROUP, label: 'pool',
                        timeoutMs: 180000, workers: [1] });
                }
                kafka.dump(t1, from1, act.file(`events-${label}-d1.jsonl`), 0);
                kafka.dump(t2, from2, act.file(`events-${label}-d2.jsonl`), 0);
                const onD1 = countKey(act.file(`events-${label}-d1.jsonl`),
                    `logs/${label}-x`);
                const onD2 = countKey(act.file(`events-${label}-d2.jsonl`),
                    `logs/${label}-x`);
                const otherD2 = countKey(act.file(`events-${label}-d2.jsonl`),
                    `other/${label}-y`);
                say(`${label}: the logs/ object reached ${D1} ${onD1} times `
                    + `and ${D2} ${onD2} times; the other object reached `
                    + `${D2} ${otherD2} times`);
                both[label] = onD1 > 0 && onD2 > 0 && otherD2 === 0;
                act.measured(`${label}: prefixed object to both destinations`,
                    both[label] ? 'yes' : 'no');
                procs.stopAll();
                 
                await procs.sleep(env.pause(5000));
            }
            note('both paths fan out, so the first-match-wins requirement is');
            note('what NEITHER implementation does. Shipping it literally');
            note('would silently stop a bucket delivering to its second');
            note('destination. Product question 6.');
            note('both paths decide the fan-out the same way now: one record');
            note('per event on today\'s topic, and the consumer matches it');
            note('against the bucket\'s rules per destination at delivery time.');
            assert.ok(both.legacy, 'the legacy path did not fan out');
            assert.ok(both.pool, 'the pool did not fan out');
        });

        it('delivers an account-scoped ARN to the global destination',
            async () => {
                const bucket = 'demo-bucket-collide';
                step(9, 'an account-scoped ARN naming a destination that does '
                    + 'not exist');
                await s3lib.createBucket(ctx.s3, bucket);
                const bad = await s3lib.putNotification(ctx.s3, bucket,
                    [`arn:scality:bucketnotif::${ACCOUNT}:not-a-destination`]);
                say(`refused with ${bad.code || 'nothing'}: `
                    + `${(bad.message || '').slice(0, 90)}`);
                note('CloudServer takes the ARN\'s last segment and requires');
                note('it to be in its own destination list, so the ARN parses');
                note('and the destination check fails.');
                act.measured('unknown account-scoped ARN',
                    bad.ok ? 'accepted (differs)' : 'refused');

                step(10, 'the same shape, naming the EXISTING global destination');
                const good = await s3lib.putNotification(ctx.s3, bucket,
                    [`arn:scality:bucketnotif::${ACCOUNT}:${D1}`]);
                say(`accepted: ${good.ok}`);
                if (good.ok) {
                    say(`read back verbatim: ${
                         JSON.stringify(good.readBack.QueueConfigurations[0])}`);
                }
                act.measured('colliding account-scoped ARN',
                    good.ok ? 'accepted' : 'refused (differs)');
                assert.ok(good.ok, 'the colliding ARN was refused, which the '
                    + 'rig did not see');

                step(11, '12 operations, and see where they land');
                await flow.startPopulator(act, legacy, 'legacy-collide');
                await flow.startProcessor(act, legacy, D1);
                await flow.warmLegacyGroup(act, { dest: D1,
                    bucket: 'demo-bucket', count: 6 });
                const from2 = kafka.head(t1, 0);
                const drive = await flow.runDriver(act, { bucket,
                    prefix: 'collide', rate: 3, count: 12 });
                await wait.frozen(env.INTERNAL_TOPIC, env.pause(12000));
                await flow.drainOrCure({ group: env.legacyGroup(D1),
                    label: `legacy ${D1}`, timeoutMs: 240000 });
                const r = flow.dumpAndCheck({ act, topic: t1, from: from2,
                    driver: drive.log, keyPrefix: 'collide', label: 'collision' });
                const ids = new Set(check.parseEvents(
                    act.file('events-collision.jsonl'))
                    .map(e => e.configurationId).filter(Boolean));
                say(`delivered ${r.totals.delivered} of 12 to the GLOBAL `
                    + `${D1}, carrying configurationId `
                    + `${Array.from(ids).join(', ')}`);
                act.measured('collision delivered to the global destination',
                    `${r.totals.delivered} of 12`);
                note('every component matches a destination by the last ARN');
                note('segment and never reads the account field. Any tenant');
                note('who can put a bucket notification configuration can do');
                note('this today, with no new code deployed, and the global');
                note('destination\'s consumer receives events for a');
                note('configuration it never set up. Product question 5, and');
                note('a row in the threat model.');
                assert.strictEqual(r.totals.gaps, 0,
                    'the collision case lost events');
                assert.ok(r.totals.delivered >= 12,
                    'the account-scoped ARN was not delivered to the global '
                    + 'destination');
            });
    });
}

module.exports = { register };
