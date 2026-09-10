'use strict';

/**
 * Act 05, rig scenarios M11 and M12: kill -9, twice on the populator and
 * once on a worker, under load.
 *
 * The populator accumulates a batch in memory while it reads and filters the
 * log, produces the whole batch to kafka in one send, and only then writes
 * its ZooKeeper offset. So there are two kill windows: during read and
 * filter, where a kill costs nothing and the restart redoes the work, and
 * between the kafka acknowledgement and the offset write, where the restart
 * republishes the whole batch. Low probability, high amplitude. Two kills on
 * the rig never hit it.
 *
 * A worker kill re-delivers exactly the uncommitted window, and the bound on
 * that window is the consumer's auto-commit interval of five seconds, not
 * the configured concurrency. That interval is not exposed anywhere in the
 * deliveryPool schema, so an operator who needs a tighter bound cannot ask
 * for one.
 */

const assert = require('assert');
const env = require('../lib/env');
const kafka = require('../lib/kafka');
const procs = require('../lib/procs');
const wait = require('../lib/wait');
const flow = require('../lib/flow');
const s3lib = require('../lib/s3');
const { Act, say, note, watch, step } = require('../lib/narrate');

const DEST = 'poc-dest-1';
const BUCKET = 'demo-bucket';

function register(ctx) {
    describe('Act 05: crashes', () => {
        const act = new Act('05', 'crashes', 'M11 and M12',
            'a populator kill costs nothing; a worker kill costs the '
            + 'uncommitted window and nothing else');
        let topic;
        let pool;
        let populator;
        let worker;

        before(async () => {
            act.open();
            topic = ctx.customerTopicOf[DEST];
            act.expect('populator kills', 2);
            act.expect('populator gaps (loss)', 0);
            act.expect('populator duplicate extras', 0);
            act.expect('worker gaps (loss)', 0);
            act.expect('worker duplicate extras',
                'the uncommitted window, about 5s of traffic');
            act.expect('worker per-key inversions', 0);
            act.expect('duplicates against concurrency',
                'far below concurrency: the bound is the 5s auto-commit');
            pool = flow.poolConfig(act, ctx);
            await s3lib.bucketWith(ctx.s3, BUCKET, [DEST]);
            populator = await flow.startPopulator(act, pool, 'pool');
            worker = await flow.startWorker(act, pool, 1, { autoRestart: true });
        });

        after(() => {
            procs.stopAll();
            act.close();
        });

        it('survives two populator kills with no loss and no duplicates',
            async () => {
                const from = kafka.head(topic, 0);
                const load = flow.startDriver(act, { 'bucket': BUCKET,
                    'prefix': 'm11', 'rate': 5, 'duration': env.workSecs(120, 60),
                    'straddle': 2,
                    'straddle-every': 5 });
                await procs.sleep(env.pause(20000));

                step(1, 'kill -9 the populator wherever its batch happens to be');
                const zkBefore = require('../lib/zk').populatorOffset();
                say(`populator pid ${populator.pid}, zookeeper offset ${zkBefore}`);
                watch('grafana', 'the delivery topic stops growing for a moment');
                populator.kill('SIGKILL');
                act.timeline(`populator KILL9 pid=${populator.pid}`);
                await procs.sleep(env.pause(5000));
                populator = await flow.startPopulator(act, pool, 'pool2');
                say('restarted; zookeeper offset now '
                    + `${require('../lib/zk').populatorOffset()}`);
                note('the offset is the checkpoint: a kill before the write');
                note('means the restart redoes the batch, which costs nothing');

                step(2, 'kill it again, this time right after it has published');
                await wait.until('a publish with no batch completion after it',
                    () => {
                        const log = populator.logText();
                        const tail = log.slice(-4000);
                        return /publishing addressed message|publish/.test(tail)
                            && !/batch completed\s*$/.test(tail);
                    }, 90000, 1000);
                say(`populator pid ${populator.pid}, killing it mid-batch`);
                populator.kill('SIGKILL');
                act.timeline(`populator KILL9 mid-batch pid=${populator.pid}`);
                await procs.sleep(env.pause(4000));
                populator = await flow.startPopulator(act, pool, 'pool3');
                act.measured('populator kills', 2);

                step(3, 'let the load finish, then drain and check');
                await wait.until('the driver to finish',
                    () => !load.proc.isRunning(), 200000, 2000);
                await wait.frozen(env.DELIVERY_TOPIC, env.pause(12000));
                await flow.drainOrCure({ group: env.DELIVERY_GROUP, label: 'pool',
                    timeoutMs: 300000, workers: [1] });
                const r = flow.dumpAndCheck({ act, topic, from,
                    driver: load.log, keyPrefix: 'm11', label: 'populator-kills' });
                act.measured('populator gaps (loss)', r.totals.gaps);
                act.measured('populator duplicate extras',
                    r.totals.duplicate_extras);
                note('per-key order also holds across a kill, because a');
                note('destination\'s delivery key is constant: every record');
                note('for it goes to one partition and one worker in publish');
                note('order');
                assert.strictEqual(r.totals.gaps, 0,
                    'a populator kill lost events');
            });

        it('confines a worker kill to the uncommitted window', async () => {
            const from = kafka.head(topic, 0);
            const load = flow.startDriver(act, { 'bucket': BUCKET, 'prefix': 'm12',
                'rate': 5, 'duration': env.workSecs(90, 45), 'straddle': 2,
                'straddle-every': 5 });

            step(4, 'wait for real lag on the delivery topic, then kill -9');
            await procs.sleep(env.pause(45000));
            const head = kafka.headTotal(env.DELIVERY_TOPIC);
            const state = kafka.groupState(env.DELIVERY_GROUP);
            const window = head - state.committed;
            const deliveredBefore = await wait.counter(1, 'delivered');
            say(`delivery head ${head}, committed ${state.committed}, so an `
                + `uncommitted window of ${window} records`);
            say(`worker has delivered ${deliveredBefore} so far`);
            act.measured('uncommitted window at the kill', `${window} records`);
            watch('grafana', 'row "Health": delivery workers up drops to 0, '
                + 'then back to 1');
            const restartsBefore = worker.restarts;
            const assignsBefore = worker.rebalances().assign;
            const killedAt = Date.now();
            worker.kill('SIGKILL');
            act.timeline(`worker1 KILL9 window=${window}`);
            note('the supervisor restarts it about two seconds later, which is');
            note('what systemd does on a deployment');
            // The killed pid can still answer a signal-0 probe for a moment
            // as a zombie, so "is it running" is the wrong question. The
            // right one is whether the supervisor has spawned a replacement.
            const respawned = await wait.until('the supervisor to spawn a replacement',
                () => worker.restarts > restartsBefore && worker.isRunning(),
                90000, 1000);
            assert.ok(respawned, 'the supervisor never restarted the worker');
            // its probe has to come back too, or the numbers below are blind:
            // a probe that cannot bind is non-fatal by design
            await wait.until('the restarted worker\'s probe to answer',
                async () => (await wait.liveness(1)) === 200, 60000, 2000);
            say(`worker back as pid ${worker.pid}, exits so far `
                + `${worker.exits.length}`);
            note('it holds nothing yet: the dead member\'s session has to');
            note('expire, and the first join is revoked and re-assigned about');
            note('forty seconds later. That pause is part of what a worker');
            note('death costs, so it is waited for and measured here.');
            const joined = await wait.until('the replacement\'s first assignment',
                () => worker.rebalances().assign > assignsBefore, 150000, 2000);
            const pause = Math.round((Date.now() - killedAt) / 1000);
            say(`replacement ${joined ? 'assigned' : 'still unassigned'} `
                + `${pause}s after the kill`);
            act.measured('pause until the replacement is assigned',
                `${pause}s`);

            step(5, 'let the load finish, drain, and check');
            await wait.until('the driver to finish',
                () => !load.proc.isRunning(), 200000, 2000);
            await wait.frozen(env.DELIVERY_TOPIC, env.pause(12000));
            await flow.drainOrCure({ group: env.DELIVERY_GROUP, label: 'pool',
                timeoutMs: 300000, workers: [1] });
            const r = flow.dumpAndCheck({ act, topic, from, driver: load.log,
                keyPrefix: 'm12', label: 'worker-kill' });
            act.measured('worker gaps (loss)', r.totals.gaps);
            act.measured('worker duplicate extras', r.totals.duplicate_extras);
            act.measured('worker per-key inversions', r.totals.inversions);
            const concurrency = require('fs').existsSync(pool)
                ? JSON.parse(require('fs').readFileSync(pool, 'utf8'))
                    .extensions.notification.deliveryPool.concurrency
                : '?';
            act.measured('duplicates against concurrency',
                `${r.totals.duplicate_extras} duplicates, concurrency ${concurrency}`);
            note(`the arithmetic to check: the uncommitted window (${window})`);
            note('is the duplicates already delivered plus the records not yet');
            note('delivered, and unique deliveries equal the driver\'s count');

            assert.strictEqual(r.totals.gaps, 0, 'a worker kill lost events');
            assert.strictEqual(r.totals.inversions, 0,
                'a worker kill reordered a key');
        });
    });
}

module.exports = { register };
