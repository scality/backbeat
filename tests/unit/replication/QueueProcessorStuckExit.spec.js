const assert = require('assert');
const fs = require('fs');
const os = require('os');
const path = require('path');
const sinon = require('sinon');
// werelogs exports a bound constructor without a .prototype: require the
// underlying class to spy on logging across newly created instances
const WerelogsLogger = require('werelogs/lib/Logger');

const QueueProcessor =
    require('../../../extensions/replication/queueProcessor/QueueProcessor');

// unique per test run, so parallel/repeated runs do not collide on the
// budget file in the shared OS temp dir
const site = 'site';
const topic = `backbeat-stuck-exit-spec-${process.pid}`;
const budgetFile = path.join(
    os.tmpdir(), `backbeat-qp-exit-budget-${site}-${topic}.json`);

const HOUR_MS = 60 * 60 * 1000;

function makeQueueProcessor() {
    return new QueueProcessor(
        topic,
        { connectionString: '127.0.0.1:2181/backbeat',
          autoCreateNamespace: false },
        null,
        { hosts: 'localhost:9092' },
        { auth: { type: 'role',
            vault: { host: 'localhost:9093',
                port: 7777 } },
            s3: { host: 'localhost:9094',
                port: 7777 },
            transport: 'http',
        },
        { auth: { type: 'role' },
            bootstrapList: [{
                site: 'site', servers: ['localhost:9095'],
            }],
            transport: 'http' },
        { topic,
          replicationStatusTopic: 'backbeat-func-test-repstatus',
          queueProcessor: {
              retry: {
                  scality: { timeoutS: 5 },
              },
              groupId: 'backbeat-func-test-group-id',
              mpuPartsConcurrency: 10,
          },
        },
        null, // no redis in unit tests
        { topic: 'metrics-test-topic' },
        {},
        {},
        site,
    );
}

describe('QueueProcessor stuck-consumer self-exit', () => {
    let qp;
    let clock;
    let exitStub;
    let fatalStub;
    let errorStub;
    let warnStub;
    const envBackup = process.env.REPLICATION_QUEUE_PROCESSOR_CRASH_ON_REBALANCE_TIMEOUT;

    beforeEach(() => {
        fs.rmSync(budgetFile, { force: true });
        delete process.env.REPLICATION_QUEUE_PROCESSOR_CRASH_ON_REBALANCE_TIMEOUT;
        qp = makeQueueProcessor();
        exitStub = sinon.stub(process, 'exit');
        fatalStub = sinon.stub(qp.logger, 'fatal');
        errorStub = sinon.stub(qp.logger, 'error');
        warnStub = sinon.stub(qp.logger, 'warn');
        clock = sinon.useFakeTimers({ now: Date.now() });
    });

    afterEach(() => {
        sinon.restore();
        fs.rmSync(budgetFile, { force: true });
        if (envBackup === undefined) {
            delete process.env.REPLICATION_QUEUE_PROCESSOR_CRASH_ON_REBALANCE_TIMEOUT;
        } else {
            process.env.REPLICATION_QUEUE_PROCESSOR_CRASH_ON_REBALANCE_TIMEOUT = envBackup;
        }
    });

    it('should log fatal and exit(1) after 1s when armed and within ' +
    'budget', () => {
        process.env.REPLICATION_QUEUE_PROCESSOR_CRASH_ON_REBALANCE_TIMEOUT = 'true';
        qp._onRebalanceTimeout({
            queueLen: 3,
            running: 2,
            stuckTasks: [
                { topic, partition: 0, offset: 12, key: 'k1', ageSeconds: 45 },
                { topic, partition: 1, offset: 34, key: 'k2', ageSeconds: 90 },
            ],
        });
        sinon.assert.calledOnceWithMatch(
            fatalStub,
            'consumer stuck after rebalance drain timeout, exiting for ' +
            'restart',
            sinon.match({
                stuckTaskCount: 2,
                oldestAgeSeconds: 90,
                queueLen: 3,
                running: 2,
                exitsInWindow: 1,
            }));
        // hard exit only fires after the 1s grace delay
        clock.tick(999);
        sinon.assert.notCalled(exitStub);
        clock.tick(1);
        sinon.assert.calledOnceWithExactly(exitStub, 1);
        // the exit was persisted to the budget file
        const persisted = JSON.parse(fs.readFileSync(budgetFile, 'utf8'));
        assert.strictEqual(persisted.length, 1);
        assert.strictEqual(typeof persisted[0], 'number');
    });

    it('should not exit and log error when env var is unset', () => {
        qp._onRebalanceTimeout({ queueLen: 3, running: 2 });
        sinon.assert.calledOnceWithMatch(
            errorStub,
            sinon.match(/self-restart disabled/),
            sinon.match({ queueLen: 3, running: 2 }));
        sinon.assert.notCalled(fatalStub);
        clock.tick(2000);
        sinon.assert.notCalled(exitStub);
    });

    it('should not exit when env var is "false" (exact match ' +
    'required)', () => {
        process.env.REPLICATION_QUEUE_PROCESSOR_CRASH_ON_REBALANCE_TIMEOUT = 'false';
        qp._onRebalanceTimeout({ queueLen: 3, running: 2 });
        sinon.assert.calledOnceWithMatch(
            errorStub, sinon.match(/self-restart disabled/));
        sinon.assert.notCalled(fatalStub);
        clock.tick(2000);
        sinon.assert.notCalled(exitStub);
    });

    it('should not exit when the exit budget is exhausted', () => {
        process.env.REPLICATION_QUEUE_PROCESSOR_CRASH_ON_REBALANCE_TIMEOUT = 'true';
        const now = Date.now();
        fs.writeFileSync(budgetFile, JSON.stringify(
            [now - (3 * HOUR_MS), now - (2 * HOUR_MS), now - HOUR_MS]));
        qp._onRebalanceTimeout({ queueLen: 3, running: 2 });
        sinon.assert.calledOnceWithMatch(
            errorStub,
            'stuck-consumer exit budget exhausted, staying disconnected ' +
            'for operator intervention',
            sinon.match({ exitsInWindow: 3, windowHours: 6 }));
        sinon.assert.notCalled(fatalStub);
        clock.tick(2000);
        sinon.assert.notCalled(exitStub);
    });

    it('should ignore exit timestamps older than the 6h window', () => {
        process.env.REPLICATION_QUEUE_PROCESSOR_CRASH_ON_REBALANCE_TIMEOUT = 'true';
        const now = Date.now();
        fs.writeFileSync(budgetFile, JSON.stringify(
            [now - (9 * HOUR_MS), now - (8 * HOUR_MS), now - (7 * HOUR_MS)]));
        qp._onRebalanceTimeout({ queueLen: 3, running: 2 });
        sinon.assert.calledOnceWithMatch(
            fatalStub, sinon.match.string,
            sinon.match({ exitsInWindow: 1 }));
        clock.tick(1000);
        sinon.assert.calledOnceWithExactly(exitStub, 1);
    });

    it('should fail open (warn and still exit) on a corrupt budget ' +
    'file', () => {
        process.env.REPLICATION_QUEUE_PROCESSOR_CRASH_ON_REBALANCE_TIMEOUT = 'true';
        fs.writeFileSync(budgetFile, 'not json {');
        qp._onRebalanceTimeout({ queueLen: 3, running: 2 });
        sinon.assert.calledWithMatch(
            warnStub,
            'could not read/persist stuck-consumer exit budget file, ' +
            'treating as empty (fail-open)',
            sinon.match({ op: 'read', path: budgetFile }));
        sinon.assert.calledOnce(fatalStub);
        clock.tick(1000);
        sinon.assert.calledOnceWithExactly(exitStub, 1);
    });

    it('should warn at boot when the budget file has fresh exit ' +
    'timestamps', () => {
        const now = Date.now();
        const lastExitAt = now - (5 * 60 * 1000);
        fs.writeFileSync(budgetFile, JSON.stringify(
            [now - HOUR_MS, lastExitAt]));
        const protoWarn = sinon.spy(WerelogsLogger.prototype, 'warn');
        makeQueueProcessor();
        const call = protoWarn.getCalls().find(
            c => c.args[0] === 'starting after stuck-consumer self-exit');
        assert(call, 'expected boot-correlation warn to be logged');
        assert.strictEqual(call.args[1].exitsInWindow, 2);
        assert.strictEqual(call.args[1].lastExitAt, lastExitAt);
        assert.strictEqual(typeof call.args[1].downSeconds, 'number');
        assert(call.args[1].downSeconds >= 0);
    });
});
