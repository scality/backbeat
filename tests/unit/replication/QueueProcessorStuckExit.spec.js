const sinon = require('sinon');

const QueueProcessor =
    require('../../../extensions/replication/queueProcessor/QueueProcessor');

const site = 'test-site-1';
const topic = 'backbeat-stuck-exit-spec';

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
                site, servers: ['localhost:9095'],
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
    const envBackup =
        process.env.REPLICATION_QUEUE_PROCESSOR_CRASH_ON_REBALANCE_TIMEOUT;

    beforeEach(() => {
        delete process.env
            .REPLICATION_QUEUE_PROCESSOR_CRASH_ON_REBALANCE_TIMEOUT;
        qp = makeQueueProcessor();
        exitStub = sinon.stub(process, 'exit');
        fatalStub = sinon.stub(qp.logger, 'fatal');
        clock = sinon.useFakeTimers();
    });

    afterEach(() => {
        sinon.restore();
        if (envBackup === undefined) {
            delete process.env
                .REPLICATION_QUEUE_PROCESSOR_CRASH_ON_REBALANCE_TIMEOUT;
        } else {
            process.env.REPLICATION_QUEUE_PROCESSOR_CRASH_ON_REBALANCE_TIMEOUT
                = envBackup;
        }
    });

    it('should exit(1) with a fatal log when the CRR self-restart gate ' +
    'is armed', () => {
        process.env.REPLICATION_QUEUE_PROCESSOR_CRASH_ON_REBALANCE_TIMEOUT
            = 'true';
        qp._onRebalanceTimeout();
        sinon.assert.calledOnceWithMatch(
            fatalStub, sinon.match(/^CRR consumer stuck/),
            sinon.match({ site }));
        // hard exit only fires after the 1s log-flush grace
        clock.tick(999);
        sinon.assert.notCalled(exitStub);
        clock.tick(1);
        sinon.assert.calledOnceWithExactly(exitStub, 1);
    });

    it('should not exit when the CRR self-restart gate env var is ' +
    'unset', () => {
        qp._onRebalanceTimeout();
        sinon.assert.notCalled(fatalStub);
        clock.tick(2000);
        sinon.assert.notCalled(exitStub);
    });

    it('should not exit when the CRR self-restart gate env var is not ' +
    'exactly "true"', () => {
        process.env.REPLICATION_QUEUE_PROCESSOR_CRASH_ON_REBALANCE_TIMEOUT
            = 'false';
        qp._onRebalanceTimeout();
        sinon.assert.notCalled(fatalStub);
        clock.tick(2000);
        sinon.assert.notCalled(exitStub);
    });
});
