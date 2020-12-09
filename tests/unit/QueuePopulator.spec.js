const assert = require('assert');

const { Logger } = require('werelogs');

const QueuePopulator = require('../../lib/queuePopulator/QueuePopulator');
const LogReader = require('../../lib/queuePopulator/LogReader');

class MockLogReader extends LogReader {
    constructor(queuePopulator, id) {
        super({
            logger: new Logger('MockLogReader'),
        });
        this.queuePopulator = queuePopulator;
        this.id = id;
        this.processLogEntriesCallCount = 0;
    }

    setup(cb) {
        process.nextTick(cb);
    }

    processLogEntries(params, done) {
        this.log.info('processLogEntries', { id: this.id });
        this.processLogEntriesCallCount += 1;
        // check that the provisioning update triggered below stopped
        // the original provisioned raft session from being processed
        assert(this.processLogEntriesCallCount <= 2);
        if (this.processLogEntriesCallCount === 2) {
            // at the 2nd invocation, trigger a provisioning update
            this.queuePopulator.logReadersUpdate = [
                new MockLogReader(this.queuePopulator, 'raft_2'),
            ];
        }
        const processedAll = (this.id === 'raft_2');
        process.nextTick(() => done(null, {
            queuedEntries: {},
            processedAll,
        }));
    }
}

describe('QueuePopulator', () => {
    it('should stop processing old raft sessions after provisioning update', done => {
        const qp = new QueuePopulator({}, {}, {
            logSource: 'bucketd',
        }, null, null, null, {});
        const logReader1 = new MockLogReader(qp, 'raft_1');
        qp.logReadersUpdate = [logReader1];
        qp.processAllLogEntries({}, err => {
            assert.ifError(err);
            assert.strictEqual(logReader1.processLogEntriesCallCount, 2);
            done();
        });
    });
});
