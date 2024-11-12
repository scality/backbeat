const assert = require('assert');
const sinon = require('sinon');
const { Logger } = require('werelogs');
const ZookeeperMock = require('zookeeper-mock');
const MongoLogReader = require('../../../../lib/queuePopulator/MongoLogReader');

class MockLogConsumer {
    constructor(params) {
        this.params = params || {};
    }

    connectMongo(callback) {
        process.nextTick(() => {
            if (this.params.connectMongoError) {
                callback(this.params.connectMongoError);
            } else {
                callback(null);
            }
        });
    }
}

describe('MongoLogReader', () => {
    let mongoLogReader;
    let zkMock;
    let logger;

    beforeEach(() => {
        zkMock = new ZookeeperMock();
        logger = new Logger('test:MongoLogReader');
        mongoLogReader = new MongoLogReader({
            zkClient: zkMock.createClient('localhost:2181'),
            kafkaConfig: { hosts: 'localhost:9092' },
            zkConfig: { connectionString: 'localhost:2181' },
            mongoConfig: { logName: 'test-log' },
            logger,
            extensions: [],
            metricsHandler: {},
        });
        mongoLogReader.setLogConsumer(new MockLogConsumer());
    });

    afterEach(() => {
        sinon.restore();
    });

    describe('constructor', () => {
        it('should initialize MongoLogReader correctly', () => {
            assert(mongoLogReader);
            assert(mongoLogReader.logConsumer instanceof MockLogConsumer);
            assert.strictEqual(mongoLogReader._mongoConfig.logName, 'test-log');
        });
    });

    describe('setup', () => {
        it('should setup MongoDB connection successfully', done => {
            const connectMongoStub = sinon.stub(mongoLogReader.logConsumer, 'connectMongo')
                .callsFake(callback => callback(null));
            mongoLogReader.setup(err => {
                assert.ifError(err);
                assert(connectMongoStub.calledOnce);
                done();
            });
        });

        it('should handle MongoDB connection error', done => {
            const connectMongoStub = sinon.stub(mongoLogReader.logConsumer, 'connectMongo')
                .callsFake(callback => callback(new Error('connection error')));
            mongoLogReader.setup(err => {
                assert.strictEqual(err.message, 'connection error');
                assert(connectMongoStub.calledOnce);
                done();
            });
        });
    });

    describe('getLogInfo', () => {
        it('should return log info', () => {
            const logInfo = mongoLogReader.getLogInfo();
            assert.deepStrictEqual(logInfo, { logName: 'test-log' });
        });
    });

    describe('getMetricLabels', () => {
        it('should return metric labels', () => {
            const metricLabels = mongoLogReader.getMetricLabels();
            assert.deepStrictEqual(metricLabels, {
                logName: 'mongo-log',
                logId: 'test-log',
            });
        });
    });
});
