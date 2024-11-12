const assert = require('assert');
const sinon = require('sinon');
const { Logger } = require('werelogs');
const ZookeeperMock = require('zookeeper-mock');
const KafkaLogReader = require('../../../../lib/queuePopulator/KafkaLogReader');

class MockLogConsumer {
    constructor(params) {
        this.params = params || {};
    }

    setup(callback) {
        process.nextTick(() => {
            if (this.params.setupError) {
                callback(this.params.setupError);
            } else {
                callback(null);
            }
        });
    }
}

describe('KafkaLogReader', () => {
    let kafkaLogReader;
    let zkMock;
    let logger;

    beforeEach(() => {
        zkMock = new ZookeeperMock();
        logger = new Logger('test:KafkaLogReader');
        kafkaLogReader = new KafkaLogReader({
            zkClient: zkMock.createClient('localhost:2181'),
            kafkaConfig: { hosts: 'localhost:9092' },
            zkConfig: { connectionString: 'localhost:2181' },
            qpKafkaConfig: { logName: 'test-log' },
            logger,
            extensions: [],
            metricsHandler: {},
        });
        kafkaLogReader.logConsumer = new MockLogConsumer();
    });

    afterEach(() => {
        sinon.restore();
    });

    describe('constructor', () => {
        it('should initialize KafkaLogReader correctly', () => {
            assert(kafkaLogReader);
            assert(kafkaLogReader.logConsumer instanceof MockLogConsumer);
            assert.strictEqual(kafkaLogReader._kafkaConfig.hosts, 'localhost:9092');
            assert.strictEqual(kafkaLogReader._kafkaConfig.logName, 'test-log');
        });
    });

    describe('setup', () => {
        it('should setup log consumer successfully', done => {
            const setupStub = sinon.stub(kafkaLogReader.logConsumer, 'setup').callsFake(callback => callback(null));
            kafkaLogReader.setup(err => {
                assert.ifError(err);
                assert(setupStub.calledOnce);
                done();
            });
        });

        it('should handle log consumer setup error', done => {
            const setupStub = sinon.stub(kafkaLogReader.logConsumer, 'setup')
                .callsFake(callback => callback(new Error('setup error')));
            kafkaLogReader.setup(err => {
                assert.strictEqual(err.message, 'setup error');
                assert(setupStub.calledOnce);
                done();
            });
        });
    });

    describe('getLogInfo', () => {
        it('should return log info', () => {
            const logInfo = kafkaLogReader.getLogInfo();
            assert.deepStrictEqual(logInfo, { logName: 'test-log' });
        });
    });

    describe('getMetricLabels', () => {
        it('should return metric labels', () => {
            const metricLabels = kafkaLogReader.getMetricLabels();
            assert.deepStrictEqual(metricLabels, {
                logName: 'kafka-log',
                logId: 'test-log',
            });
        });
    });
});
