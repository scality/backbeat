const assert = require('assert');
const sinon = require('sinon');
const werelogs = require('werelogs');
const { errors } = require('arsenal');
const kafka = require('node-rdkafka');
const logger = new werelogs.Logger('KafkaLogConsumer');
const ListRecordStream =
    require('../../../../../lib/queuePopulator/KafkaLogConsumer/ListRecordStream');
const LogConsumer =
    require('../../../../../lib/queuePopulator/KafkaLogConsumer/LogConsumer');

const kafkaConfig = {
    hosts: 'localhost:9092',
    topic: 'backbeat-oplog-topic',
    groupId: 'backbeat-oplog-group',
};

const changeStreamDocument = {
    ns: {
        db: 'metadata',
        coll: 'example-bucket',
    },
    documentKey: {
        _id: 'example-key',
    },
    operationType: 'insert',
    clusterTime: {
        $timestamp: {
            t: 1701270357,
            i: 1,
        },
    },
    fullDocument: {
        value: {
            field: 'value'
        }
    }
};

function getKafkaMessage(partition = 0, offset = 0) {
    return {
        value: Buffer.from(JSON.stringify(changeStreamDocument)),
        timestamp: Date.now(),
        size: 2,
        topic: 'oplog-topic',
        offset,
        partition,
        key: Buffer.from('key'),
    };
}

describe('LogConsumer', () => {
    let logConsumer;
    beforeEach(() => {
        logConsumer = new LogConsumer(kafkaConfig, logger);
        logConsumer._consumer = {
            offsetsStore: () => null,
        };
    });

    afterEach(() => {
        sinon.restore();
    });

    describe('_waitForAssignment', () => {
        it('should wait for consumer group to balance', done => {
            const waitAssignementSpy = sinon.spy(logConsumer, '_waitForAssignment');
            const getAssignemntsStub = sinon.stub();
            getAssignemntsStub.onCall(0).returns([]);
            getAssignemntsStub.onCall(1).returns([{
                topic: 'backbeat-oplog-topic',
                partition: 0,
            }]);
            logConsumer._consumer = {
                assignments: getAssignemntsStub,
            };
            logConsumer._waitForAssignment(0, () => {
                assert.strictEqual(waitAssignementSpy.getCall(1).args.at(0), 2000);
                return done();
            });
        }).timeout(5000);
    });

    describe('_resetRecordStream', () => {
        it('should initialize record stream', () => {
            logConsumer._resetRecordStream();
            assert(logConsumer._listRecordStream instanceof ListRecordStream);
            assert.strictEqual(typeof(logConsumer._listRecordStream.getOffset), 'function');
        });
    });

    describe('_consumeKafkaMessages', () => {
        it('should consume kafka messages', done => {
            const consumeStub = sinon.stub();
            const kafkaMessage = getKafkaMessage(0, 0);
            consumeStub.callsArgWith(1, null, [kafkaMessage]);
            logConsumer._consumer = {
                consume: consumeStub,
            };
            logConsumer._consumeKafkaMessages(1, err => {
                assert.ifError(err);
                assert.strictEqual(consumeStub.getCall(0).args.at(0), 1);
                logConsumer._listRecordStream.once('data', msg => {
                    assert.deepEqual(msg, {
                        timestamp: new Date(kafkaMessage.timestamp),
                        db: 'example-bucket',
                        entries: [{
                            key: 'example-key',
                            type: 'put',
                            value: JSON.stringify({
                                field: 'value'
                            }),
                            timestamp: '2023-11-29T15:05:57.000Z',
                        }],
                    });
                    return done();
                });
            });
        });

        it('should save next partition offsets', done => {
            const consumeStub = sinon.stub();
            consumeStub.callsArgWith(1, null, [
                getKafkaMessage(0, 10),
                getKafkaMessage(1, 20),
                getKafkaMessage(2, 30),
            ]);
            logConsumer._consumer = {
                consume: consumeStub,
            };
            logConsumer._consumeKafkaMessages(3, err => {
                assert.ifError(err);
                logConsumer._listRecordStream.once('data', () => {
                    assert.deepEqual(logConsumer._topicPartition, [
                        { topic: 'oplog-topic', partition: 0, offset: 11 },
                        { topic: 'oplog-topic', partition: 1, offset: 21 },
                        { topic: 'oplog-topic', partition: 2, offset: 31 },
                    ]);
                });
                return done();
            });
        });
    });

    describe('readRecords', () => {
        it('should return stream', done => {
            const waitAssignementStub = sinon.stub(logConsumer, '_waitForAssignment')
                .callsArg(1);
            const consumeKafkaStub = sinon.stub(logConsumer, '_consumeKafkaMessages')
                .callsArg(1);
            logConsumer._resetRecordStream();
            logConsumer.readRecords({ limit: 1 }, (err, res) => {
                assert(waitAssignementStub.called);
                assert(consumeKafkaStub.called);
                assert.ifError(err);
                assert(res.log instanceof ListRecordStream);
                assert.strictEqual(res.tailable, false);
                assert.strictEqual(typeof(res.log.getOffset), 'function');
                return done();
            });
        });

        it('should fail if consumer group failed to stabilize', done => {
            const waitAssignementStub = sinon.stub(logConsumer, '_waitForAssignment')
                .callsArgWith(1, errors.InternalError);
            logConsumer.readRecords({ limit: 1 }, err => {
                assert(waitAssignementStub.called);
                assert.deepEqual(err, errors.InternalError);
                return done();
            });
        });

        it('should fail if it can\'t consume kafka messages', done => {
            const waitAssignementStub = sinon.stub(logConsumer, '_waitForAssignment')
                .callsArg(1);
            const consumeKafkaStub = sinon.stub(logConsumer, '_consumeKafkaMessages')
                .callsArgWith(1, errors.InternalError);
            logConsumer.readRecords({ limit: 1 }, err => {
                assert(waitAssignementStub.called);
                assert(consumeKafkaStub.called);
                assert.deepEqual(err, errors.InternalError);
                return done();
            });
        });
    });

    describe('storeOffsets', () => {
        it('should not store offsets if there are none', () => {
            const offsetsStore = sinon.stub(logConsumer._consumer, 'offsetsStore').returns(null);
            logConsumer.storeOffsets();
            assert(offsetsStore.notCalled);
        });
        it('should reset topicPartition after storing offsets', () => {
            const offsetsStore = sinon.stub(logConsumer._consumer, 'offsetsStore').returns(null);
            logConsumer._topicPartition = [{ topic: 'oplog-topic', partition: 0, offset: 1 }];
            logConsumer.storeOffsets();
            assert(offsetsStore.calledWithMatch([{ topic: 'oplog-topic', partition: 0, offset: 1 }]));
            assert.strictEqual(logConsumer._topicPartition, null);
        });
    });

    describe('_getOffset', () => {
        it('should return null', () => {
            const result = logConsumer._getOffset();
            assert.strictEqual(result, null);
        });
    });

    describe('_onOffsetCommit', () => {
        it('should not log an error if it receives a NO_OFFSET error', () => {
            const logErrorSpy = sinon.spy(logConsumer._log, 'error');
            logConsumer._onOffsetCommit({ code: kafka.CODES.ERRORS.ERR__NO_OFFSET }, null);
            assert(logErrorSpy.notCalled);
        });
        it('should log an error if it receives an error other than NO_OFFSET', () => {
            const logErrorSpy = sinon.spy(logConsumer._log, 'error');
            const error = { code: kafka.CODES.ERRORS.ERR__UNKNOWN_TOPIC };
            logConsumer._onOffsetCommit(error, null);
            assert(logErrorSpy.calledOnce);
        });
        it('should log debug message on successful commit', () => {
            const logDebugSpy = sinon.spy(logConsumer._log, 'debug');
            const topicPartitions = [{ topic: 'oplog-topic', partition: 0, offset: 1 }];
            logConsumer._onOffsetCommit(null, topicPartitions);
            assert(logDebugSpy.calledOnce);
        });
    });
});
