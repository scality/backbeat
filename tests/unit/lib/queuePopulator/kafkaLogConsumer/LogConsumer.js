const assert = require('assert');
const sinon = require('sinon');
const werelogs = require('werelogs');
const { errors } = require('arsenal');
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
    fullDocument: {
        value: {
            field: 'value'
        }
    }
};
const kafkaMessage = {
    value: Buffer.from(JSON.stringify(changeStreamDocument)),
    timestamp: Date.now(),
    size: 2,
    topic: 'oplog-topic',
    offset: 1337,
    partition: 0,
    key: Buffer.from('key'),
};

describe('LogConsumer', () => {
    let logConsumer;
    beforeEach(() => {
        logConsumer = new LogConsumer(kafkaConfig, logger);
    });

    describe('_waitForAssignment', () => {
        it('Should wait for consumer group to balance', done => {
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

        it('Should timeout', done => {
            const getAssignemntsStub = sinon.stub();
            getAssignemntsStub.returns([]);
            logConsumer._consumer = {
                assignments: getAssignemntsStub,
            };
            logConsumer._waitForAssignment(120001, err => {
                assert.strict(err, true);
                return done();
            });
        }).timeout(5000);
    });

    describe('_storeCurrentOffsets', () => {
        it('Should store offsets', done => {
            const committedStub = sinon.stub();
            committedStub.callsArgWith(1, null, [{
                topic: 'backbeat-oplog-topic',
                partition: 0,
                offset: 0
            }]);
            logConsumer._consumer = {
                committed: committedStub,
            };
            logConsumer._storeCurrentOffsets(err => {
                assert.ifError(err);
                assert.deepEqual(logConsumer._offsets, [{
                    topic: 'backbeat-oplog-topic',
                    partition: 0,
                    offset: 0
                }]);
                return done();
            });
        });
    });

    describe('_resetRecordStream', () => {
        it('Should initialize record stream', () => {
            logConsumer._resetRecordStream();
            assert(logConsumer._listRecordStream instanceof ListRecordStream);
            assert.strictEqual(typeof(logConsumer._listRecordStream.getOffset), 'function');
        });
    });

    describe('_consumeKafkaMessages', () => {
        it('Should consume kafka messages', done => {
            const consumeStub = sinon.stub();
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
                        }],
                    });
                    return done();
                });
            });
        });
    });

    describe('readRecords', () => {
        it('Should return stream', done => {
            const waitAssignementStub = sinon.stub(logConsumer, '_waitForAssignment')
                .callsArg(1);
            const storeOffsetsStub = sinon.stub(logConsumer, '_storeCurrentOffsets')
                .callsArg(0);
            const consumeKafkaStub = sinon.stub(logConsumer, '_consumeKafkaMessages')
                .callsArg(1);
            logConsumer._resetRecordStream();
            logConsumer.readRecords({ limit: 1 }, (err, res) => {
                assert(waitAssignementStub.called);
                assert(storeOffsetsStub.called);
                assert(consumeKafkaStub.called);
                assert.ifError(err);
                assert(res.log instanceof ListRecordStream);
                assert.strictEqual(res.tailable, false);
                assert.strictEqual(typeof(res.log.getOffset), 'function');
                return done();
            });
        });

        it('Should fail if consumer group failed to stabilize', done => {
            const waitAssignementStub = sinon.stub(logConsumer, '_waitForAssignment')
                .callsArgWith(1, errors.InternalError);
            logConsumer.readRecords({ limit: 1 }, err => {
                assert(waitAssignementStub.called);
                assert.deepEqual(err, errors.InternalError);
                return done();
            });
        });

        it('Should fail if it can\'t store offsets', done => {
            const waitAssignementStub = sinon.stub(logConsumer, '_waitForAssignment')
                .callsArg(1);
            const storeOffsetsStub = sinon.stub(logConsumer, '_storeCurrentOffsets')
                .callsArgWith(0, errors.InternalError);
            logConsumer.readRecords({ limit: 1 }, err => {
                assert(waitAssignementStub.called);
                assert(storeOffsetsStub.called);
                assert.deepEqual(err, errors.InternalError);
                return done();
            });
        });

        it('Should fail if it can\'t consume kafka messages', done => {
            const waitAssignementStub = sinon.stub(logConsumer, '_waitForAssignment')
                .callsArg(1);
            const storeOffsetsStub = sinon.stub(logConsumer, '_storeCurrentOffsets')
                .callsArg(0);
            const consumeKafkaStub = sinon.stub(logConsumer, '_consumeKafkaMessages')
                .callsArgWith(1, errors.InternalError);
            logConsumer.readRecords({ limit: 1 }, err => {
                assert(waitAssignementStub.called);
                assert(storeOffsetsStub.called);
                assert(consumeKafkaStub.called);
                assert.deepEqual(err, errors.InternalError);
                return done();
            });
        });
    });
});
