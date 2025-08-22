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
                isConnected: () => true,
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

        it('should skip consuming when the consumer is not ready', done => {
            const consumeStub = sinon.stub();
            logConsumer._consumer = {
                consume: consumeStub,
                isConnected: () => false,
            };
            logConsumer._consumeKafkaMessages(5, err => {
                assert.ifError(err);
                sinon.assert.notCalled(consumeStub);
                return done();
            });
        });

        it('should skip consuming when previously consumed batch was not processed yet', done => {
            const consumeStub = sinon.stub();
            logConsumer._consumer = {
                consume: consumeStub,
                isConnected: () => true,
            };
            logConsumer._topicPartition = [{ topic: 'oplog-topic', partition: 0, offset: 1 }];
            logConsumer._consumeKafkaMessages(5, err => {
                assert.ifError(err);
                sinon.assert.notCalled(consumeStub);
                return done();
            });
        });

        it('should store topicPartitions correctly after consuming messages with duplicate partitions', done => {
            const consumeStub = sinon.stub();
            consumeStub.callsArgWith(1, null, [
                getKafkaMessage(0, 5),
                getKafkaMessage(1, 10),
                getKafkaMessage(0, 8),
                getKafkaMessage(2, 15),
                getKafkaMessage(1, 12),
            ]);
            logConsumer._consumer = {
                consume: consumeStub,
                isConnected: () => true,
            };
            logConsumer._consumeKafkaMessages(5, err => {
                assert.ifError(err);
                assert.deepEqual(logConsumer._topicPartition, [
                    { topic: 'oplog-topic', partition: 0, offset: 9 },
                    { topic: 'oplog-topic', partition: 1, offset: 13 },
                    { topic: 'oplog-topic', partition: 2, offset: 16 },
                ]);
                return done();
            });
        });

        it('should store empty topicPartitions when no messages are consumed', done => {
            const consumeStub = sinon.stub();
            consumeStub.callsArgWith(1, null, []);
            logConsumer._consumer = {
                consume: consumeStub,
                isConnected: () => true,
            };
            logConsumer._consumeKafkaMessages(10, err => {
                assert.ifError(err);
                sinon.assert.calledOnce(consumeStub);
                assert.deepEqual(logConsumer._topicPartition, []);
                return done();
            });
        });

        it('should handle consumer errors gracefully and not store topicPartitions', done => {
            const consumeStub = sinon.stub();
            const consumerError = new Error('Consumer error');
            consumeStub.callsArgWith(1, consumerError);
            logConsumer._consumer = {
                consume: consumeStub,
                isConnected: () => true,
            };
            logConsumer._consumeKafkaMessages(5, err => {
                assert.ifError(err);
                sinon.assert.calledOnce(consumeStub);
                assert.strictEqual(logConsumer._topicPartition, null);
                return done();
            });
        });
    });

    describe('readRecords', () => {
        it('should return stream', done => {
            const consumeKafkaStub = sinon.stub(logConsumer, '_consumeKafkaMessages')
                .callsArg(1);
            logConsumer._resetRecordStream();
            logConsumer.readRecords({ limit: 1 }, (err, res) => {
                assert(consumeKafkaStub.called);
                assert.ifError(err);
                assert(res.log instanceof ListRecordStream);
                assert.strictEqual(res.tailable, false);
                assert.strictEqual(typeof(res.log.getOffset), 'function');
                return done();
            });
        });

        it('should fail if it can\'t consume kafka messages', done => {
            const consumeKafkaStub = sinon.stub(logConsumer, '_consumeKafkaMessages')
                .callsArgWith(1, errors.InternalError);
            logConsumer.readRecords({ limit: 1 }, err => {
                assert(consumeKafkaStub.called);
                assert.deepEqual(err, errors.InternalError);
                return done();
            });
        });
    });

    describe('storeOffsets', () => {
        it('should not call offsetsStore when topicPartition is undefined', () => {
            const offsetsStore = sinon.stub(logConsumer._consumer, 'offsetsStore').returns(null);
            logConsumer._topicPartition = undefined;
            
            logConsumer.storeOffsets();
            
            sinon.assert.notCalled(offsetsStore);
            assert.strictEqual(logConsumer._pendingCommit, false);
        });

        it('should not store offsets if topicPartition is empty array', () => {
            const offsetsStore = sinon.stub(logConsumer._consumer, 'offsetsStore').returns(null);
            logConsumer._topicPartition = [];
            logConsumer.storeOffsets();
            assert(offsetsStore.notCalled);
            assert.strictEqual(logConsumer._pendingCommit, false);
        });

        it('should reset topicPartition after storing offsets', () => {
            const offsetsStore = sinon.stub(logConsumer._consumer, 'offsetsStore').returns(null);
            const topicPartition = [{ topic: 'oplog-topic', partition: 0, offset: 1 }];
            logConsumer._topicPartition = topicPartition;
            logConsumer.storeOffsets();
            assert(offsetsStore.calledWithMatch(topicPartition));
            assert.strictEqual(logConsumer._topicPartition, null);
            assert.strictEqual(logConsumer._pendingCommit, true);
        });

        it('should store multiple partition offsets correctly', () => {
            const offsetsStore = sinon.stub(logConsumer._consumer, 'offsetsStore').returns(null);
            const topicPartition = [
                { topic: 'oplog-topic', partition: 0, offset: 10 },
                { topic: 'oplog-topic', partition: 1, offset: 20 },
                { topic: 'oplog-topic', partition: 2, offset: 30 },
            ];
            logConsumer._topicPartition = topicPartition;
            logConsumer.storeOffsets();
            
            sinon.assert.calledOnce(offsetsStore);
            sinon.assert.calledWith(offsetsStore, topicPartition);
            assert.strictEqual(logConsumer._topicPartition, null);
            assert.strictEqual(logConsumer._pendingCommit, true);
        });

        it('should set pendingCommit to true when storing offsets', () => {
            const offsetsStore = sinon.stub(logConsumer._consumer, 'offsetsStore').returns(null);
            logConsumer._topicPartition = [{ topic: 'oplog-topic', partition: 0, offset: 5 }];
            logConsumer._pendingCommit = false;
            
            logConsumer.storeOffsets();
            
            assert.strictEqual(logConsumer._pendingCommit, true);
            sinon.assert.calledOnce(offsetsStore);
        });
    });

    describe('_getOffset', () => {
        it('should return null', () => {
            const result = logConsumer._getOffset();
            assert.strictEqual(result, null);
        });
    });

    describe('_onOffsetCommit', () => {
        it('should not log error and not change pendingCommit on NO_OFFSET error', () => {
            const logErrorSpy = sinon.spy(logConsumer._log, 'error');
            const logDebugSpy = sinon.spy(logConsumer._log, 'debug');
            logConsumer._pendingCommit = true;
            
            const result = logConsumer._onOffsetCommit({ code: kafka.CODES.ERRORS.ERR__NO_OFFSET }, null);
            
            sinon.assert.notCalled(logErrorSpy);
            sinon.assert.notCalled(logDebugSpy);
            assert.strictEqual(logConsumer._pendingCommit, true);
            assert.strictEqual(result, undefined);
        });

        it('should log error and not change pendingCommit on non-NO_OFFSET errors', () => {
            const logErrorSpy = sinon.spy(logConsumer._log, 'error');
            const logDebugSpy = sinon.spy(logConsumer._log, 'debug');
            const error = { code: kafka.CODES.ERRORS.ERR__UNKNOWN_TOPIC };
            const topicPartitions = [{ topic: 'test-topic', partition: 1, offset: 5 }];
            logConsumer._pendingCommit = true;
            
            const result = logConsumer._onOffsetCommit(error, topicPartitions);
            
            sinon.assert.calledOnce(logErrorSpy);
            sinon.assert.notCalled(logDebugSpy);
            assert.strictEqual(logConsumer._pendingCommit, true);
            assert.strictEqual(result, undefined);
        });

        it('should set pendingCommit to false on successful commit', () => {
            const topicPartitions = [
                { topic: 'oplog-topic', partition: 0, offset: 10 },
                { topic: 'oplog-topic', partition: 1, offset: 20 }
            ];
            logConsumer._pendingCommit = true;
            
            const result = logConsumer._onOffsetCommit(null, topicPartitions);
            
            assert.strictEqual(logConsumer._pendingCommit, false);
            assert.strictEqual(result, undefined);
        });
    });

    describe('_onRebalance', () => {
        beforeEach(() => {
            logConsumer._topic = 'test-topic';
            logConsumer._consumerGroupId = 'test-group';
            logConsumer._maxPollIntervalMs = 300000;
        });

        describe('partition assignment (ERR__ASSIGN_PARTITIONS)', () => {
            it('should assign partitions on successful assignment', () => {
                const assignStub = sinon.stub();
                const assignment = [{ topic: 'test-topic', partition: 0 }];
                logConsumer._consumer = {
                    assign: assignStub,
                };

                logConsumer._onRebalance({ code: kafka.CODES.ERRORS.ERR__ASSIGN_PARTITIONS }, assignment);

                sinon.assert.calledOnce(assignStub);
                sinon.assert.calledWith(assignStub, assignment);
            });
        });

        describe('partition revocation (ERR__REVOKE_PARTITIONS)', () => {
            let clock;

            beforeEach(() => {
                clock = sinon.useFakeTimers();
            });

            afterEach(() => {
                clock.restore();
            });

            it('should immediately unassign when no pending messages or commits', done => {
                const unassignStub = sinon.stub();
                const assignment = [{ topic: 'test-topic', partition: 0 }];
                logConsumer._consumer = {
                    unassign: unassignStub,
                    isConnected: () => true,
                };
                logConsumer._topicPartition = [];
                logConsumer._pendingCommit = false;

                logConsumer.once('unassigned', () => {
                    sinon.assert.calledOnce(unassignStub);
                    done();
                });

                logConsumer._onRebalance({ code: kafka.CODES.ERRORS.ERR__REVOKE_PARTITIONS }, assignment);
                clock.tick(1000);
            });

            it('should wait for pending messages to be processed and committed before unassigning', done => {
                const unassignStub = sinon.stub();
                const commitStub = sinon.stub();
                logConsumer._consumer = {
                    unassign: unassignStub,
                    commit: commitStub,
                    isConnected: () => true,
                };
                logConsumer._topicPartition = [{ topic: 'test', partition: 0, offset: 1 }];
                logConsumer._pendingCommit = false;

                logConsumer._onRebalance({ code: kafka.CODES.ERRORS.ERR__REVOKE_PARTITIONS }, []);

                // First tick - should not unassign yet due to pending messages
                clock.tick(1000);
                sinon.assert.notCalled(unassignStub);

                // simulate end of processing
                logConsumer._topicPartition = null;
                logConsumer._pendingCommit = true;

                // second tick - should not unassign yet due to pending commit
                clock.tick(1000);
                sinon.assert.notCalled(unassignStub);

                // simulate a successful commit
                logConsumer._pendingCommit = false;

                logConsumer.once('unassigned', () => {
                    sinon.assert.calledOnce(commitStub);
                    sinon.assert.calledOnce(unassignStub);
                    done();
                });

                // Third tick - should now unassign
                clock.tick(1000);
            });

            it('should disconnect consumer on timeout', () => {
                const logErrorSpy = sinon.spy(logConsumer._log, 'error');
                const disconnectStub = sinon.stub();
                logConsumer._consumer = {
                    disconnect: disconnectStub,
                    isConnected: () => true,
                };
                logConsumer._maxPollIntervalMs = 5000;
                logConsumer._topicPartition = [{ topic: 'test', partition: 0, offset: 1 }];

                logConsumer._onRebalance({ code: kafka.CODES.ERRORS.ERR__REVOKE_PARTITIONS }, []);

                // Advance time to trigger timeout
                clock.tick(4000);

                sinon.assert.calledOnce(logErrorSpy);
                sinon.assert.calledOnce(disconnectStub);
            });
        });

        describe('unknown error handling', () => {
            it('should log error for unknown rebalance errors', () => {
                const logErrorSpy = sinon.spy(logConsumer._log, 'error');
                const unknownError = { code: kafka.CODES.ERRORS.ERR__UNKNOWN };
                const assignment = [{ topic: 'test-topic', partition: 0 }];

                logConsumer._onRebalance(unknownError, assignment);

                sinon.assert.calledOnce(logErrorSpy);
            });
        });
    });

    describe('isReady', () => {
        it('should return true when consumer is connected', () => {
            logConsumer._consumer = {
                isConnected: () => true,
            };

            assert.strictEqual(logConsumer.isReady(), true);
        });

        it('should return false when consumer is not connected', () => {
            logConsumer._consumer = {
                isConnected: () => false,
            };
            
            assert.strictEqual(logConsumer.isReady(), false);
        });
    });

    describe('close', () => {
        it('should close consumer that is connected and subscribed', done => {
            const unsubscribeStub = sinon.stub();
            const disconnectStub = sinon.stub();
            logConsumer._consumer = {
                isConnected: () => true,
                subscription: () => ['test-topic'],
                unsubscribe: unsubscribeStub,
                disconnect: disconnectStub,
                once: sinon.stub(),
            };
            
            logConsumer._consumer.once.withArgs('disconnected').callsArg(1);
            
            logConsumer.close(err => {
                assert.ifError(err);
                sinon.assert.calledOnce(unsubscribeStub);
                sinon.assert.calledOnce(disconnectStub);
                sinon.assert.calledWith(logConsumer._consumer.once, 'disconnected');
                return done();
            });
            
            setImmediate(() => {
                logConsumer.emit('unassigned');
            });
        });

        it('should close consumer that is connected but not subscribed', done => {
            const disconnectStub = sinon.stub();
            logConsumer._consumer = {
                isConnected: () => true,
                subscription: () => [],
                disconnect: disconnectStub,
                once: sinon.stub(),
            };
            
            logConsumer._consumer.once.withArgs('disconnected').callsArg(1);
            
            logConsumer.close(err => {
                assert.ifError(err);
                sinon.assert.calledOnce(disconnectStub);
                sinon.assert.calledWith(logConsumer._consumer.once, 'disconnected');
                return done();
            });
        });

        it('should handle case when consumer is not connected', done => {
            logConsumer._consumer = {
                isConnected: () => false,
            };
            
            logConsumer.close(err => {
                assert.ifError(err);
                return done();
            });
        });

        it('should handle case when consumer is null', done => {
            logConsumer._consumer = null;
            
            logConsumer.close(err => {
                assert.ifError(err);
                return done();
            });
        });
    });
});
