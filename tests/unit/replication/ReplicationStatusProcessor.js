const assert = require('assert');
const promClient = require('prom-client');
const sinon = require('sinon');

const constants = require('../../../lib/constants');
const ReplicationStatusProcessor =
    require('../../../extensions/replication/replicationStatusProcessor/ReplicationStatusProcessor');

function makeReplicationStatusProcessor(replayTopics) {
    return new ReplicationStatusProcessor(
        { hosts: 'localhost:9092' },
        {
            auth: {},
            s3: {},
            transport: 'http',
        },
        { replayTopics, objectSizeMetrics: [100, 1000] },
        {});
}

describe('ReplicationStatusProcessor', () => {
    beforeEach(() => {
        // Clear register to avoid:
        // Error: A metric with the name kafka_lag has already been registered.
        promClient.register.clear();
    });

    describe('::_reshapeReplayTopics', () => {
        it('should return undefined if the config replay topics is undefined', () => {
            const replayTopics = undefined;
            const replicationStatusProcessor = makeReplicationStatusProcessor(replayTopics);
            const stateVars = replicationStatusProcessor.getStateVars();

            assert.strictEqual(stateVars.replayTopics, undefined);
        });

        it('should return undefined if the config replay topics is empty array', () => {
            const replayTopics = [];
            const replicationStatusProcessor = makeReplicationStatusProcessor(replayTopics);
            const stateVars = replicationStatusProcessor.getStateVars();

            assert.strictEqual(stateVars.replayTopics, undefined);
        });

        it('should return an array of five items if the config replay topic has five retries', () => {
            const replayTopics = [
                {
                    topicName: 'backbeat-replication-replay-0',
                    retries: 5,
                },
            ];
            const replicationStatusProcessor = makeReplicationStatusProcessor(replayTopics);
            const stateVars = replicationStatusProcessor.getStateVars();

            assert.deepStrictEqual(stateVars.replayTopics, [
                'backbeat-replication-replay-0',
                'backbeat-replication-replay-0',
                'backbeat-replication-replay-0',
                'backbeat-replication-replay-0',
                'backbeat-replication-replay-0',
            ]);
        });

        it('should return an array of seven items if the config replay topic has seven retries', () => {
            const replayTopics = [
                {
                    topicName: 'backbeat-replication-replay-0',
                    retries: 5,
                },
                {
                    topicName: 'backbeat-replication-replay-1',
                    retries: 2,
                },
            ];
            const replicationStatusProcessor = makeReplicationStatusProcessor(replayTopics);
            const stateVars = replicationStatusProcessor.getStateVars();

            assert.deepStrictEqual(stateVars.replayTopics, [
                'backbeat-replication-replay-1',
                'backbeat-replication-replay-1',
                'backbeat-replication-replay-0',
                'backbeat-replication-replay-0',
                'backbeat-replication-replay-0',
                'backbeat-replication-replay-0',
                'backbeat-replication-replay-0',
            ]);
        });
    });

    describe('::_makeTopicNames', () => {
        it('should return an empty array if the config replay topics is undefined', () => {
            const replayTopics = undefined;
            const replicationStatusProcessor = makeReplicationStatusProcessor(replayTopics);
            const stateVars = replicationStatusProcessor.getStateVars();

            assert.deepStrictEqual(stateVars.replayTopicNames, []);
        });

        it('should return an empty array if the config replay topics is an empty array', () => {
            const replayTopics = [];
            const replicationStatusProcessor = makeReplicationStatusProcessor(replayTopics);
            const stateVars = replicationStatusProcessor.getStateVars();

            assert.deepStrictEqual(stateVars.replayTopicNames, []);
        });

        it('should return an array of one item if one topic name', () => {
            const replayTopics = [
                {
                    topicName: 'backbeat-replication-replay-0',
                    retries: 5,
                },
            ];
            const replicationStatusProcessor = makeReplicationStatusProcessor(replayTopics);
            const stateVars = replicationStatusProcessor.getStateVars();

            assert.deepStrictEqual(stateVars.replayTopicNames, [
                'backbeat-replication-replay-0',
            ]);
        });

        it('should return an array of two items if two topic names', () => {
            const replayTopics = [
                {
                    topicName: 'backbeat-replication-replay-0',
                    retries: 5,
                },
                {
                    topicName: 'backbeat-replication-replay-1',
                    retries: 2,
                },
            ];
            const replicationStatusProcessor = makeReplicationStatusProcessor(replayTopics);
            const stateVars = replicationStatusProcessor.getStateVars();

            assert.deepStrictEqual(stateVars.replayTopicNames, [
                'backbeat-replication-replay-0',
                'backbeat-replication-replay-1',
            ]);
        });
    });
});

describe('ReplicationStatusProcessor: handlers', () => {
    let processor;
    let mockResponse;
    let logger;
    const topicName = 'backbeat-replication-replay-0';
    const replayTopics = [
        {
            topicName,
            retries: 5,
        },
    ];

    beforeEach(() => {
        // Clear register to avoid:
        // Error: A metric with the name kafka_lag has already been registered.
        promClient.register.clear();

        processor = makeReplicationStatusProcessor(replayTopics);
        mockResponse = {
            writeHead: sinon.stub(),
            end: sinon.stub(),
        };
        logger = {
            debug: sinon.stub(),
            error: sinon.stub(),
        };

        sinon.stub(promClient.register, 'metrics').resolves('metrics_data');
    });

    afterEach(() => {
        sinon.restore();
    });

    describe('handleLiveness', () => {
        it('should respond with 200 when all components are healthy', () => {
            processor._consumer = { _consumerReady: true };
            processor._FailedCRRProducer = { _producer: { isReady: () => true } };
            processor._ReplayProducers = {
                [topicName]: { _producer: { isReady: () => true } },
            };

            processor.handleLiveness(mockResponse, logger);

            assert(mockResponse.writeHead.calledWith(200));
            assert(mockResponse.end.calledOnce);
        });

        it('should respond with 500 when consumer is not ready', () => {
            processor._consumer = { _consumerReady: false };
            processor._FailedCRRProducer = { _producer: { isReady: () => true } };
            processor._ReplayProducers = {
                [topicName]: { _producer: { isReady: () => true } },
            };

            processor.handleLiveness(mockResponse, logger);

            sinon.assert.calledOnceWithExactly(mockResponse.writeHead, 500);

            const expectedRes = [{
                component: 'Consumer',
                status: constants.statusNotReady,
            }];
            sinon.assert.calledOnceWithExactly(mockResponse.end, JSON.stringify(expectedRes));

            const expectedLogError = {
                httpCode: 500,
                error: expectedRes,
            };
            sinon.assert.calledOnceWithExactly(logger.error, 'sending back error response', expectedLogError);
        });

        it('should respond with 500 when failed CRR Producer is not ready', () => {
            processor._consumer = { _consumerReady: true };
            processor._FailedCRRProducer = { _producer: { isReady: () => false } };
            processor._ReplayProducers = {
                [topicName]: { _producer: { isReady: () => true } },
            };

            processor.handleLiveness(mockResponse, logger);

            sinon.assert.calledOnceWithExactly(mockResponse.writeHead, 500);

            const expectedRes = [{
                component: 'Failed CRR Producer',
                status: constants.statusNotReady,
            }];
            sinon.assert.calledOnceWithExactly(mockResponse.end, JSON.stringify(expectedRes));

            const expectedLogError = {
                httpCode: 500,
                error: expectedRes,
            };
            sinon.assert.calledOnceWithExactly(logger.error, 'sending back error response', expectedLogError);
        });

        it('should respond with 500 when Replay Producers is not ready', () => {
            processor._consumer = { _consumerReady: true };
            processor._FailedCRRProducer = { _producer: { isReady: () => true } };
            processor._ReplayProducers = {
                [topicName]: { _producer: { isReady: () => false } },
            };

            processor.handleLiveness(mockResponse, logger);

            sinon.assert.calledOnceWithExactly(mockResponse.writeHead, 500);

            const expectedRes = [{
                component: 'Replay CRR Producer to backbeat-replication-replay-0',
                status: constants.statusNotReady,
            }];
            sinon.assert.calledOnceWithExactly(mockResponse.end, JSON.stringify(expectedRes));

            const expectedLogError = {
                httpCode: 500,
                error: expectedRes,
            };
            sinon.assert.calledOnceWithExactly(logger.error, 'sending back error response', expectedLogError);
        });
    });

    describe('handleMetrics', () => {
        it('should properly handle metrics response', async () => {
            const r = await processor.handleMetrics(mockResponse, logger);
            assert.strictEqual(r, undefined);

            sinon.assert.calledOnce(promClient.register.metrics);

            sinon.assert.calledOnceWithExactly(mockResponse.writeHead, 200, {
                'Content-Type': promClient.register.contentType,
            });

            sinon.assert.calledOnceWithExactly(mockResponse.end, 'metrics_data');
        });
    });
});
