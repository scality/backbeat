const assert = require('assert');

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
    describe('::_getConsumerOptions', () => {
        it('should consume the status topic from the earliest offset', () => {
            const replicationStatusProcessor = new ReplicationStatusProcessor(
                { hosts: 'localhost:9092' },
                {
                    auth: {},
                    s3: {},
                    transport: 'http',
                },
                {
                    replicationStatusTopic: 'backbeat-replication-status',
                    replicationStatusProcessor: {
                        groupId: 'backbeat-replication-status-group',
                        concurrency: 10,
                        maxQueued: 1000,
                    },
                    objectSizeMetrics: [100, 1000],
                },
                {});
            const consumerOptions =
                replicationStatusProcessor._getConsumerOptions({});

            assert.strictEqual(consumerOptions.fromOffset, 'earliest');
            assert.strictEqual(consumerOptions.topic,
                'backbeat-replication-status');
            assert.strictEqual(consumerOptions.groupId,
                'backbeat-replication-status-group');
            assert.strictEqual(consumerOptions.bootstrap, false);
        });
    });
});
