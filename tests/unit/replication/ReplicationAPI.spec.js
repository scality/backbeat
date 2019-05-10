const assert = require('assert');

const ReplicationAPI =
      require('../../../extensions/replication/ReplicationAPI');

describe('ReplicationAPI', () => {
    it('should create a "copyLocation" action with createCopyLocationAction()',
    () => {
        const action = ReplicationAPI.createCopyLocationAction({
            bucketName: 'somebucket',
            objectKey: 'someobject',
            versionId: 'someversion',
            eTag: 'someetag',
            lastModified: '2019-05-09T17:36:05.302Z',
            toLocation: 'aws',
            originLabel: 'replication-api-test',
            fromLocation: 'local',
            contentLength: 1000,
            resultsTopic: 'test-results-topic',
        });
        assert.strictEqual(action.getActionType(), 'copyLocation');
        assert.strictEqual(action.getResultsTopic(), 'test-results-topic');
        assert.deepStrictEqual(action.getAttribute('target'), {
            bucket: 'somebucket',
            eTag: 'someetag',
            key: 'someobject',
            lastModified: '2019-05-09T17:36:05.302Z',
            version: 'someversion',
        });
        assert.strictEqual(action.getAttribute('toLocation'), 'aws');
    });

    it('should return data mover topic name with getDataMoverTopic()', () => {
        assert.strictEqual(ReplicationAPI.getDataMoverTopic(),
                           'backbeat-data-mover');
    });

    it('should set the data mover topic name with setDataMoverTopic()', () => {
        ReplicationAPI.setDataMoverTopic('new-topic');
        assert.strictEqual(ReplicationAPI.getDataMoverTopic(), 'new-topic');
    });

    it('should convert consumer group name to location name with ' +
    'consumerGroupToLocation()', () => {
        assert.strictEqual(ReplicationAPI.consumerGroupToLocation(
            'backbeat-replication-group-foobar'), 'foobar');
        assert.strictEqual(ReplicationAPI.consumerGroupToLocation(
            'backbeat-bad-replication-group-foobar'), null);
    });

    it('should convert location name to consumer group name with ' +
    'locationToConsumerGroup()', () => {
        assert.strictEqual(ReplicationAPI.locationToConsumerGroup(
            'foobar'), 'backbeat-replication-group-foobar');
    });

    it('should return a regular expression matching all consumer group' +
    'with getConsumerGroupRegexp()', () => {
        assert.strictEqual(ReplicationAPI.getConsumerGroupRegexp(),
                           'backbeat-replication-group-.*');
    });
});
