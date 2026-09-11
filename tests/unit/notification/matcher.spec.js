const assert = require('assert');

const { destinationOfArn, matchDestinations } =
    require('../../../extensions/notification/utils/matcher');

const BUCKET = 'photos';

function queueConfig(id, destinationId, events, filterRules) {
    const config = {
        id,
        queueArn: `arn:scality:bucketnotif:::${destinationId}`,
        events,
    };
    if (filterRules) {
        config.filterRules = filterRules;
    }
    return config;
}

function bucketConfig(queueConfigs) {
    return {
        bucket: BUCKET,
        notificationConfiguration: { queueConfig: queueConfigs },
    };
}

const putEntry = {
    bucket: BUCKET,
    key: 'logs/2026/a.txt',
    eventType: 's3:ObjectCreated:Put',
};

describe('notification utils matcher', () => {
    it('should take the last ARN segment as the destination', () => {
        assert.strictEqual(destinationOfArn('arn:scality:bucketnotif:::dest-1'),
            'dest-1');
        assert.strictEqual(destinationOfArn(
            'arn:scality:bucketnotif::123456789012:dest-2'), 'dest-2');
    });

    it('should match nothing without a configuration', () => {
        assert.deepStrictEqual(matchDestinations({
            bucketConfig: null, entry: putEntry, isServed: () => true,
        }), []);
        assert.deepStrictEqual(matchDestinations({
            bucketConfig: {}, entry: putEntry, isServed: () => true,
        }), []);
        assert.deepStrictEqual(matchDestinations({
            bucketConfig: bucketConfig([]), entry: putEntry, isServed: () => true,
        }), []);
    });

    it('should return every destination the event matches, in order', () => {
        const matches = matchDestinations({
            bucketConfig: bucketConfig([
                queueConfig('all-to-a', 'dest-a', ['s3:ObjectCreated:*']),
                queueConfig('puts-to-b', 'dest-b', ['s3:ObjectCreated:Put']),
                queueConfig('deletes-to-c', 'dest-c', ['s3:ObjectRemoved:*']),
            ]),
            entry: putEntry,
            isServed: () => true,
        });
        assert.deepStrictEqual(matches, [
            { destinationId: 'dest-a', configurationId: 'all-to-a' },
            { destinationId: 'dest-b', configurationId: 'puts-to-b' },
        ]);
    });

    it('should apply filter rules per destination', () => {
        const matches = matchDestinations({
            bucketConfig: bucketConfig([
                queueConfig('logs-to-a', 'dest-a', ['s3:ObjectCreated:*'],
                    [{ name: 'Prefix', value: 'logs/' }]),
                queueConfig('images-to-b', 'dest-b', ['s3:ObjectCreated:*'],
                    [{ name: 'Prefix', value: 'images/' }]),
            ]),
            entry: putEntry,
            isServed: () => true,
        });
        assert.deepStrictEqual(matches, [
            { destinationId: 'dest-a', configurationId: 'logs-to-a' },
        ]);
    });

    it('should leave out destinations the caller does not serve', () => {
        const served = new Set(['dest-b']);
        const matches = matchDestinations({
            bucketConfig: bucketConfig([
                queueConfig('all-to-a', 'dest-a', ['s3:ObjectCreated:*']),
                queueConfig('all-to-b', 'dest-b', ['s3:ObjectCreated:*']),
            ]),
            entry: putEntry,
            isServed: destinationId => served.has(destinationId),
        });
        assert.deepStrictEqual(matches, [
            { destinationId: 'dest-b', configurationId: 'all-to-b' },
        ]);
    });

    it('should match a destination once when several rules name it', () => {
        const matches = matchDestinations({
            bucketConfig: bucketConfig([
                queueConfig('deletes-to-a', 'dest-a', ['s3:ObjectRemoved:*']),
                queueConfig('puts-to-a', 'dest-a', ['s3:ObjectCreated:Put']),
                queueConfig('all-to-a', 'dest-a', ['s3:ObjectCreated:*']),
            ]),
            entry: putEntry,
            isServed: () => true,
        });
        assert.strictEqual(matches.length, 1);
        assert.strictEqual(matches[0].destinationId, 'dest-a');
        // the first rule of that destination the event satisfies
        assert.strictEqual(matches[0].configurationId, 'puts-to-a');
    });

    it('should match nothing for an entry without an event type', () => {
        const matches = matchDestinations({
            bucketConfig: bucketConfig([
                queueConfig('all-to-a', 'dest-a', ['s3:ObjectCreated:*']),
            ]),
            entry: { bucket: BUCKET, key: 'k' },
            isServed: () => true,
        });
        assert.deepStrictEqual(matches, []);
    });
});
