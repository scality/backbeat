const assert = require('assert');
const notifConfUtil
    = require('../../../../extensions/notification/utils/config');

const testConfigs = [
    {
        bucket: 'bucket1',
        notificationConfiguration: {
            queueConfig: [
                {
                    events: ['s3:ObjectCreated:Put'],
                    queueArn: 'q1',
                    filterRules: [],
                    id: 'config1',
                },
            ],
        },
    },
    {
        bucket: 'bucket2',
        notificationConfiguration: {
            queueConfig: [
                {
                    events: ['s3:ObjectRemoved:Delete'],
                    queueArn: 'q2',
                    filterRules: [
                        {
                            name: 'Prefix',
                            value: 'abcd',
                        },
                    ],
                    id: 'config2',
                },
            ],
        },
    },
    {
        bucket: 'bucket3',
        notificationConfiguration: {
            queueConfig: [
                {
                    events: ['s3:ObjectRemoved:DeleteMarkerCreated'],
                    queueArn: 'q3',
                    filterRules: [
                        {
                            name: 'Suffix',
                            value: '.png',
                        },
                    ],
                    id: 'config3',
                },
            ],
        },
    },
    {
        bucket: 'bucket4',
        notificationConfiguration: {
            queueConfig: [
                {
                    events: ['s3:ObjectCreated:Put'],
                    queueArn: 'q4',
                    filterRules: [
                        {
                            name: 'Prefix',
                            value: 'abcd',
                        },
                    ],
                    id: 'config4',
                },
                {
                    events: ['s3:ObjectRemoved:Delete'],
                    queueArn: 'q4',
                    filterRules: [
                        {
                            name: 'Suffix',
                            value: '.png',
                        },
                    ],
                    id: 'config4.1',
                },
            ],
        },
    },
    {
        bucket: 'bucket5',
        notificationConfiguration: {
            queueConfig: [
                {
                    events: ['s3:ObjectCreated:CompleteMultipartUpload'],
                    queueArn: 'q5',
                    filterRules: [
                        {
                            name: 'Prefix',
                            value: 'abcd',
                        },
                        {
                            name: 'Suffix',
                            value: '.png',
                        },
                    ],
                    id: 'config5',
                },
            ],
        },
    },
    {
        bucket: 'bucket6',
        notificationConfiguration: {
            queueConfig: [
                {
                    events: ['s3:ObjectCreated:Copy'],
                    queueArn: 'q61',
                    id: 'config61',
                },
                {
                    events: ['s3:ObjectCreated:Copy'],
                    queueArn: 'q62',
                    filterRules: [
                        {
                            name: 'Prefix',
                            value: 'abcd',
                        },
                        {
                            name: 'Suffix',
                            value: '.png',
                        },
                    ],
                    id: 'config62',
                },
            ],
        },
    },
    {
        bucket: 'bucket7',
        notificationConfiguration: {
            queueConfig: [
                {
                    events: ['s3:ObjectCreated:*'],
                    queueArn: 'q7',
                    filterRules: [],
                    id: 'config7',
                },
            ],
        },
    },
    {
        bucket: 'bucket8',
        notificationConfiguration: {
            queueConfig: [
                {
                    events: ['s3:ObjectRemoved:Delete'],
                    queueArn: 'q8',
                    filterRules: [],
                    id: 'config8',
                },
            ],
        },
    },
    {
        bucket: 'bucket9',
        notificationConfiguration: {
            queueConfig: [
                {
                    events: ['s3:ObjectCreated:Put'],
                    queueArn: 'q9',
                    filterRules: [
                        {
                            name: 'Prefix',
                            value: '0833/epolicy/',
                        },
                    ],
                    id: 'config9',
                },
                {
                    events: ['s3:ObjectCreated:Put'],
                    queueArn: 'q9',
                    filterRules: [
                        {
                            name: 'Prefix',
                            value: '0394/ars/',
                        },
                    ],
                    id: 'config9.1',
                },
            ],
        },
    },
];

const tests = [
    {
        desc: 'pass if the event matches a bucket notification configuration',
        entry: {
            eventType: 's3:ObjectCreated:Put',
            bucket: 'bucket1',
            key: 'test.png',
        },
        pass: true,
        expectedMatchingConfig: {
            events: ['s3:ObjectCreated:Put'],
            queueArn: 'q1',
            filterRules: [],
            id: 'config1',
        },
    },
    {
        desc: 'pass if the object key prefix matches the configuration',
        entry: {
            eventType: 's3:ObjectRemoved:Delete',
            bucket: 'bucket2',
            key: 'abcd.png',
        },
        pass: true,
        expectedMatchingConfig: {
            events: ['s3:ObjectRemoved:Delete'],
            queueArn: 'q2',
            filterRules: [
                {
                    name: 'Prefix',
                    value: 'abcd',
                },
            ],
            id: 'config2',
        },
    },
    {
        desc: 'pass if the object key suffix matches the configuration',
        entry: {
            eventType: 's3:ObjectRemoved:DeleteMarkerCreated',
            bucket: 'bucket3',
            key: 'test.png',
        },
        pass: true,
        expectedMatchingConfig: {
            events: ['s3:ObjectRemoved:DeleteMarkerCreated'],
            queueArn: 'q3',
            filterRules: [
                {
                    name: 'Suffix',
                    value: '.png',
                },
            ],
            id: 'config3',
        },
    },
    {
        desc: 'pass if the object key prefix matches the first of the configuration',
        entry: {
            eventType: 's3:ObjectCreated:Put',
            bucket: 'bucket4',
            key: 'abcdefgh',
        },
        pass: true,
        expectedMatchingConfig: {
            events: ['s3:ObjectCreated:Put'],
            queueArn: 'q4',
            filterRules: [
                {
                    name: 'Prefix',
                    value: 'abcd',
                },
            ],
            id: 'config4',
        },
    },
    {
        desc: 'pass if the object key suffix matches the second of the configuration',
        entry: {
            eventType: 's3:ObjectRemoved:Delete',
            bucket: 'bucket4',
            key: 'toto.png',
        },
        pass: true,
        expectedMatchingConfig: {
            events: ['s3:ObjectRemoved:Delete'],
            queueArn: 'q4',
            filterRules: [
                {
                    name: 'Suffix',
                    value: '.png',
                },
            ],
            id: 'config4.1',
        },
    },
    {
        desc: 'pass if the object key matches both configuration rules',
        entry: {
            eventType: 's3:ObjectRemoved:Delete',
            bucket: 'bucket4',
            key: 'abcd.png',
        },
        pass: true,
        expectedMatchingConfig: {
            events: ['s3:ObjectRemoved:Delete'],
            queueArn: 'q4',
            filterRules: [
                {
                    name: 'Suffix',
                    value: '.png',
                },
            ],
            id: 'config4.1',
        },
    },
    {
        desc: 'pass if object key prefix & suffix matches the configuration',
        entry: {
            eventType: 's3:ObjectCreated:CompleteMultipartUpload',
            bucket: 'bucket5',
            key: 'abcdef.png',
        },
        pass: true,
        expectedMatchingConfig: {
            events: ['s3:ObjectCreated:CompleteMultipartUpload'],
            queueArn: 'q5',
            filterRules: [
                {
                    name: 'Prefix',
                    value: 'abcd',
                },
                {
                    name: 'Suffix',
                    value: '.png',
                },
            ],
            id: 'config5',
        },
    },
    {
        desc: 'pass if object passes at least one notification configuration',
        entry: {
            eventType: 's3:ObjectCreated:Copy',
            bucket: 'bucket6',
            key: 'test.jpg',
        },
        pass: true,
        expectedMatchingConfig: {
            events: ['s3:ObjectCreated:Copy'],
            queueArn: 'q61',
            id: 'config61',
        },
    },
    {
        desc: 'pass if the event matches wildcard event',
        entry: {
            eventType: 's3:ObjectCreated:Post',
            bucket: 'bucket7',
            key: 'abcd.png',
        },
        pass: true,
        expectedMatchingConfig: {
            events: ['s3:ObjectCreated:*'],
            queueArn: 'q7',
            filterRules: [],
            id: 'config7',
        },
    },
    {
        desc: 'pass if match the first configuration',
        entry: {
            eventType: 's3:ObjectCreated:Put',
            bucket: 'bucket9',
            key: '0833/epolicy/1',
        },
        pass: true,
        expectedMatchingConfig: {
            events: ['s3:ObjectCreated:Put'],
            queueArn: 'q9',
            filterRules: [{
                name: 'Prefix',
                value: '0833/epolicy/',
            }],
            id: 'config9',
        },
    },
    {
        desc: 'pass if match the second configuration',
        entry: {
            eventType: 's3:ObjectCreated:Put',
            bucket: 'bucket9',
            key: '0394/ars/1',
        },
        pass: true,
        expectedMatchingConfig: {
            events: ['s3:ObjectCreated:Put'],
            queueArn: 'q9',
            filterRules: [{
                name: 'Prefix',
                value: '0394/ars/',
            }],
            id: 'config9.1',
        },
    },
    {
        desc: 'fail if the event type does not match the configuration',
        entry: {
            eventType: 's3:ObjectCreated:Post',
            bucket: 'bucket1',
            key: 'test.png',
        },
        pass: false,
    },
    {
        desc: 'fail if the event type does not match config',
        entry: {
            eventType: 's3:ObjectRemoved:DeleteMarkerCreated',
            bucket: 'bucket8',
            key: 'abcd.png',
        },
        pass: false,
    },
    {
        desc: 'fail if the object key does not match configuration prefix',
        entry: {
            eventType: 's3:ObjectRemoved:Delete',
            bucket: 'bucket2',
            key: 'one.png',
        },
        pass: false,
    },
    {
        desc: 'fail if the object key does not match configuration suffix',
        entry: {
            eventType: 's3:ObjectRemoved:DeleteMarkerCreated',
            bucket: 'bucket3',
            key: 'test.jpg',
        },
        pass: false,
    },
    {
        desc: 'fail if only key prefix matches the configuration',
        entry: {
            eventType: 's3:ObjectCreated:CompleteMultipartUpload',
            bucket: 'bucket5',
            key: 'abcdef.jpg',
        },
        pass: false,
    },
    {
        desc: 'fail if only key suffix matches the configuration',
        entry: {
            eventType: 's3:ObjectCreated:CompleteMultipartUpload',
            bucket: 'bucket5',
            key: 'abc.png',
        },
        pass: false,
    },
    {
        desc: 'fail if object passes no notification configuration filter',
        entry: {
            eventType: 's3:ObjectCreated:Post',
            bucket: 'bucket6',
            key: 'abcd.png',
        },
        pass: false,
    },
    {
        desc: 'fail if the event does not match the wildcard event',
        entry: {
            eventType: 's3:ObjectRemoved:Post',
            bucket: 'bucket7',
            key: 'abcd.png',
        },
        pass: false,
    },
    {
        desc: 'fail if the event type is unavailable',
        entry: {
            eventType: undefined,
            bucket: 'bucket8',
            key: 'abcd.png',
        },
        pass: false,
    },
    {
        desc: 'fail if key does not match any configuration',
        entry: {
            eventType: undefined,
            bucket: 'bucket9',
            key: 'abcd.png',
        },
        pass: false,
    },
];

describe('Notification configuration util', () => {
    describe('ConfigArrayToMap', () => {
        it('should transform an array of configs to a Map', () => {
            const configMap = notifConfUtil.configArrayToMap(testConfigs);
            assert(configMap instanceof Map);
            testConfigs.forEach(config => {
                const bucket = config.bucket;
                assert(configMap.has(bucket));
            });
        });
    });

    describe('ValidateEntry', () => {
        let configMap = null;
        function getBucketNotifConfig(bucket, bnConfigMap) {
            const bnConfigs
                = bnConfigMap.get(bucket);
            return {
                bucket,
                notificationConfiguration: {
                    queueConfig: bnConfigs,
                },
            };
        }

        before(() => {
            configMap = notifConfUtil.configArrayToMap(testConfigs);
        });

        tests.forEach(test => {
            it(`should ${test.desc}`, () => {
                const bnConfig
                    = getBucketNotifConfig(test.entry.bucket, configMap);
                const result
                    = notifConfUtil.validateEntry(bnConfig, test.entry);
                assert.strictEqual(test.pass, result.isValid);
                if (test.pass) {
                    assert.deepStrictEqual(test.expectedMatchingConfig, result.matchingConfig);
                } else {
                    assert(!result.matchingConfig);
                }
            });
        });
    });
});
