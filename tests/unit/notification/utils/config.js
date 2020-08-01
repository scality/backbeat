const assert = require('assert');
const notifConfUtil
    = require('../../../../extensions/notification/utils/config');

const testConfigs = [
    {
        bucket: 'bucket1',
        notificationConfiguration: {
            queueConfigurations: [
                {
                    events: ['s3:ObjectCreated:Put'],
                    queueArn: 'q1',
                    filter: {
                        filterRules: [
                            {
                                name: 'prefix',
                                value: 'test',
                            },
                        ],
                    },
                    id: 'config1',
                },
            ],
        },
    },
    {
        bucket: 'bucket2',
        notificationConfiguration: {
            queueConfigurations: [
                {
                    events: ['s3:ObjectRemoved:Delete'],
                    queueArn: 'q2',
                    filter: {
                        filterRules: [
                            {
                                name: 'prefix',
                                value: 'abcd',
                            },
                        ],
                    },
                    id: 'config2',
                },
            ],
        },
    },
    {
        bucket: 'bucket1',
        notificationConfiguration: {
            queueConfigurations: [
                {
                    events: ['s3:ObjectCreated:Copy'],
                    queueArn: 'q3',
                    filter: {
                        filterRules: [
                            {
                                name: 'suffix',
                                value: '.png',
                            },
                        ],
                    },
                    id: 'config3',
                },
            ],
        },
    },
    {
        bucket: 'bucket3',
        notificationConfiguration: {
            queueConfigurations: [
                {
                    events: ['s3:ObjectCreated:Copy'],
                    queueArn: 'q4',
                    filter: {},
                    id: 'config4',
                },
            ],
        },
    },
    {
        bucket: 'bucket4',
        notificationConfiguration: {
            queueConfigurations: [
                {
                    events: ['s3:ObjectCreated:Copy'],
                    queueArn: 'q5',
                    filterRules: [
                        {
                            name: 'prefix',
                            value: 'abcd',
                        },
                        {
                            name: 'suffix',
                            value: '.png',
                        },
                    ],
                    id: 'config5',
                },
            ],
        },
    },
];

const tests = [
    {
        desc: 'pass if the event matches a bucket notification configuration',
        entry: {
            type: 's3:ObjectCreated:Copy',
            bucket: 'bucket3',
            key: 'test.png',
        },
        pass: true,
    },
    {
        desc: 'pass if the object key prefix matches the configuration',
        entry: {
            type: 's3:ObjectCreated:Put',
            bucket: 'bucket1',
            key: 'test.png',
        },
        pass: true,
    },
    {
        desc: 'pass if the object key suffix matches the configuration',
        entry: {
            type: 's3:ObjectCreated:Copy',
            bucket: 'bucket1',
            key: 'test.png',
        },
        pass: true,
    },
    {
        desc: 'pass if object key prefix & suffix matches the configuration',
        entry: {
            type: 's3:ObjectCreated:Copy',
            bucket: 'bucket4',
            key: 'abcdef.png',
        },
        pass: true,
    },
    {
        desc: 'fail if the object bucket has no configuration setup',
        entry: {
            type: 's3:ObjectCreated:Put',
            bucket: 'bucket10',
            key: 'test.png',
        },
        pass: false,
    },
    {
        desc: 'fail if the event type does not match the configuration',
        entry: {
            type: 's3:ObjectCreated:Post',
            bucket: 'bucket1',
            key: 'test.png',
        },
        pass: false,
    },
    {
        desc: 'fail if the object key does not match configuration prefix',
        entry: {
            type: 's3:ObjectCreated:Put',
            bucket: 'bucket1',
            key: 'one.png',
        },
        pass: false,
    },
    {
        desc: 'fail if the object key does not match configuration suffix',
        entry: {
            type: 's3:ObjectCreated:Copy',
            bucket: 'bucket1',
            key: 'test.jpg',
        },
        pass: false,
    },
    {
        desc: 'fail if only key prefix matches the configuration',
        entry: {
            type: 's3:ObjectCreated:Copy',
            bucket: 'bucket4',
            key: 'abcdef.jpg',
        },
        pass: true,
    },
    {
        desc: 'fail if only key suffix matches the configuration',
        entry: {
            type: 's3:ObjectCreated:Copy',
            bucket: 'bucket4',
            key: 'abc.png',
        },
        pass: true,
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
                const currentConfig = configMap.get(bucket);
                const bucketConfig
                    = testConfigs.filter(c => c.bucket === bucket);
                assert.strictEqual(bucketConfig.length, currentConfig.length);
            });
        });
    });

    describe('ValidateEntry', () => {
        const configMap = notifConfUtil.configArrayToMap(testConfigs);
        tests.forEach(test => {
            it(`should ${test.desc}`, () => {
                const result
                    = notifConfUtil.validateEntry(configMap, test.entry);
                assert.strictEqual(test.pass, result);
            });
        });
    });
});
