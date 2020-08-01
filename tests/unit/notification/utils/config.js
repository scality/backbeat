const assert = require('assert');
const notifConfigUtil
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
                    queueArn: 'q3',
                    filter: {},
                    id: 'config4',
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
        desc: 'pass if the object name prefix matches the configuration',
        entry: {
            type: 's3:ObjectCreated:Put',
            bucket: 'bucket1',
            key: 'test.png',
        },
        pass: true,
    },
    {
        desc: 'pass if the object name suffix matches the configuration',
        entry: {
            type: 's3:ObjectCreated:Copy',
            bucket: 'bucket1',
            key: 'test.png',
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
        desc: 'fail if the object name does not match configuration prefix',
        entry: {
            type: 's3:ObjectCreated:Put',
            bucket: 'bucket1',
            key: 'one.png',
        },
        pass: false,
    },
    {
        desc: 'fail if the object name does not match configuration suffix',
        entry: {
            type: 's3:ObjectCreated:Copy',
            bucket: 'bucket1',
            key: 'test.jpg',
        },
        pass: false,
    },
];

describe('Notification configuration util', () => {
    describe('Transform', () => {
        it('Should transform an array of configs to a Map', () => {
            const configMap = notifConfigUtil.transform(testConfigs);
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

    describe('validateEntry', () => {
        const configMap = notifConfigUtil.transform(testConfigs);

        tests.forEach(test => {
            it(`should ${test.desc}`, () => {
                const result
                    = notifConfigUtil.validateEntry(configMap, test.entry);
                assert.strictEqual(test.pass, result);
            });
        });
    });
});
