const assert = require('assert');
const werelogs = require('werelogs');
const sinon = require('sinon');

const NotificationConfigManager
    = require('../../../extensions/notification/NotificationConfigManager');
const NotificationQueuePopulator
    = require('../../../extensions/notification/NotificationQueuePopulator');
const mongoConfig
    = require('../../config.json').queuePopulator.mongo;
const notificationConfig
    = require('../../config.notification.json').extensions.notification;

const logger = new werelogs.Logger('NotificationConfigManager:test');

const config = {
    bucket: 'example-bucket',
    notificationConfiguration: {
        queueConfig: [
            {
                events: ['s3:ObjectCreated:Put'],
                queueArn: 'arn:scality:bucketnotif:::destination1',
                filterRules: [],
            },
            {
                events: ['s3:ObjectRemoved:Delete'],
                queueArn: 'arn:scality:bucketnotif:::destination2',
                filterRules: [],
            },
        ],
    },
};

describe('NotificationQueuePopulator ::', () => {
    let bnConfigManager;
    let notificationQueuePopulator;

    beforeEach(() => {
        bnConfigManager = new NotificationConfigManager({
            mongoConfig,
            bucketMetastore: '__metastore',
            maxCachedConfigs: 1000,
            logger,
        });
        notificationQueuePopulator = new NotificationQueuePopulator({
            config: notificationConfig,
            bnConfigManager,
            logger,
        });
        notificationQueuePopulator._metricsStore = {
            notifEvent: () => null,
        };
    });

    describe('_isBucketEntry ::', () => {
        it('should return true if entry is a bucket entry', done => {
            const isBucket =
                notificationQueuePopulator._isBucketEntry('__metastore', 'example-bucket');
            assert.strictEqual(isBucket, true);
            return done();
        });
        it('should return false if entry is an object entry', done => {
            const isBucket =
                notificationQueuePopulator._isBucketEntry('example-bucket', 'example-key');
            assert.strictEqual(isBucket, false);
            return done();
        });
    });

    describe('_processObjectEntry ::', () => {
        it('should publish object entry in notification topic of destination1', async () => {
            sinon.stub(bnConfigManager, 'getConfig').returns(config);
            const publishStub = sinon.stub(notificationQueuePopulator, 'publish');
            await notificationQueuePopulator._processObjectEntry(
                'example-bucket',
                'example-key',
                {
                    'originOp': 's3:ObjectCreated:Put',
                    'dataStoreName': 'metastore',
                    'content-length': '100',
                    'last-modified': '0000',
                    'md-model-version': '1',
                });
            assert.strictEqual(publishStub.getCall(0).args.at(0), 'internal-notification-topic-destination1');
        });

        it('should publish object entry in notification topic of destination2', async () => {
            sinon.stub(bnConfigManager, 'getConfig').returns(config);
            const publishStub = sinon.stub(notificationQueuePopulator, 'publish');
            await notificationQueuePopulator._processObjectEntry(
                'example-bucket',
                'example-key',
                {
                    'originOp': 's3:ObjectRemoved:Delete',
                    'dataStoreName': 'metastore',
                    'content-length': '100',
                    'last-modified': '0000',
                    'md-model-version': '1',
                });
                assert.strictEqual(publishStub.getCall(0).args.at(0), 'internal-notification-topic-destination2');
        });

        it('should not publish object entry in notification topic if ' +
            'config validation failed', async () => {
            sinon.stub(bnConfigManager, 'getConfig').returns(config);
            const publishStub = sinon.stub(notificationQueuePopulator, 'publish');
            await notificationQueuePopulator._processObjectEntry(
                'example-bucket',
                'example-key',
                {
                    'originOp': 's3:ObjectRemoved:DeleteMarkerCreated',
                    'dataStoreName': 'metastore',
                    'content-length': '100',
                    'versionId': '1234',
                    'last-modified': '0000',
                    'md-model-version': '1',
                });
            assert(publishStub.notCalled);
        });

        it('should publish object entry to internal shared topic only once ' +
            'when multiple destinations are valid for that event', async () => {
            // override the destinations' config to use the default shared topic
            notificationQueuePopulator.notificationConfig = {
                ...notificationConfig,
                destinations: notificationConfig.destinations.map(destination => ({
                    ...destination,
                    internalTopic: '',
                })),
            };
            sinon.stub(bnConfigManager, 'getConfig').returns({
                bucket: 'example-bucket',
                notificationConfiguration: {
                    queueConfig: [
                        {
                            events: ['s3:ObjectCreated:Put'],
                            queueArn: 'arn:scality:bucketnotif:::destination1',
                            filterRules: [],
                        },
                        {
                            events: ['s3:ObjectCreated:Put'],
                            queueArn: 'arn:scality:bucketnotif:::destination2',
                            filterRules: [],
                        }
                    ],
                },
            });
            const publishStub = sinon.stub(notificationQueuePopulator, 'publish');
            await notificationQueuePopulator._processObjectEntry(
                'example-bucket',
                'example-key',
                {
                    'originOp': 's3:ObjectCreated:Put',
                    'dataStoreName': 'metastore',
                    'content-length': '100',
                    'last-modified': '0000',
                    'md-model-version': '1',
                });
            assert(publishStub.calledOnce);
            assert.strictEqual(publishStub.getCall(0).args.at(0), 'backbeat-bucket-notification');
        });

        it('should publish object entry to same custom internal topic only once ' +
            'when multiple destinations are valid for that event', async () => {
            // override the destinations' config to use a single custom internal topic
            notificationQueuePopulator.notificationConfig = {
                ...notificationConfig,
                destinations: notificationConfig.destinations.map(destination => ({
                    ...destination,
                    internalTopic: 'custom-topic',
                })),
            };
            sinon.stub(bnConfigManager, 'getConfig').returns({
                bucket: 'example-bucket',
                notificationConfiguration: {
                    queueConfig: [
                        {
                            events: ['s3:ObjectCreated:Put'],
                            queueArn: 'arn:scality:bucketnotif:::destination1',
                            filterRules: [],
                        },
                        {
                            events: ['s3:ObjectCreated:Put'],
                            queueArn: 'arn:scality:bucketnotif:::destination2',
                            filterRules: [],
                        }
                    ],
                },
            });
            const publishStub = sinon.stub(notificationQueuePopulator, 'publish');
            await notificationQueuePopulator._processObjectEntry(
                'example-bucket',
                'example-key',
                {
                    'originOp': 's3:ObjectCreated:Put',
                    'dataStoreName': 'metastore',
                    'content-length': '100',
                    'last-modified': '0000',
                    'md-model-version': '1',
                });
            assert(publishStub.calledOnce);
            assert.strictEqual(publishStub.getCall(0).args.at(0), 'custom-topic');
        });

        it('should publish object entry to each entry\'s destination topic when multiple ' +
            'destinations are valid for an event', async () => {
            sinon.stub(bnConfigManager, 'getConfig').returns({
                bucket: 'example-bucket',
                notificationConfiguration: {
                    queueConfig: [
                        {
                            events: ['s3:ObjectCreated:Put'],
                            queueArn: 'arn:scality:bucketnotif:::destination1',
                            filterRules: [],
                        },
                        {
                            events: ['s3:ObjectCreated:Put'],
                            queueArn: 'arn:scality:bucketnotif:::destination2',
                            filterRules: [],
                        },
                        {
                            events: ['s3:ObjectCreated:Put'],
                            queueArn: 'arn:scality:bucketnotif:::destination3',
                            filterRules: [],
                        },
                        {
                            events: ['s3:ObjectCreated:Put'],
                            queueArn: 'arn:scality:bucketnotif:::destination4',
                            filterRules: [],
                        },
                    ],
                },
            });
            // override the destinations' config to add two new destinations that use
            // the default shared internal topic
            notificationQueuePopulator.notificationConfig = {
                ...notificationConfig,
                destinations: [
                    ...notificationConfig.destinations,
                    {
                        resource: 'destination3',
                        topic: 'destination-topic-3',
                    },
                    {
                        resource: 'destination4',
                        topic: 'destination-topic-4',
                    },
                ],
            };
            const publishStub = sinon.stub(notificationQueuePopulator, 'publish');
            await notificationQueuePopulator._processObjectEntry(
                'example-bucket',
                'example-key',
                {
                    'originOp': 's3:ObjectCreated:Put',
                    'dataStoreName': 'metastore',
                    'content-length': '100',
                    'last-modified': '0000',
                    'md-model-version': '1',
                });
            assert(publishStub.calledThrice);
            assert.strictEqual(publishStub.getCall(0).args.at(0), 'internal-notification-topic-destination1');
            assert.strictEqual(publishStub.getCall(1).args.at(0), 'internal-notification-topic-destination2');
            assert.strictEqual(publishStub.getCall(2).args.at(0), 'backbeat-bucket-notification');
        });

        it('should not publish object entry in notification topic if ' +
            'notification is non standard', async () => {
            sinon.stub(bnConfigManager, 'getConfig').returns({
                bucket: 'example-bucket',
                notificationConfiguration: {
                    queueConfig: [
                        {
                            events: ['s3:ObjectCreated:*'],
                            queueArn: 'arn:scality:bucketnotif:::destination1',
                            filterRules: [],
                        },
                    ],
                },
            });
            const publishStub = sinon.stub(notificationQueuePopulator, 'publish');
            await notificationQueuePopulator._processObjectEntry(
                'example-bucket',
                'example-key',
                {
                    'originOp': 's3:ObjectCreated:non-standard',
                    'dataStoreName': 'metastore',
                    'content-length': '100',
                    'last-modified': '0000',
                    'md-model-version': '1',
                });
            assert(publishStub.notCalled);
        });

        it('should use proper fields or S3C delete notification', async () => {
            sinon.stub(bnConfigManager, 'getConfig').returns(config);
            const publishStub = sinon.stub(notificationQueuePopulator, 'publish').returns();
            const timestamp = new Date().toISOString();
            await notificationQueuePopulator._processObjectEntry(
                'example-bucket',
                'example-key\x0098500086134471999999RG001  0',
                {},
                'del',
                {
                    versionId: '123456',
                    commitTimestamp: timestamp,
                }
            );
            const expectedMessage = {
                bucket: 'example-bucket',
                key: 'example-key',
                versionId: '123456',
                dateTime: timestamp,
                eventType: 's3:ObjectRemoved:Delete',
                region: null,
                schemaVersion: null,
                size: null,
            };
            assert(publishStub.calledOnce);
            assert.strictEqual(publishStub.getCall(0).args.at(0), 'internal-notification-topic-destination2');
            assert.strictEqual(publishStub.getCall(0).args.at(1), 'example-bucket/example-key');
            assert.deepEqual(JSON.parse(publishStub.getCall(0).args.at(2)), expectedMessage);
        });

        it('should use originOp from overheadFields for delete operation with lifecycle expiration', async () => {
            sinon.stub(bnConfigManager, 'getConfig').returns({
                bucket: 'example-bucket',
                notificationConfiguration: {
                    queueConfig: [
                        {
                            events: ['s3:ObjectRemoved:Delete', 's3:LifecycleExpiration:Delete'],
                            queueArn: 'arn:scality:bucketnotif:::destination1',
                            filterRules: [],
                        },
                    ],
                },
            });
            const publishStub = sinon.stub(notificationQueuePopulator, 'publish').returns();
            const timestamp = new Date().toISOString();
            await notificationQueuePopulator._processObjectEntry(
                'example-bucket',
                'example-key',
                {},
                'del',
                {
                    versionId: '123456',
                    commitTimestamp: timestamp,
                    originOp: 's3:LifecycleExpiration:Delete',
                }
            );
            const expectedMessage = {
                bucket: 'example-bucket',
                key: 'example-key',
                versionId: '123456',
                dateTime: timestamp,
                eventType: 's3:LifecycleExpiration:Delete',
                region: null,
                schemaVersion: null,
                size: null,
            };
            assert(publishStub.calledOnce);
            assert.strictEqual(publishStub.getCall(0).args.at(0), 'internal-notification-topic-destination1');
            assert.strictEqual(publishStub.getCall(0).args.at(1), 'example-bucket/example-key');
            assert.deepEqual(JSON.parse(publishStub.getCall(0).args.at(2)), expectedMessage);
        });
    });

    describe('filterAsync ::', () => {
        it('should fail if entry value parse fails', done => {
            const processEntryStub = sinon.stub(notificationQueuePopulator, '_processObjectEntry');
            const entry = {
                bucket: 'example-bucket',
                key: 'examlpe-key',
                type: 'put',
                value: '}{',
                overheadFields: {
                    opTimestamp: new Date().toISOString(),
                },
            };
            notificationQueuePopulator.filterAsync(entry, err => {
                assert.ifError(err);
                assert(processEntryStub.notCalled);
                return done();
            });
        });

        it('should ignore bucket operations', done => {
            const processObjectEntryStub = sinon.stub(notificationQueuePopulator, '_processObjectEntry');
            const processBucketEntryStub = sinon.stub(notificationQueuePopulator, '_processBucketEntry');
            const entry = {
                bucket: 'users..bucket',
                key: 'example-key',
                type: 'put',
                value: '{}',
                overheadFields: {
                    opTimestamp: new Date().toISOString(),
                },
            };
            notificationQueuePopulator.filterAsync(entry, err => {
                assert.ifError(err);
                assert(processObjectEntryStub.notCalled);
                assert(processBucketEntryStub.notCalled);
                return done();
            });
        });

        it('should ignore internal bucket operations', done => {
            const processObjectEntryStub = sinon.stub(notificationQueuePopulator, '_processObjectEntry');
            const processBucketEntryStub = sinon.stub(notificationQueuePopulator, '_processBucketEntry');
            const entry = {
                bucket: 'internal..backupIndex',
                key: 'example-key',
                type: 'put',
                value: '{}',
                overheadFields: {
                    opTimestamp: new Date().toISOString(),
                },
            };
            notificationQueuePopulator.filterAsync(entry, err => {
                assert.ifError(err);
                assert(processObjectEntryStub.notCalled);
                assert(processBucketEntryStub.notCalled);
                return done();
            });
        });

        it('should ignore mpu bucket operations', done => {
            const processObjectEntryStub = sinon.stub(notificationQueuePopulator, '_processObjectEntry');
            const processBucketEntryStub = sinon.stub(notificationQueuePopulator, '_processBucketEntry');
            const entry = {
                bucket: '__metastore',
                key: 'mpuShadowBucketexample-bucket',
                type: 'put',
                value: '{}',
                overheadFields: {
                    opTimestamp: new Date().toISOString(),
                },
            };
            notificationQueuePopulator.filterAsync(entry, err => {
                assert.ifError(err);
                assert(processObjectEntryStub.notCalled);
                assert(processBucketEntryStub.notCalled);
                return done();
            });
        });

        it('should updated config when a bucket entry contains notification configuration', done => {
            const processEntryStub = sinon.stub(notificationQueuePopulator, '_processObjectEntry').yields();
            const setConfigStub = sinon.stub(notificationQueuePopulator.bnConfigManager, 'setConfig').returns(true);
            const entry = {
                bucket: '__metastore',
                key: 'example-bucket',
                type: 'put',
                value: '{"attributes":"{\\"name\\":\\"example-bucket\\",\\"notificationConfiguration\\":' +
                    '{\\"queueConfig\\":[{\\"events\\":[\\"s3:ObjectCreated:*\\"],\\"queueArn\\":' +
                    '\\"arn:scality:bucketnotif:::notification-target\\"}]}}"}',
                overheadFields: {
                    opTimestamp: new Date().toISOString(),
                },
            };
            notificationQueuePopulator.filterAsync(entry, err => {
                assert.ifError(err);
                assert(processEntryStub.notCalled);
                assert(setConfigStub.calledWithMatch(
                    'example-bucket',
                    {
                        bucket: 'example-bucket',
                        notificationConfiguration: {
                            queueConfig: [
                                {
                                    events: ['s3:ObjectCreated:*'],
                                    queueArn: 'arn:scality:bucketnotif:::notification-target',
                                },
                            ],
                        },
                    },
                ));
                return done();
            });
        });

        it('remove config when bucket no longer has notification configured', done => {
            const processEntryStub = sinon.stub(notificationQueuePopulator, '_processObjectEntry').yields();
            const removeConfigStub = sinon.stub(notificationQueuePopulator.bnConfigManager, 'removeConfig')
                .returns(true);
            const entry = {
                bucket: '__metastore',
                key: 'example-bucket',
                type: 'put',
                value: '{"attributes":"{\\"name\\":\\"example-bucket\\"}"}',
                overheadFields: {
                    opTimestamp: new Date().toISOString(),
                },
            };
            notificationQueuePopulator.filterAsync(entry, err => {
                assert.ifError(err);
                assert(processEntryStub.notCalled);
                assert(removeConfigStub.calledWithMatch('example-bucket'));
                return done();
            });
        });

        it('should remove config whe bucket is deleted', done => {
            const processEntryStub = sinon.stub(notificationQueuePopulator, '_processObjectEntry').yields();
            const removeConfigStub = sinon.stub(notificationQueuePopulator.bnConfigManager, 'removeConfig')
                .returns(true);
            const entry = {
                bucket: '__metastore',
                key: 'example-bucket',
                type: 'del',
                value: undefined,
                overheadFields: {
                    opTimestamp: new Date().toISOString(),
                },
            };
            notificationQueuePopulator.filterAsync(entry, err => {
                assert.ifError(err);
                assert(processEntryStub.notCalled);
                assert(removeConfigStub.calledWithMatch('example-bucket'));
                return done();
            });
        });

        it('should process an object entry', done => {
            const processEntryCbStub = sinon.stub(notificationQueuePopulator, '_processObjectEntryCb')
                .yields();
            const entry = {
                bucket: 'example-bucket',
                key: 'example-key',
                type: 'put',
                value: '{}',
                overheadFields: {
                    opTimestamp: new Date().toISOString(),
                },
            };
            notificationQueuePopulator.filterAsync(entry, err => {
                assert.ifError(err);
                assert(processEntryCbStub.calledOnceWith(entry.bucket, entry.key, {}));
                return done();
            });
        });
    });

    describe('_getVersionId', () => {
        [
            {
                desc: 'non versioned',
                input: {},
                overhead: null,
                out: null
            },
            {
                desc: 'versioned',
                input: { versionId: '1234' },
                overhead: null,
                out: '1234'
            },
            {
                desc: 'versioned (S3C delete case)',
                input: {},
                overhead: { versionId: '1234' },
                out: '1234'
            },
            {
                desc: 'a null version',
                input: { isNull: true, versionId: '1234' },
                overhead: null,
                out: null
            },
        ].forEach(tests => {
            const { desc, input, overhead, out } = tests;
            it(`Should return ${out} when object is ${desc}`, () => {
                const versionId = notificationQueuePopulator._getVersionId(input, overhead);
                assert.strictEqual(versionId, out);
            });
        });
    });

    describe('_shouldProcessEntry ::', () => {
        [
            {
                desc: 'version',
                key: 'version-key\x001234',
                value: {
                    versionId: '1234',
                },
                out: true,
            },
            {
                desc: 'non versioned master',
                key: 'master-key',
                value: {},
                out: true,
            },
            {
                desc: 'null versioned master',
                key: 'master-key',
                value: {
                    versionId: '1234',
                    isNull: true,
                },
                out: true,
            },
            {
                desc: 'null versioned PHD master',
                key: 'master-key',
                value: {
                    versionId: '1234',
                    isNull: true,
                },
                out: true,
            },
            {
                desc: 'versioned master',
                key: 'master-key',
                value: {
                    versionId: '1234',
                },
                out: false,
            },
        ].forEach(params => {
            const { desc, key, value, out } = params;
            it(`Should return ${out} if ${desc}`, () => {
                const val = notificationQueuePopulator._shouldProcessEntry(key, value);
                assert.strictEqual(val, out);
            });
        });
    });

    describe('_processObjectEntryCb ::', () => {
        it('should properly throw an error if entry value parse fails', done => {
            notificationQueuePopulator._metricsStore = {
                notifEvent: sinon.stub().throws(new Error('Error processing entry')),
            };
            notificationQueuePopulator._processObjectEntryCb('example-bucket', 'example-key', {}, 'put', {}, err => {
                assert(err);
                assert.strictEqual(err.message, 'Error processing entry');
                return done();
            });
        });
    });
});


describe('NotificationQueuePopulator with multiple rules ::', () => {
    let bnConfigManager;
    let notificationQueuePopulator;

    beforeEach(() => {
        bnConfigManager = new NotificationConfigManager({
            mongoConfig,
            bucketMetastore: '__metastore',
            maxCachedConfigs: 1000,
            logger,
        });
        sinon.stub(bnConfigManager, 'getConfig').returns({
            bucket: 'example-bucket',
            notificationConfiguration: {
                queueConfig: [
                    {
                        events: ['s3:ObjectCreated:*'],
                        queueArn: 'arn:scality:bucketnotif:::destination1',
                        id: '0',
                        filterRules: [
                            {
                                name: 'Prefix',
                                value: 'toto/',
                            },
                        ],
                    }, {
                        events: ['s3:ObjectCreated:*'],
                        queueArn: 'arn:scality:bucketnotif:::destination1',
                        id: '1',
                        filterRules: [
                            {
                                name: 'Prefix',
                                value: 'tata/',
                            },
                        ],
                    },
                ],
            },
        });
        notificationQueuePopulator = new NotificationQueuePopulator({
            config: notificationConfig,
            bnConfigManager,
            logger,
        });
        notificationQueuePopulator._metricsStore = {
            notifEvent: () => null,
        };
    });

    describe('_processObjectEntry with multiple rules::', () => {
        it('should publish object entry if it matches the first rule', async () => {
            const publishStub = sinon.stub(notificationQueuePopulator, 'publish');
            await notificationQueuePopulator._processObjectEntry(
                'example-bucket',
                'toto/example-key',
                {
                    'key': 'toto/example-key',
                    'originOp': 's3:ObjectCreated:Put',
                    'dataStoreName': 'metastore',
                    'content-length': '100',
                    'last-modified': '0000',
                    'md-model-version': '1',
                });
            assert.strictEqual(publishStub.getCall(0).args.at(0), 'internal-notification-topic-destination1');
        });

        it('should publish object entry if it matches the second rule', async () => {
            const publishStub = sinon.stub(notificationQueuePopulator, 'publish');
            await notificationQueuePopulator._processObjectEntry(
                'example-bucket',
                'tata/example-key',
                {
                    'key': 'tata/example-key',
                    'originOp': 's3:ObjectCreated:Put',
                    'dataStoreName': 'metastore',
                    'content-length': '100',
                    'last-modified': '0000',
                    'md-model-version': '1',
                });
            assert.strictEqual(publishStub.getCall(0).args.at(0), 'internal-notification-topic-destination1');
        });

        it('should not publish object entry if it does not match any rule', async () => {
            const publishStub = sinon.stub(notificationQueuePopulator, 'publish');
            await notificationQueuePopulator._processObjectEntry(
                'example-bucket',
                'example-key',
                {
                    'key': 'example-key',
                    'originOp': 's3:ObjectCreated:Put',
                    'dataStoreName': 'metastore',
                    'content-length': '100',
                    'last-modified': '0000',
                    'md-model-version': '1',
                });
            sinon.assert.notCalled(publishStub);
        });
    });
});

describe('NotificationQueuePopulator with delivery pool ::', () => {
    const deliveryTopic = notificationConfig.deliveryPool.topic;
    const addressedConfig = {
        bucket: 'example-bucket',
        notificationConfiguration: {
            queueConfig: [
                {
                    events: ['s3:ObjectCreated:Put'],
                    queueArn: 'arn:scality:bucketnotif:::destination1',
                    id: 'config-1',
                    filterRules: [],
                },
                {
                    events: ['s3:ObjectCreated:Put'],
                    queueArn: 'arn:scality:bucketnotif:::destination2',
                    id: 'config-2',
                    filterRules: [],
                },
            ],
        },
    };
    const objectEntry = {
        'originOp': 's3:ObjectCreated:Put',
        'dataStoreName': 'metastore',
        'content-length': '100',
        'last-modified': '0000',
        'md-model-version': '1',
    };
    let bnConfigManager;
    let notificationQueuePopulator;

    beforeEach(() => {
        bnConfigManager = new NotificationConfigManager({
            mongoConfig,
            bucketMetastore: '__metastore',
            maxCachedConfigs: 1000,
            logger,
        });
        sinon.stub(bnConfigManager, 'getConfig').returns(addressedConfig);
        notificationQueuePopulator = new NotificationQueuePopulator({
            config: {
                ...notificationConfig,
                deliveryPool: {
                    ...notificationConfig.deliveryPool,
                    enabled: true,
                },
            },
            bnConfigManager,
            logger,
        });
        notificationQueuePopulator._metricsStore = {
            notifEvent: () => null,
        };
    });

    it('should publish one record per matching destination on the delivery topic', async () => {
        const publishStub = sinon.stub(notificationQueuePopulator, 'publish');
        await notificationQueuePopulator._processObjectEntry(
            'example-bucket',
            'example-key',
            objectEntry);
        assert(publishStub.calledTwice);
        assert.strictEqual(publishStub.getCall(0).args.at(0), deliveryTopic);
        assert.strictEqual(publishStub.getCall(1).args.at(0), deliveryTopic);
    });

    it('should use the destination resource as key when the spread factor is 1', async () => {
        const publishStub = sinon.stub(notificationQueuePopulator, 'publish');
        await notificationQueuePopulator._processObjectEntry(
            'example-bucket',
            'example-key',
            objectEntry);
        assert.strictEqual(publishStub.getCall(0).args.at(1), 'destination1');
        assert.strictEqual(publishStub.getCall(1).args.at(1), 'destination2');
    });

    it('should address each record with its destination and configuration', async () => {
        const publishStub = sinon.stub(notificationQueuePopulator, 'publish');
        await notificationQueuePopulator._processObjectEntry(
            'example-bucket',
            'example-key',
            objectEntry);
        const first = JSON.parse(publishStub.getCall(0).args.at(2));
        const second = JSON.parse(publishStub.getCall(1).args.at(2));
        assert.strictEqual(first.destinationId, 'destination1');
        assert.strictEqual(first.configurationId, 'config-1');
        assert.strictEqual(first.bucket, 'example-bucket');
        assert.strictEqual(first.key, 'example-key');
        assert.strictEqual(first.eventType, 's3:ObjectCreated:Put');
        assert.strictEqual(second.destinationId, 'destination2');
        assert.strictEqual(second.configurationId, 'config-2');
    });

    it('should publish one record per destination even when destinations ' +
        'share an internal topic', async () => {
        notificationQueuePopulator.notificationConfig = {
            ...notificationQueuePopulator.notificationConfig,
            destinations: notificationConfig.destinations.map(destination => ({
                ...destination,
                internalTopic: 'custom-topic',
            })),
        };
        const publishStub = sinon.stub(notificationQueuePopulator, 'publish');
        await notificationQueuePopulator._processObjectEntry(
            'example-bucket',
            'example-key',
            objectEntry);
        assert(publishStub.calledTwice);
        assert.strictEqual(publishStub.getCall(0).args.at(0), deliveryTopic);
        assert.strictEqual(publishStub.getCall(1).args.at(0), deliveryTopic);
        const destinationIds = [0, 1].map(call =>
            JSON.parse(publishStub.getCall(call).args.at(2)).destinationId);
        assert.deepStrictEqual(destinationIds, ['destination1', 'destination2']);
    });

    it('should spread a destination over its spread factor and keep the key ' +
        'stable for the same object', async () => {
        notificationQueuePopulator.notificationConfig = {
            ...notificationQueuePopulator.notificationConfig,
            destinations: notificationConfig.destinations.map(destination => ({
                ...destination,
                spreadFactor: 4,
            })),
        };
        const publishStub = sinon.stub(notificationQueuePopulator, 'publish');
        await notificationQueuePopulator._processObjectEntry(
            'example-bucket',
            'example-key',
            objectEntry);
        await notificationQueuePopulator._processObjectEntry(
            'example-bucket',
            'example-key',
            objectEntry);
        assert.strictEqual(publishStub.callCount, 4);
        const key = publishStub.getCall(0).args.at(1);
        const [resource, index] = key.split('|');
        assert.strictEqual(resource, 'destination1');
        assert(Number(index) >= 0 && Number(index) < 4);
        // the same object is always addressed to the same key, so the same
        // partition, so the same delivery worker
        assert.strictEqual(publishStub.getCall(2).args.at(1), key);
    });

    it('should not publish anything when no destination matches', async () => {
        bnConfigManager.getConfig.returns({
            bucket: 'example-bucket',
            notificationConfiguration: {
                queueConfig: [
                    {
                        events: ['s3:ObjectRemoved:Delete'],
                        queueArn: 'arn:scality:bucketnotif:::destination1',
                        id: 'config-1',
                        filterRules: [],
                    },
                ],
            },
        });
        const publishStub = sinon.stub(notificationQueuePopulator, 'publish');
        await notificationQueuePopulator._processObjectEntry(
            'example-bucket',
            'example-key',
            objectEntry);
        assert(publishStub.notCalled);
    });
});
