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

const notificationConfiguration = {
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
};

describe('NotificationQueuePopulator ::', () => {
    let bnConfigManager;
    let notificationQueuePopulator;

    beforeEach(() => {
        bnConfigManager = new NotificationConfigManager({
            mongoConfig,
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
            sinon.stub(bnConfigManager, 'getConfig').returns(notificationConfiguration);
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
            sinon.stub(bnConfigManager, 'getConfig').returns(notificationConfiguration);
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
            sinon.stub(bnConfigManager, 'getConfig').returns(notificationConfiguration);
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
                queueConfig: [
                    {
                        events: ['s3:ObjectCreated:*'],
                        queueArn: 'arn:scality:bucketnotif:::destination1',
                        filterRules: [],
                    },
                ],
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

        it('should fail if entry is a bucket entry', done => {
            const processEntryStub = sinon.stub(notificationQueuePopulator, '_processObjectEntry');
            const entry = {
                bucket: '__metastore',
                key: 'example-bucket',
                type: 'put',
                value: '{}',
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

        it('should process the entry', done => {
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
                out: null
            },
            {
                desc: 'versioned',
                input: { versionId: '1234' },
                out: '1234'
            },
            {
                desc: 'a null version',
                input: { isNull: true, versionId: '1234' },
                out: null
            },
        ].forEach(tests => {
            const { desc, input, out } = tests;
            it(`Should return ${out} when object is ${desc}`, () => {
                const versionId = notificationQueuePopulator._getVersionId(input);
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
});

describe('NotificationQueuePopulator with multiple rules ::', () => {
    let bnConfigManager;
    let notificationQueuePopulator;

    beforeEach(() => {
        bnConfigManager = new NotificationConfigManager({
            mongoConfig,
            logger,
        });
        sinon.stub(bnConfigManager, 'getConfig').returns({
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
