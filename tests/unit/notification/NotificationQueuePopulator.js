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
            events: ['s3:ObjectCreated:*'],
            queueArn: 'arn:scality:bucketnotif:::destination1',
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
        sinon.stub(bnConfigManager, 'getConfig').returns(notificationConfiguration);
        notificationQueuePopulator = new NotificationQueuePopulator({
            config: notificationConfig,
            bnConfigManager,
            logger,
        });
    });

    describe('_isBucketEntry ::', () => {
        it('Should return true if entry is a bucket entry', done => {
            const isBucket =
                notificationQueuePopulator._isBucketEntry('__metastore', 'example-bucket');
            assert.strictEqual(isBucket, true);
            return done();
        });
        it('Should return false if entry is an object entry', done => {
            const isBucket =
                notificationQueuePopulator._isBucketEntry('example-bucket', 'example-key');
            assert.strictEqual(isBucket, false);
            return done();
        });
    });

    describe('_processObjectEntry ::', () => {
        it('Should publish object entry in notification topic', async () => {
            const publishStub = sinon.stub(notificationQueuePopulator, 'publish');
            await notificationQueuePopulator._processObjectEntry(
                'example-bucket',
                'example-key',
                {
                    'originOp': 's3:ObjectCreated:Put',
                    'dataStoreName': 'metastore',
                    'content-length': '100',
                    'versionId': '1234',
                    'last-modified': '0000',
                    'md-model-version': '1',
                },
                'put');
            assert(publishStub.calledOnce);
        });

        it('Should not publish object entry in notification topic if ' +
            'config validation failed', async () => {
            const publishStub = sinon.stub(notificationQueuePopulator, 'publish');
            await notificationQueuePopulator._processObjectEntry(
                'example-bucket',
                'example-key',
                {
                    'originOp': 's3:ObjectRemoved:Delete',
                    'dataStoreName': 'metastore',
                    'content-length': '100',
                    'versionId': '1234',
                    'last-modified': '0000',
                    'md-model-version': '1',
                },
                'put');
            assert(publishStub.notCalled);
        });

        it('Should not publish object entry in notification topic if ' +
            'entry is a version event', async () => {
            const publishStub = sinon.stub(notificationQueuePopulator, 'publish');
            await notificationQueuePopulator._processObjectEntry(
                'example-bucket',
                'example-key\x001234',
                {
                    'originOp': 's3:ObjectCreated:Put',
                    'dataStoreName': 'metastore',
                    'content-length': '100',
                    'versionId': '1234',
                    'last-modified': '0000',
                    'md-model-version': '1',
                },
                'put');
            assert(publishStub.notCalled);
        });
    });

    describe('filter ::', () => {
        it('Should fail if entry value parse fails', done => {
            const processEntryStub = sinon.stub(notificationQueuePopulator, '_processObjectEntry');
            const entry = {
                bucket: 'example-bucket',
                key: 'examlpe-key',
                type: 'put',
                value: '}{',
            };
            notificationQueuePopulator.filter(entry);
            assert(processEntryStub.notCalled);
            return done();
        });

        it('Should fail if entry is a bucket entry', done => {
            const processEntryStub = sinon.stub(notificationQueuePopulator, '_processObjectEntry');
            const entry = {
                bucket: '__metastore',
                key: 'example-bucket',
                type: 'put',
                value: '{}',
            };
            notificationQueuePopulator.filter(entry);
            assert(processEntryStub.notCalled);
            return done();
        });

        it('Should process the entry', done => {
            const processEntryStub = sinon.stub(notificationQueuePopulator, '_processObjectEntry');
            const entry = {
                bucket: 'example-bucket',
                key: 'examlpe-key',
                type: 'put',
                value: '{}',
            };
            notificationQueuePopulator.filter(entry);
            assert(processEntryStub.calledOnceWith(entry.bucket, entry.key, {}, entry.type));
            return done();
        });
    });
});
