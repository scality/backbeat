const assert = require('assert');
const werelogs = require('werelogs');
const sinon = require('sinon');
const { ObjectMD } = require('arsenal').models;

const GarbageCollectorTask = require('../../../extensions/gc/tasks/GarbageCollectorTask');
const ActionQueueEntry = require('../../../lib/models/ActionQueueEntry');

const {
    GarbageCollectorProducerMock,
    ProcessorMock,
    BackbeatClientMock,
    BackbeatMetadataProxyMock,
} = require('../mocks');

const bucket = 'testbucket';
const key = 'testkey';
const version = 'testversion';
const accountId = '834789881858';
const owner = 'ownerinfo';

const loc = [{
    key: 'key',
    size: 10,
    start: 0,
    dataStoreName: 'locationName',
    dataStoreType: 'aws_s3',
    dataStoreETag: 'tag',
    dataStoreVersionId: '1234567890',
}];

describe('GarbageCollectorTask', () => {
    let gcTask;
    let backbeatClient;
    let backbeatMetadataProxyClient;
    let gcProducer;
    let gcProcessor;
    let mdObj;
    let gcConfig;

    beforeEach(() => {
        mdObj = new ObjectMD();
        backbeatClient = new BackbeatClientMock();
        backbeatMetadataProxyClient = new BackbeatMetadataProxyMock();
        gcProducer = new GarbageCollectorProducerMock();
        gcConfig = {
            consumer: {
                retry: {
                    maxRetries: 3,
                },
            },
        };
        gcProcessor = new ProcessorMock(
            null,
            null,
            backbeatClient,
            backbeatMetadataProxyClient,
            gcProducer,
            null,
            gcConfig,
            new werelogs.Logger('test:GarbageCollectorTask'));
        gcTask = new GarbageCollectorTask(gcProcessor);
    });

    it('should delete archived location info', done => {
        backbeatClient.batchDeleteResponse = { error: null, res: null };

        const entry = ActionQueueEntry.create('deleteArchivedSourceData')
              .addContext({
                  origin: 'lifecycle',
                  ruleType: 'archive',
                  bucketName: bucket,
                  objectKey: key,
                  versionId: version,
              })
              .setAttribute('serviceName', 'lifecycle-transition')
              .setAttribute('target.oldLocation', 'old-location')
              .setAttribute('target.newLocation', 'new-location')
              .setAttribute('target.bucket', bucket)
              .setAttribute('target.key', version)
              .setAttribute('target.version', key)
              .setAttribute('target.accountId', accountId)
              .setAttribute('target.owner', owner);

        mdObj.setLocation(loc)
            .setDataStoreName('old-location')
            .setAmzStorageClass('old-location')
            .setTransitionInProgress(true);
        backbeatMetadataProxyClient.setMdObj(mdObj);

        gcTask.processActionEntry(entry, (err, commitInfo) => {
            assert.ifError(err);
            assert.strictEqual(commitInfo, undefined);

            const updatedMD = backbeatMetadataProxyClient.mdObj;
            assert.deepStrictEqual(updatedMD.getLocation(), []);
            assert.strictEqual(updatedMD.getDataStoreName(), 'new-location');
            assert.strictEqual(updatedMD.getAmzStorageClass(), 'new-location');
            assert.strictEqual(updatedMD.getTransitionInProgress(), false);
            done();
        });
    });


    it('should delete archived location info if gc failed with 404', done => {
        backbeatClient.batchDeleteResponse = { error: { statusCode: 404 }, res: null };

        const entry = ActionQueueEntry.create('deleteArchivedSourceData')
              .addContext({
                  origin: 'lifecycle',
                  ruleType: 'archive',
                  bucketName: bucket,
                  objectKey: key,
                  versionId: version,
              })
              .setAttribute('serviceName', 'lifecycle-transition')
              .setAttribute('target.oldLocation', 'old-location')
              .setAttribute('target.newLocation', 'new-location')
              .setAttribute('target.bucket', bucket)
              .setAttribute('target.key', version)
              .setAttribute('target.version', key)
              .setAttribute('target.accountId', accountId)
              .setAttribute('target.owner', owner);

        mdObj.setLocation(loc)
            .setDataStoreName('old-location')
            .setAmzStorageClass('old-location')
            .setTransitionInProgress(true);
        backbeatMetadataProxyClient.setMdObj(mdObj);

        gcTask.processActionEntry(entry, (err, commitInfo) => {
            assert.ifError(err);
            assert.strictEqual(commitInfo, undefined);

            const updatedMD = backbeatMetadataProxyClient.mdObj;
            assert.deepStrictEqual(updatedMD.getLocation(), []);
            assert.strictEqual(updatedMD.getDataStoreName(), 'new-location');
            assert.strictEqual(updatedMD.getAmzStorageClass(), 'new-location');
            assert.strictEqual(updatedMD.getTransitionInProgress(), false);
            done();
        });
    });

    it('should not delete archived location info if gc failed', done => {
        backbeatClient.batchDeleteResponse = { error: { statusCode: 500 }, res: null };

        const entry = ActionQueueEntry.create('deleteArchivedSourceData')
              .addContext({
                  origin: 'lifecycle',
                  ruleType: 'archive',
                  bucketName: bucket,
                  objectKey: key,
                  versionId: version,
              })
              .setAttribute('serviceName', 'lifecycle-transition')
              .setAttribute('target.oldLocation', 'old-location')
              .setAttribute('target.newLocation', 'new-location')
              .setAttribute('target.bucket', bucket)
              .setAttribute('target.key', version)
              .setAttribute('target.version', key)
              .setAttribute('target.accountId', accountId)
              .setAttribute('target.owner', owner);

        mdObj.setLocation(loc)
            .setDataStoreName('old-location')
            .setAmzStorageClass('old-location')
            .setTransitionInProgress(true);
        backbeatMetadataProxyClient.setMdObj(mdObj);

        gcTask.processActionEntry(entry, (err, commitInfo) => {
            assert.strictEqual(err.statusCode, 500);
            assert.deepStrictEqual(commitInfo, { committable: false });
            assert.strictEqual(backbeatMetadataProxyClient.getReceivedMd(), null);

            const objMD = backbeatMetadataProxyClient.mdObj;
            assert.deepStrictEqual(objMD.getLocation(), loc);
            assert.strictEqual(objMD.getDataStoreName(), 'old-location');
            assert.strictEqual(objMD.getAmzStorageClass(), 'old-location');
            assert.strictEqual(objMD.getTransitionInProgress(), true);
            done();
        });
    });

    it('should retry delete operation if gc failed with retryable error', done => {
        backbeatClient.batchDeleteResponse = { error: { statusCode: 500, retryable: true }, res: null };

        const entry = ActionQueueEntry.create('deleteArchivedSourceData')
              .addContext({
                  origin: 'lifecycle',
                  ruleType: 'archive',
                  bucketName: bucket,
                  objectKey: key,
                  versionId: version,
              })
              .setAttribute('serviceName', 'lifecycle-transition')
              .setAttribute('target.oldLocation', 'old-location')
              .setAttribute('target.newLocation', 'new-location')
              .setAttribute('target.bucket', bucket)
              .setAttribute('target.key', version)
              .setAttribute('target.version', key)
              .setAttribute('target.accountId', accountId)
              .setAttribute('target.owner', owner);

        mdObj.setLocation(loc)
            .setDataStoreName('old-location')
            .setAmzStorageClass('old-location')
            .setTransitionInProgress(true);
        backbeatMetadataProxyClient.setMdObj(mdObj);

        const batchDeleteDataSpy = sinon.spy(gcTask, '_batchDeleteData');

        gcTask.processActionEntry(entry, (err, commitInfo) => {
            assert.strictEqual(batchDeleteDataSpy.callCount, 4);
            assert.strictEqual(err.statusCode, 500);
            assert.deepStrictEqual(commitInfo, { committable: false });
            batchDeleteDataSpy.restore();
            done();
        });
    });

    it('should send committable error when object is not found', done => {
        backbeatMetadataProxyClient.error = { statusCode: 404, code: 'ObjNotFound' };

        const entry = ActionQueueEntry.create('deleteArchivedSourceData')
              .addContext({
                  origin: 'lifecycle',
                  ruleType: 'archive',
                  bucketName: bucket,
                  objectKey: key,
                  versionId: version,
              })
              .setAttribute('serviceName', 'lifecycle-transition')
              .setAttribute('target.oldLocation', 'old-location')
              .setAttribute('target.newLocation', 'new-location')
              .setAttribute('target.bucket', bucket)
              .setAttribute('target.key', version)
              .setAttribute('target.version', key)
              .setAttribute('target.accountId', accountId)
              .setAttribute('target.owner', owner);

        backbeatMetadataProxyClient.setMdObj(undefined);

        gcTask.processActionEntry(entry, (err, commitInfo) => {
            assert.strictEqual(err.statusCode, 404);
            assert.deepStrictEqual(commitInfo, { committable: true });
            done();
        });
    });

    it('should send committable error when bucket is not found', done => {
        backbeatMetadataProxyClient.error = { statusCode: 404, code: 'NoSuchBucket' };

        const entry = ActionQueueEntry.create('deleteArchivedSourceData')
              .addContext({
                  origin: 'lifecycle',
                  ruleType: 'archive',
                  bucketName: bucket,
                  objectKey: key,
                  versionId: version,
              })
              .setAttribute('serviceName', 'lifecycle-transition')
              .setAttribute('target.oldLocation', 'old-location')
              .setAttribute('target.newLocation', 'new-location')
              .setAttribute('target.bucket', bucket)
              .setAttribute('target.key', version)
              .setAttribute('target.version', key)
              .setAttribute('target.accountId', accountId)
              .setAttribute('target.owner', owner);

        backbeatMetadataProxyClient.setMdObj(undefined);

        gcTask.processActionEntry(entry, (err, commitInfo) => {
            assert.strictEqual(err.statusCode, 404);
            assert.deepStrictEqual(commitInfo, { committable: true });
            done();
        });
    });

    it('should delete data', done => {
        backbeatClient.batchDeleteResponse = { error: null, res: null };

        const entry = ActionQueueEntry.create('deleteData')
            .addContext({
                origin: 'lifecycle',
                ruleType: 'expiration',
                bucketName: bucket,
                objectKey: key,
                versionId: version,
            })
            .setAttribute('serviceName', 'lifecycle-expiration')
            .setAttribute('target', {
                bucket,
                key: version,
                version: key,
                accountId,
                owner,
                locations: [{
                    key: 'locationKey',
                    dataStoreName: 'dataStoreName',
                    size: 'size',
                    dataStoreVersionId: 'dataStoreVersionId',
                }],
            });
        mdObj.setLocation(loc)
            .setDataStoreName('locationName')
            .setAmzStorageClass('STANDARD');

        backbeatMetadataProxyClient.setMdObj(mdObj);

        const batchDeleteDataSpy = sinon.spy(gcTask, '_batchDeleteData');

        gcTask.processActionEntry(entry, (err, commitInfo) => {
            assert.ifError(err);
            assert.strictEqual(commitInfo, undefined);

            const updatedMD = backbeatMetadataProxyClient.mdObj;
            assert.deepStrictEqual(updatedMD.getLocation(), loc);
            assert.strictEqual(updatedMD.getDataStoreName(), 'locationName');
            assert.strictEqual(updatedMD.getAmzStorageClass(), 'STANDARD');

            assert.strictEqual(batchDeleteDataSpy.callCount, 1);
            batchDeleteDataSpy.restore();
            done();
        });
    });

    it('should retry delete operation if gc failed with retryable error', done => {
        backbeatClient.batchDeleteResponse = { error: { statusCode: 500, retryable: true }, res: null };

        const entry = ActionQueueEntry.create('deleteData')
            .addContext({
                origin: 'lifecycle',
                ruleType: 'expiration',
                bucketName: bucket,
                objectKey: key,
                versionId: version,
            })
            .setAttribute('serviceName', 'lifecycle-expiration')
            .setAttribute('target', {
                bucket,
                key: version,
                version: key,
                accountId,
                owner,
                locations: [{
                    key: 'locationKey',
                    dataStoreName: 'dataStoreName',
                    size: 'size',
                    dataStoreVersionId: 'dataStoreVersionId',
                }],
            });
        mdObj.setLocation(loc)
            .setDataStoreName('locationName')
            .setAmzStorageClass('STANDARD');

        backbeatMetadataProxyClient.setMdObj(mdObj);

        const batchDeleteDataSpy = sinon.spy(gcTask, '_batchDeleteData');

        gcTask.processActionEntry(entry, err => {
            assert.strictEqual(batchDeleteDataSpy.callCount, 4);
            assert.strictEqual(err.statusCode, 500);
            batchDeleteDataSpy.restore();
            done();
        });
    });

});
