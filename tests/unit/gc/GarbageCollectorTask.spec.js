const assert = require('assert');
const werelogs = require('werelogs');
const sinon = require('sinon');
const { ObjectMD } = require('arsenal').models;

const GarbageCollectorTask = require('../../../extensions/gc/tasks/GarbageCollectorTask');
const { GarbageCollectorMetrics } = require('../../../extensions/gc/GarbageCollectorMetrics');
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
                    backoff: { min: 10, max: 20, factor: 1, jitter: 0 },
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

    it('should commit and record a failure metric if gc failed', done => {
        backbeatClient.batchDeleteResponse = { error: { statusCode: 500 }, res: null };
        const onGcFailedSpy = sinon.spy(GarbageCollectorMetrics, 'onGcFailed');

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
            assert.strictEqual(commitInfo, undefined);
            assert.strictEqual(onGcFailedSpy.callCount, 1);
            assert.deepStrictEqual(onGcFailedSpy.firstCall.args.slice(1),
                ['archive', 'old-location']);
            assert.strictEqual(backbeatMetadataProxyClient.getReceivedMd(), null);

            const objMD = backbeatMetadataProxyClient.mdObj;
            assert.deepStrictEqual(objMD.getLocation(), loc);
            assert.strictEqual(objMD.getDataStoreName(), 'old-location');
            assert.strictEqual(objMD.getAmzStorageClass(), 'old-location');
            assert.strictEqual(objMD.getTransitionInProgress(), true);
            onGcFailedSpy.restore();
            done();
        });
    });

    it('should retry delete archived source data if gc failed with retryable error', done => {
        backbeatClient.batchDeleteResponse = { error: { statusCode: 500, retryable: true }, res: null };
        const onGcFailedSpy = sinon.spy(GarbageCollectorMetrics, 'onGcFailed');

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
            assert.strictEqual(commitInfo, undefined);
            assert.strictEqual(onGcFailedSpy.callCount, 1);
            assert.deepStrictEqual(onGcFailedSpy.firstCall.args.slice(1),
                ['archive', 'old-location']);
            batchDeleteDataSpy.restore();
            onGcFailedSpy.restore();
            done();
        });
    });

    it('should commit without retry when object is not found', done => {
        backbeatMetadataProxyClient.error = { statusCode: 404, name: 'ObjNotFound' };

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
            assert.strictEqual(commitInfo, undefined);
            done();
        });
    });

    it('should commit without retry when bucket is not found', done => {
        backbeatMetadataProxyClient.error = { statusCode: 404, name: 'NoSuchBucket' };

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
            assert.strictEqual(commitInfo, undefined);
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

    it('should retry delete data if gc failed with retryable error', done => {
        backbeatClient.batchDeleteResponse = { error: { statusCode: 500, retryable: true }, res: null };
        const onGcFailedSpy = sinon.spy(GarbageCollectorMetrics, 'onGcFailed');

        const entry = ActionQueueEntry.create('deleteData')
            .addContext({
                origin: 'lifecycle',
                ruleType: 'expiration',
                bucketName: bucket,
                objectKey: key,
                versionId: version,
            })
            .setAttribute('serviceName', 'lifecycle-expiration')
            .setAttribute('source', {
                bucket,
                objectKey: key,
                storageClass: 'sourceStorageClass',
            })
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
            assert.strictEqual(onGcFailedSpy.callCount, 1);
            assert.deepStrictEqual(onGcFailedSpy.firstCall.args.slice(1),
                ['expiration', 'sourceStorageClass']);
            batchDeleteDataSpy.restore();
            onGcFailedSpy.restore();
            done();
        });
    });

    describe('with CRR locations', () => {
        let log;

        function createDeleteDataEntry(locations) {
            return ActionQueueEntry.create('deleteData')
                .addContext({
                    origin: 'lifecycle',
                    ruleType: 'transition',
                    bucketName: bucket,
                    objectKey: key,
                    versionId: version,
                })
                .setAttribute('serviceName', 'lifecycle-transition')
                .setAttribute('source', {
                    bucket,
                    objectKey: key,
                    storageClass: 'sourceStorageClass',
                })
                .setAttribute('target', {
                    bucket,
                    key: version,
                    version: key,
                    accountId,
                    owner,
                    locations,
                });
        }

        const crrLocation = {
            key: 'crrKey',
            dataStoreName: 'location-crr-source',
            size: 10,
            dataStoreVersionId: 'crrVersionId',
        };
        const regularLocation = {
            key: 'locationKey',
            dataStoreName: 'us-east-1',
            size: 20,
            dataStoreVersionId: 'dataStoreVersionId',
        };

        beforeEach(() => {
            log = {
                info: sinon.spy(),
                warn: sinon.spy(),
                debug: sinon.spy(),
                error: sinon.spy(),
                getSerializedUids: () => 'uids',
            };
            log.end = () => log;
            gcTask.logger = { newRequestLogger: () => log };
            backbeatClient.batchDeleteResponse = { error: null, res: null };
        });

        it('should not delete anything and warn when all locations are on a ' +
        'CRR location', done => {
            const entry = createDeleteDataEntry([crrLocation]);
            const batchDeleteDataSpy = sinon.spy(gcTask, '_batchDeleteData');
            const onGcCompletedSpy = sinon.spy(GarbageCollectorMetrics, 'onGcCompleted');

            gcTask.processActionEntry(entry, err => {
                assert.ifError(err);
                assert.strictEqual(batchDeleteDataSpy.callCount, 0);
                assert.strictEqual(backbeatClient.times.batchDeleteResponse, 0);
                assert.strictEqual(onGcCompletedSpy.callCount, 0);
                assert.strictEqual(log.warn.callCount, 1);
                assert.strictEqual(
                    log.warn.firstCall.args[1].dataStoreName,
                    'location-crr-source');
                assert.strictEqual(entry.getStatus(), 'success');
                batchDeleteDataSpy.restore();
                onGcCompletedSpy.restore();
                done();
            });
        });

        it('should not delete anything when any location is on a CRR ' +
        'location', done => {
            const entry = createDeleteDataEntry([regularLocation, crrLocation]);
            const batchDeleteDataSpy = sinon.spy(gcTask, '_batchDeleteData');

            gcTask.processActionEntry(entry, err => {
                assert.ifError(err);
                assert.strictEqual(batchDeleteDataSpy.callCount, 0);
                assert.strictEqual(log.warn.callCount, 1);
                assert.strictEqual(entry.getStatus(), 'success');
                batchDeleteDataSpy.restore();
                done();
            });
        });

        it('should delete all locations and not warn when none is on a CRR ' +
        'location', done => {
            const entry = createDeleteDataEntry([regularLocation]);
            const batchDeleteDataSpy = sinon.spy(gcTask, '_batchDeleteData');

            gcTask.processActionEntry(entry, err => {
                assert.ifError(err);
                assert.strictEqual(batchDeleteDataSpy.callCount, 1);
                assert.deepStrictEqual(
                    batchDeleteDataSpy.firstCall.args[0].Locations,
                    [regularLocation]);
                assert.strictEqual(log.warn.callCount, 0);
                batchDeleteDataSpy.restore();
                done();
            });
        });
    });
});
