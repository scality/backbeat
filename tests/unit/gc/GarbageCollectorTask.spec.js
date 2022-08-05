const assert = require('assert');
const werelogs = require('werelogs');
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

    beforeEach(() => {
        mdObj = new ObjectMD();
        backbeatClient = new BackbeatClientMock();
        backbeatMetadataProxyClient = new BackbeatMetadataProxyMock();
        gcProducer = new GarbageCollectorProducerMock();
        gcProcessor = new ProcessorMock(
            null,
            backbeatClient,
            backbeatMetadataProxyClient,
            gcProducer,
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
});
