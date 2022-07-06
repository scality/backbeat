const assert = require('assert');
const werelogs = require('werelogs');
const { ObjectMD } = require('arsenal').models;

const LifecycleColdStatusArchiveTask = require(
    '../../../extensions/lifecycle/tasks/LifecycleColdStatusArchiveTask');
const ColdStorageStatusQueueEntry =
    require('../../../lib/models/ColdStorageStatusQueueEntry');

const {
    GarbageCollectorProducerMock,
    LifecycleObjectProcessorMock,
    BackbeatClientMock,
    BackbeatMetadataProxyMock,
} = require('./mocks');

const message = Buffer.from(`{
"op":"archive",
"bucketName":"testBucket",
"objectKey":"testObj",
"objectVersion":"testversionkey",
"accountId":"834789881858",
"archiveInfo":{"archiveId":"da80b6dc-280d-4dce-83b5-d5b40276e321","archiveVersion":5166759712787974},
"requestId":"060733275a408411c862"
}`);

const loc = [{
    key: 'key',
    size: 10,
    start: 0,
    dataStoreName: 'locationName',
    dataStoreType: 'aws_s3',
    dataStoreETag: 'tag',
    dataStoreVersionId: '1234567890',
}];

describe('LifecycleColdStatusArchiveTask', () => {
    let archiveTask;
    let backbeatClient;
    let backbeatMetadataProxyClient;
    let gcProducer;
    let objectProcessor;
    let mdObj;

    beforeEach(() => {
        mdObj = new ObjectMD();
        backbeatClient = new BackbeatClientMock();
        backbeatMetadataProxyClient = new BackbeatMetadataProxyMock();
        gcProducer = new GarbageCollectorProducerMock();
        objectProcessor = new LifecycleObjectProcessorMock(
            null,
            backbeatClient,
            backbeatMetadataProxyClient,
            gcProducer,
            new werelogs.Logger('test:LifecycleColdStatusArchiveTask'));
        archiveTask = new LifecycleColdStatusArchiveTask(objectProcessor);
    });

    it('should delete set archive info and delete location info', done => {
        backbeatClient.batchDeleteResponse = { error: null, res: null };

        const entry = ColdStorageStatusQueueEntry.createFromKafkaEntry({ value: message });
        mdObj.setLocation(loc)
            .setArchive(null);
        backbeatMetadataProxyClient.setMdObj(mdObj);

        archiveTask.processEntry(entry, err => {
            assert.ifError(err);

            const updatedMD = backbeatMetadataProxyClient.getReceivedMd();
            assert.strictEqual(updatedMD.location, null);
            assert.deepStrictEqual(updatedMD.archive.archiveInfo, {
                archiveId: 'da80b6dc-280d-4dce-83b5-d5b40276e321',
                archiveVersion: 5166759712787974,
            });
            done();
        });
    });

    it('should not delete location info if gc failed', done => {
        backbeatClient.batchDeleteResponse = { error: { statusCode: 500 }, res: null };

        const entry = ColdStorageStatusQueueEntry.createFromKafkaEntry({ value: message });
        mdObj.setLocation(loc)
            .setArchive(null);
        backbeatMetadataProxyClient.setMdObj(mdObj);

        archiveTask.processEntry(entry, err => {
            assert.strictEqual(err.statusCode, 500);

            const updatedMD = backbeatMetadataProxyClient.getReceivedMd();
            assert.deepStrictEqual(updatedMD.location, loc);
            assert.deepStrictEqual(updatedMD.archive.archiveInfo, {
                archiveId: 'da80b6dc-280d-4dce-83b5-d5b40276e321',
                archiveVersion: 5166759712787974,
            });
            done();
        });
    });

    it('should delete location info if gc failed with 404', done => {
        backbeatClient.batchDeleteResponse = { error: { statusCode: 404 }, res: null };

        const entry = ColdStorageStatusQueueEntry.createFromKafkaEntry({ value: message });
        mdObj.setLocation(loc)
            .setArchive(null);
        backbeatMetadataProxyClient.setMdObj(mdObj);

        archiveTask.processEntry(entry, err => {
            assert.ifError(err);

            const updatedMD = backbeatMetadataProxyClient.getReceivedMd();
            assert.strictEqual(updatedMD.location, null);
            assert.deepStrictEqual(updatedMD.archive.archiveInfo, {
                archiveId: 'da80b6dc-280d-4dce-83b5-d5b40276e321',
                archiveVersion: 5166759712787974,
            });
            done();
        });
    });

    it('should skip delete gc if location info is empty', done => {
        backbeatClient.batchDeleteResponse = { error: { statusCode: 404 }, res: null };

        const entry = ColdStorageStatusQueueEntry.createFromKafkaEntry({ value: message });
        mdObj.setLocation()
            .setArchive(null);
        backbeatMetadataProxyClient.setMdObj(mdObj);

        archiveTask.processEntry(entry, err => {
            assert.ifError(err);

            const updatedMD = backbeatMetadataProxyClient.getReceivedMd();
            assert.strictEqual(backbeatClient.times.batchDeleteResponse, 0);
            assert.strictEqual(updatedMD.location, null);
            assert.deepStrictEqual(updatedMD.archive.archiveInfo, {
                archiveId: 'da80b6dc-280d-4dce-83b5-d5b40276e321',
                archiveVersion: 5166759712787974,
            });
            done();
        });
    });
});
