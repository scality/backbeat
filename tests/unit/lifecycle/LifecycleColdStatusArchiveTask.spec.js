const assert = require('assert');
const werelogs = require('werelogs');
const { ObjectMD } = require('arsenal').models;
const sinon = require('sinon');
const config = require('../../config.json');

const LifecycleColdStatusArchiveTask = require(
    '../../../extensions/lifecycle/tasks/LifecycleColdStatusArchiveTask');
const ColdStorageStatusQueueEntry =
    require('../../../lib/models/ColdStorageStatusQueueEntry');

const {
    GarbageCollectorProducerMock,
    ProcessorMock,
    BackbeatClientMock,
    BackbeatMetadataProxyMock,
    BackbeatProducerMock,
} = require('../mocks');

const message = Buffer.from(`{
"op":"archive",
"bucketName":"testBucket",
"objectKey":"testObj",
"objectVersion":"testversion",
"accountId":"834789881858",
"archiveInfo":{"archiveId":"da80b6dc-280d-4dce-83b5-d5b40276e321","archiveVersion":5166759712787974},
"requestId":"060733275a408411c862"
}`);

const coldLocation = 'cold';

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
    let coldProducer;
    let objectProcessor;
    let mdObj;

    beforeEach(() => {
        mdObj = new ObjectMD();
        backbeatClient = new BackbeatClientMock();
        backbeatMetadataProxyClient = new BackbeatMetadataProxyMock();
        gcProducer = new GarbageCollectorProducerMock();
        coldProducer = new BackbeatProducerMock();
        objectProcessor = new ProcessorMock(
            config.extensions.lifecycle,
            null,
            backbeatClient,
            backbeatMetadataProxyClient,
            gcProducer,
            coldProducer,
            null,
            new werelogs.Logger('test:LifecycleColdStatusArchiveTask'));
        archiveTask = new LifecycleColdStatusArchiveTask(objectProcessor);
    });

    it('should set archive info and publish gc info', done => {
        backbeatClient.batchDeleteResponse = { error: null, res: null };

        const entry = ColdStorageStatusQueueEntry.createFromKafkaEntry({ value: message });
        mdObj.setLocation(loc)
            .setDataStoreName('us-east-1')
            .setAmzStorageClass('us-east-1')
            .setArchive(null);
        backbeatMetadataProxyClient.setMdObj(mdObj);

        archiveTask.processEntry(coldLocation, entry, err => {
            assert.ifError(err);

            const updatedMD = backbeatMetadataProxyClient.getReceivedMd();
            const gcEntry = gcProducer.getReceivedEntry();
            assert.strictEqual(gcEntry.getActionType(), 'deleteArchivedSourceData');
            assert.deepStrictEqual(gcEntry.getAttribute('target'), {
                oldLocation: 'us-east-1',
                newLocation: 'cold',
                bucket: 'testBucket',
                version: 'testversion',
                key: 'testObj',
                accountId: '834789881858',
            });
            assert.strictEqual(updatedMD.dataStoreName, 'us-east-1');
            assert.deepStrictEqual(updatedMD.archive.archiveInfo, {
                archiveId: 'da80b6dc-280d-4dce-83b5-d5b40276e321',
                archiveVersion: 5166759712787974,
            });
            done();
        });
    });

    it('should set archive info and not publish gc entry if location info is empty', done => {
        backbeatClient.batchDeleteResponse = { error: { statusCode: 404 }, res: null };

        const entry = ColdStorageStatusQueueEntry.createFromKafkaEntry({ value: message });
        mdObj.setLocation()
            .setDataStoreName('us-east-1')
            .setAmzStorageClass('us-east-1')
            .setArchive(null);
        backbeatMetadataProxyClient.setMdObj(mdObj);

        archiveTask.processEntry(coldLocation, entry, err => {
            assert.ifError(err);

            const updatedMD = backbeatMetadataProxyClient.getReceivedMd();
            const gcEntry = gcProducer.getReceivedEntry();
            assert.strictEqual(gcEntry, null);
            assert.strictEqual(updatedMD.dataStoreName, 'cold');
            assert.strictEqual(updatedMD['x-amz-storage-class'], 'cold');
            assert.deepStrictEqual(updatedMD.archive.archiveInfo, {
                archiveId: 'da80b6dc-280d-4dce-83b5-d5b40276e321',
                archiveVersion: 5166759712787974,
            });
            done();
        });
    });

    it('should send kafka entry to delete orphan cold object when source object was deleted', done => {
        const entry = ColdStorageStatusQueueEntry.createFromKafkaEntry({ value: message });

        sinon.stub(backbeatMetadataProxyClient, 'getMetadata').yields({ code: 'ObjNotFound' });

        archiveTask.processEntry(coldLocation, entry, err => {
            assert.ifError(err);

            const gcColdEntry = coldProducer.getReceivedEntry()[0];
            const message = JSON.parse(gcColdEntry.message);

            const topic = coldProducer.getReceivedTopic();

            assert.strictEqual(topic, 'cold-gc-req-cold');

            assert.strictEqual(message.bucketName, entry.target.bucketName);
            assert.strictEqual(message.objectKey, entry.target.objectKey);
            assert.strictEqual(message.objectVersion, entry.target.objectVersion);
            assert.deepStrictEqual(message.archiveInfo.archiveId, entry.archiveInfo.archiveId);
            assert.strictEqual(message.archiveInfo.archiveVersion, entry.archiveInfo.archiveVersion);

            const updatedMD = backbeatMetadataProxyClient.getReceivedMd();
            assert.strictEqual(updatedMD, null);

            const gcEntry = gcProducer.getReceivedEntry();
            assert.strictEqual(gcEntry, null);

            done();
        });
    });
});
