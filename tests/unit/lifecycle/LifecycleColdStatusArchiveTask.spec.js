const assert = require('assert');
const werelogs = require('werelogs');
const { ObjectMD, ObjectMDArchive } = require('@scality/arsenal').models;
const sinon = require('sinon');
const config = require('../../config.json');

const { LifecycleMetrics } = require('../../../extensions/lifecycle/LifecycleMetrics');
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
            assert.strictEqual(updatedMD.originOp, 's3:LifecycleTransition');
            assert.deepStrictEqual(updatedMD.archive.archiveInfo, {
                archiveId: 'da80b6dc-280d-4dce-83b5-d5b40276e321',
                archiveVersion: 5166759712787974,
            });
            done();
        });
    });

    it('should report the transition latency if location info is empty', done => {
        const onLifecycleCompleted = sinon.spy(LifecycleMetrics, 'onLifecycleCompleted');

        const entry = ColdStorageStatusQueueEntry.createFromKafkaEntry({ value: message });
        mdObj.setLocation()
            .setDataStoreName('us-east-1')
            .setAmzStorageClass('us-east-1')
            .setTransitionInProgress(true, Date.now() - 1000)
            .setArchive(null);
        backbeatMetadataProxyClient.setMdObj(mdObj);

        archiveTask.processEntry(coldLocation, entry, err => {
            onLifecycleCompleted.restore();
            assert.ifError(err);

            assert(onLifecycleCompleted.calledOnce);
            const durationMs = onLifecycleCompleted.firstCall.args[3];
            assert(durationMs >= 1000, `unexpected latency ${durationMs}`);
            done();
        });
    });

    it('should propagate the error without setting committable when metadata update fails', done => {
        const putError = new Error('metadata service down');
        putError.retryable = false;
        sinon.stub(backbeatMetadataProxyClient, 'putMetadata').yields(putError);

        const entry = ColdStorageStatusQueueEntry.createFromKafkaEntry({ value: message });
        mdObj.setLocation(loc)
            .setDataStoreName('us-east-1')
            .setAmzStorageClass('us-east-1')
            .setArchive(null);
        backbeatMetadataProxyClient.setMdObj(mdObj);

        archiveTask.processEntry(coldLocation, entry, (err, commitInfo) => {
            assert.strictEqual(err, putError);
            assert.strictEqual(commitInfo, undefined);
            done();
        });
    });

    it('should set the direct transition origin op if the cold class was requested', done => {
        backbeatClient.batchDeleteResponse = { error: { statusCode: 404 }, res: null };

        const entry = ColdStorageStatusQueueEntry.createFromKafkaEntry({ value: message });
        mdObj.setLocation()
            .setDataStoreName('us-east-1')
            .setAmzStorageClass(coldLocation)
            .setArchive(null);
        backbeatMetadataProxyClient.setMdObj(mdObj);

        archiveTask.processEntry(coldLocation, entry, err => {
            assert.ifError(err);

            const updatedMD = backbeatMetadataProxyClient.getReceivedMd();
            assert.strictEqual(updatedMD.dataStoreName, 'cold');
            assert.strictEqual(updatedMD.originOp, 's3:LifecycleTransition:Direct');
            done();
        });
    });

    it('should keep a restore requested during the archive window', done => {
        backbeatClient.batchDeleteResponse = { error: { statusCode: 404 }, res: null };

        const entry = ColdStorageStatusQueueEntry.createFromKafkaEntry({ value: message });
        mdObj.setLocation()
            .setDataStoreName('us-east-1')
            .setAmzStorageClass(coldLocation);
        // setArchive() requires archiveInfo, which the object does not have yet
        mdObj.getValue().archive = {
            restoreRequestedAt: '2017-07-11T02:44:25.515Z',
            restoreRequestedDays: 3,
        };
        backbeatMetadataProxyClient.setMdObj(mdObj);

        archiveTask.processEntry(coldLocation, entry, err => {
            assert.ifError(err);

            const updatedMD = backbeatMetadataProxyClient.getReceivedMd();
            assert.strictEqual(updatedMD.originOp, 's3:LifecycleTransition:Direct');
            assert.deepStrictEqual(updatedMD.archive, {
                archiveInfo: {
                    archiveId: 'da80b6dc-280d-4dce-83b5-d5b40276e321',
                    archiveVersion: 5166759712787974,
                },
                restoreRequestedAt: '2017-07-11T02:44:25.515Z',
                restoreRequestedDays: 3,
            });
            done();
        });
    });

    it('should send kafka entry to delete orphan cold object when source object was deleted', done => {
        const entry = ColdStorageStatusQueueEntry.createFromKafkaEntry({ value: message });

        sinon.stub(backbeatMetadataProxyClient, 'getMetadata').yields({ name: 'ObjNotFound' });

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

    describe('redundant archive', () => {
        const existingArchiveInfo = {
            archiveId: '11111111-2222-3333-4444-555555555555',
            archiveVersion: 42,
        };

        it('should delete the new archive when the object was already archived', done => {
            const entry = ColdStorageStatusQueueEntry.createFromKafkaEntry({ value: message });
            mdObj.setLocation(loc)
                .setDataStoreName('us-east-1')
                .setAmzStorageClass('us-east-1')
                .setArchive(new ObjectMDArchive(existingArchiveInfo));
            backbeatMetadataProxyClient.setMdObj(mdObj);

            archiveTask.processEntry(coldLocation, entry, err => {
                assert.ifError(err);

                const coldGcMessage = JSON.parse(coldProducer.getReceivedEntry()[0].message);
                assert.strictEqual(coldProducer.getReceivedTopic(), 'cold-gc-req-cold');
                assert.deepStrictEqual(coldGcMessage.archiveInfo, entry.archiveInfo);

                // the object keeps referring to the existing archive
                assert.strictEqual(backbeatMetadataProxyClient.getReceivedMd(), null);
                assert.strictEqual(gcProducer.getReceivedEntry(), null);
                done();
            });
        });

        it('should delete the new archive when a restore is pending on the previous one', done => {
            const entry = ColdStorageStatusQueueEntry.createFromKafkaEntry({ value: message });
            mdObj.setLocation(loc)
                .setDataStoreName('us-east-1')
                .setAmzStorageClass(coldLocation)
                .setArchive(new ObjectMDArchive(
                    existingArchiveInfo, '2017-07-11T02:44:25.515Z', 3));
            backbeatMetadataProxyClient.setMdObj(mdObj);

            archiveTask.processEntry(coldLocation, entry, err => {
                assert.ifError(err);

                const coldGcMessage = JSON.parse(coldProducer.getReceivedEntry()[0].message);
                assert.deepStrictEqual(coldGcMessage.archiveInfo, entry.archiveInfo);

                // metadata is left untouched: the restore request refers to the existing archive
                assert.strictEqual(backbeatMetadataProxyClient.getReceivedMd(), null);
                assert.strictEqual(gcProducer.getReceivedEntry(), null);
                done();
            });
        });

        it('should delete the new archive when the object is restored', done => {
            const entry = ColdStorageStatusQueueEntry.createFromKafkaEntry({ value: message });
            mdObj.setLocation(loc)
                .setDataStoreName('us-east-1')
                .setAmzStorageClass(coldLocation);
            mdObj.getValue().archive = {
                archiveInfo: existingArchiveInfo,
                restoreRequestedAt: '2017-07-11T02:44:25.515Z',
                restoreRequestedDays: 3,
                restoreCompletedAt: '2017-07-11T03:44:25.515Z',
                restoreWillExpireAt: '2017-07-14T03:44:25.515Z',
            };
            backbeatMetadataProxyClient.setMdObj(mdObj);

            archiveTask.processEntry(coldLocation, entry, err => {
                assert.ifError(err);

                const coldGcMessage = JSON.parse(coldProducer.getReceivedEntry()[0].message);
                assert.deepStrictEqual(coldGcMessage.archiveInfo, entry.archiveInfo);

                // the restored (hot) data must stay readable, and expire on its own
                assert.strictEqual(backbeatMetadataProxyClient.getReceivedMd(), null);
                assert.strictEqual(gcProducer.getReceivedEntry(), null);
                done();
            });
        });

        it('should not delete anything when the status entry is a duplicate', done => {
            const entry = ColdStorageStatusQueueEntry.createFromKafkaEntry({ value: message });
            mdObj.setLocation(loc)
                .setDataStoreName('us-east-1')
                .setAmzStorageClass('us-east-1')
                .setArchive(new ObjectMDArchive(entry.archiveInfo));
            backbeatMetadataProxyClient.setMdObj(mdObj);

            archiveTask.processEntry(coldLocation, entry, err => {
                assert.ifError(err);

                assert.strictEqual(coldProducer.getReceivedEntry(), null);
                assert.strictEqual(gcProducer.getReceivedEntry().getActionType(),
                    'deleteArchivedSourceData');
                done();
            });
        });
    });
});
