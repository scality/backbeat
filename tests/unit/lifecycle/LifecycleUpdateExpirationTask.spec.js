const assert = require('assert');
const werelogs = require('werelogs');

const { ObjectMD } = require('arsenal').models;
const ActionQueueEntry = require('../../../lib/models/ActionQueueEntry');
const LifecycleUpdateExpirationTask = require(
    '../../../extensions/lifecycle/tasks/LifecycleUpdateExpirationTask');

const {
    GarbageCollectorProducerMock,
    BackbeatMetadataProxyMock,
    ProcessorMock,
} = require('../mocks');

describe('LifecycleUpdateExpirationTask', () => {
    let backbeatMetadataProxyClient;
    let gcProducer;
    let objectProcessor;
    let actionEntry;
    let oldLocation;
    let task;
    beforeEach(() => {
        oldLocation = [{
            key: '3-A8EFF8D86D82EAE12FD2A17188A3E64B9D3E0000-60D',
            size: 10,
            start: 0,
            dataStoreName: 'us-east-1',
            dataStoreType: 'scality',
            dataStoreETag: 'oldETag',
            dataStoreVersionId: '1:5c800459000ee371f7a7983b565b4fa4',
        }];

        backbeatMetadataProxyClient = new BackbeatMetadataProxyMock();
        gcProducer = new GarbageCollectorProducerMock();
        objectProcessor = new ProcessorMock(
            null,
            null,
            backbeatMetadataProxyClient,
            gcProducer,
            new werelogs.Logger('test:LifecycleUpdateExpirationTask'));
        actionEntry = ActionQueueEntry.create('deleteObject')
            .setAttribute('target', {
                owner: 'eab6642741045d0ae7cb3333962ad56f847ce0d9bb73de98eb4959428fc28108',
                bucket: 'somebucket',
                key: 'somekey',
                accountId: '871467171849',
                location: 'location-dmf-v1',
            })
            .setAttribute('details', {
                lastModified: '2023-06-02T12:50:57.016Z',
            })
            .addContext('details', {
                origin: 'lifecycle',
                ruleType: 'expiration',
                reqId: '8b902aef7346801d99fc',
            });
        task = new LifecycleUpdateExpirationTask(objectProcessor);
    });

    it('should expire restored object', done => {
        const requestedAt = new Date();
        const restoreCompletedAt = new Date();
        const expireDate = new Date();
        const mdObj = new ObjectMD();
        mdObj.setKey('somekey')
            .setLocation(oldLocation)
            .setContentMd5('1ccc7006b902a4d30ec26e9ddcf759d8')
            .setLastModified('1970-01-01T00:00:00.000Z')
            .setArchive({
                archiveInfo: {
                    archiveId: '7206409b-7a26-484f-8452-494a0f0ba708',
                    archiveVersion: 5577006791947779,
                },
                restoreRequestedAt: requestedAt.setDate(requestedAt.getDate() - 2),
                restoreRequestedDays: 1,
                restoreCompletedAt: restoreCompletedAt.setDate(restoreCompletedAt.getDate() - 1),
                restoreWillExpireAt: expireDate,
            });
        backbeatMetadataProxyClient.setMdObj(mdObj);
        task.processActionEntry(actionEntry, err => {
            assert.ifError(err);
            const receivedMd = backbeatMetadataProxyClient.getReceivedMd();
            assert.deepStrictEqual(receivedMd.archive, {
                archiveInfo: {
                    archiveId: '7206409b-7a26-484f-8452-494a0f0ba708',
                    archiveVersion: 5577006791947779
                  }
            });
            assert.strictEqual(receivedMd['x-amz-storage-class'], 'location-dmf-v1');
            assert.strictEqual(receivedMd.dataStoreName, 'location-dmf-v1');
            const receivedGcEntry = gcProducer.getReceivedEntry();
            assert.strictEqual(receivedGcEntry.getActionType(), 'deleteData');
            assert.deepStrictEqual(receivedGcEntry.getAttribute('target.locations'), oldLocation);
            done();
        });
    });

    it('should not expire restored object when the expire delay got updated', done => {
        const requestedAt = new Date();
        const restoreCompletedAt = new Date();
        const expireDate = new Date();
        const mdObj = new ObjectMD();
        mdObj.setKey('somekey')
            .setLocation(oldLocation)
            .setContentMd5('1ccc7006b902a4d30ec26e9ddcf759d8')
            .setLastModified('1970-01-01T00:00:00.000Z')
            .setArchive({
                archiveInfo: {
                    archiveId: '7206409b-7a26-484f-8452-494a0f0ba708',
                    archiveVersion: 5577006791947779,
                },
                restoreRequestedAt: requestedAt.setDate(requestedAt.getDate() - 2),
                restoreRequestedDays: 1,
                restoreCompletedAt: restoreCompletedAt.setDate(restoreCompletedAt.getDate() - 1),
                restoreWillExpireAt: expireDate.setDate(expireDate.getDate() + 2),
            });
        backbeatMetadataProxyClient.setMdObj(mdObj);
        task.processActionEntry(actionEntry, err => {
            assert.ifError(err);
            const receivedMd = backbeatMetadataProxyClient.getReceivedMd();
            assert.strictEqual(receivedMd, null);
            const receivedGcEntry = gcProducer.getReceivedEntry();
            assert.strictEqual(receivedGcEntry, null);
            done();
        });
    });
});
