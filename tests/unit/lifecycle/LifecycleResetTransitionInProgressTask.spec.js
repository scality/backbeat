const assert = require('assert');
const werelogs = require('werelogs');

const { ObjectMD, ObjectMDArchive } = require('@scality/arsenal').models;
const ActionQueueEntry = require('../../../lib/models/ActionQueueEntry');
const { LifecycleResetTransitionInProgressTask } = require(
    '../../../extensions/lifecycle/tasks/LifecycleResetTransitionInProgressTask');

const {
    BackbeatMetadataProxyMock,
    GarbageCollectorProducerMock,
    ProcessorMock,
} = require('../mocks');

describe('LifecycleResetTransitionInProgressTask', () => {
    let backbeatMetadataProxyClient;
    let gcProducer;
    let objectProcessor;
    let task;

    const byAccount = {
        123: {
            bucket1: [{
                objectKey: 'obj1',
                objectVersion: 'v1',
                eTag: '"etag1"',
                try: 12,
            }]
        },
    };
    const actionEntry = ActionQueueEntry.create('requeueTransition')
        .setAttribute('target', { byAccount, location: 'location-dmf-v1' });

    const objectNotTransitioning = new ObjectMD()
        .setContentMd5('etag1');
    const objectTransitioning = new ObjectMD()
        .setContentMd5('etag1')
        .setTransitionInProgress(true)
        .setUserMetadata({
            'x-amz-meta-scal-s3-transition-attempt': 11,
        });
    // "direct" transition: the cold storage class was requested in the PUT request, and the
    // data still lies in the hot location
    const objectDirectTransitioning = new ObjectMD()
        .setContentMd5('etag1')
        .setTransitionInProgress(true)
        .setAmzStorageClass('location-dmf-v1')
        .setDataStoreName('us-east-1');
    const hotLocation = [{ key: 'data-1', dataStoreName: 'us-east-1', size: 42, start: 0 }];
    // archived, waiting for the garbage collector to remove the hot data
    const objectDirectArchived = new ObjectMD()
        .setContentMd5('etag1')
        .setOwnerId('owner-1')
        .setTransitionInProgress(true)
        .setAmzStorageClass('location-dmf-v1')
        .setDataStoreName('us-east-1')
        .setLocation(hotLocation)
        .setArchive(new ObjectMDArchive({ archiveId: 'archive-1' }));
    // same, for a lifecycle transition
    const objectLifecycleArchived = new ObjectMD()
        .setContentMd5('etag1')
        .setOwnerId('owner-1')
        .setTransitionInProgress(true)
        .setAmzStorageClass('STANDARD')
        .setDataStoreName('us-east-1')
        .setLocation(hotLocation)
        .setArchive(new ObjectMDArchive({ archiveId: 'archive-1' }));
    // empty object: nothing to garbage collect
    const emptyObjectDirectArchived = new ObjectMD()
        .setContentMd5('etag1')
        .setOwnerId('owner-1')
        .setTransitionInProgress(true)
        .setAmzStorageClass('location-dmf-v1')
        .setDataStoreName('us-east-1')
        .setUserMetadata({ 'x-amz-meta-scal-s3-transition-attempt': 11 })
        .setArchive(new ObjectMDArchive({ archiveId: 'archive-1' }, '2017-07-11T02:44:25.515Z', 3));
    const objectDirectTransitioned = new ObjectMD()
        .setContentMd5('etag1')
        .setTransitionInProgress(true)
        .setAmzStorageClass('location-dmf-v1')
        .setDataStoreName('location-dmf-v1');

    beforeEach(() => {
        backbeatMetadataProxyClient = new BackbeatMetadataProxyMock();
        gcProducer = new GarbageCollectorProducerMock();

        objectProcessor = new ProcessorMock(
            null,
            null,
            null,
            backbeatMetadataProxyClient,
            gcProducer,
            null,
            null,
            new werelogs.Logger('test:LifecycleResetTransitionInProgressTask'));

        task = new LifecycleResetTransitionInProgressTask(objectProcessor);
    });

    it('should skip object not transitioning', done => {
        backbeatMetadataProxyClient.setMdObj(objectNotTransitioning);
        task.processActionEntry(actionEntry, err => {
            assert.ifError(err);
            assert.ok(backbeatMetadataProxyClient.receivedMd === null);

            done();
        });
    });

    it('should store current attempt in user metadata', done => {
        backbeatMetadataProxyClient.setMdObj(objectTransitioning);
        task.processActionEntry(actionEntry, err => {
            assert.ifError(err);

            const md = backbeatMetadataProxyClient.mdObj;
            const umd =  JSON.parse(md.getUserMetadata());
            const attempts = umd['x-amz-meta-scal-s3-transition-attempt'];
            assert.deepStrictEqual(12, attempts);

            done();
        });
    });

    it('should reset transition in progress flag', done => {
        backbeatMetadataProxyClient.setMdObj(objectTransitioning);
        task.processActionEntry(actionEntry, err => {
            assert.ifError(err);

            const md = backbeatMetadataProxyClient.mdObj;
            assert.ok(!md.getTransitionInProgress());
            assert.strictEqual(md.getOriginOp(), 's3:LifecycleTransition:Retry');

            done();
        });
    });

    it('should keep transition in progress flag for a direct transition', done => {
        backbeatMetadataProxyClient.setMdObj(objectDirectTransitioning);
        task.processActionEntry(actionEntry, err => {
            assert.ifError(err);

            const md = backbeatMetadataProxyClient.mdObj;
            assert.ok(md.getTransitionInProgress());
            assert.strictEqual(md.getOriginOp(), 's3:LifecycleTransition:Retry');
            const umd = JSON.parse(md.getUserMetadata());
            assert.strictEqual(umd['x-amz-meta-scal-s3-transition-attempt'], 12);

            done();
        });
    });

    describe('already archived object', () => {
        function assertGcEntry(gcEntry, oldLocation, newLocation) {
            assert.strictEqual(gcEntry.getActionType(), 'deleteArchivedSourceData');
            assert.deepStrictEqual(gcEntry.getAttribute('target'), {
                oldLocation,
                newLocation,
                bucket: 'bucket1',
                key: 'obj1',
                version: 'v1',
                accountId: '123',
                owner: 'owner-1',
            });
            assert.strictEqual(gcEntry.getAttribute('serviceName'), 'lifecycle-transition');
            assert.strictEqual(gcEntry.getContextAttribute('ruleType'), 'archive');
        }

        it('should release the hot data of a direct transition', done => {
            backbeatMetadataProxyClient.setMdObj(objectDirectArchived);
            task.processActionEntry(actionEntry, err => {
                assert.ifError(err);
                // the garbage collector updates the metadata once the data is released
                assert.strictEqual(backbeatMetadataProxyClient.receivedMd, null);
                assertGcEntry(gcProducer.getReceivedEntry(), 'us-east-1', 'location-dmf-v1');

                done();
            });
        });

        it('should release the hot data of a lifecycle transition', done => {
            backbeatMetadataProxyClient.setMdObj(objectLifecycleArchived);
            task.processActionEntry(actionEntry, err => {
                assert.ifError(err);
                assert.strictEqual(backbeatMetadataProxyClient.receivedMd, null);
                assertGcEntry(gcProducer.getReceivedEntry(), 'us-east-1', 'location-dmf-v1');

                done();
            });
        });

        it('should skip when the requeue message does not give a cold location', done => {
            backbeatMetadataProxyClient.setMdObj(objectDirectArchived);
            const entry = ActionQueueEntry.create('requeueTransition')
                .setAttribute('target', { byAccount, location: 'us-east-1' });
            task.processActionEntry(entry, err => {
                assert.ifError(err);
                assert.strictEqual(backbeatMetadataProxyClient.receivedMd, null);
                assert.strictEqual(gcProducer.getReceivedEntry(), null);

                done();
            });
        });

        it('should complete the transition of an empty object without garbage collection', done => {
            backbeatMetadataProxyClient.setMdObj(emptyObjectDirectArchived);
            task.processActionEntry(actionEntry, err => {
                assert.ifError(err);
                assert.strictEqual(gcProducer.getReceivedEntry(), null);

                const md = backbeatMetadataProxyClient.mdObj;
                assert.strictEqual(md.getDataStoreName(), 'location-dmf-v1');
                assert.strictEqual(md.getAmzStorageClass(), 'location-dmf-v1');
                assert.ok(!md.getTransitionInProgress());
                assert.strictEqual(md.getOriginOp(), 's3:LifecycleTransition:Direct');
                assert.ok(!md.getUserMetadata()?.includes('x-amz-meta-scal-s3-transition-attempt'));
                // the deferred restore request is kept for the populator to re-drive
                assert.strictEqual(md.getArchive().restoreRequestedAt, '2017-07-11T02:44:25.515Z');

                done();
            });
        });
    });

    it('should reset transition in progress flag once the object is in the cold location', done => {
        backbeatMetadataProxyClient.setMdObj(objectDirectTransitioned);
        task.processActionEntry(actionEntry, err => {
            assert.ifError(err);

            const md = backbeatMetadataProxyClient.mdObj;
            assert.ok(!md.getTransitionInProgress());

            done();
        });
    });
});
