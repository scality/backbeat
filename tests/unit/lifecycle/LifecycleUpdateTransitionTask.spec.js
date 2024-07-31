const assert = require('assert');
const werelogs = require('werelogs');

const { ObjectMD } = require('arsenal').models;
const ActionQueueEntry = require('../../../lib/models/ActionQueueEntry');
const LifecycleUpdateTransitionTask = require(
    '../../../extensions/lifecycle/tasks/LifecycleUpdateTransitionTask');

const {
    GarbageCollectorProducerMock,
    BackbeatMetadataProxyMock,
    ProcessorMock,
} = require('../mocks');
const { errors } = require('arsenal');

describe('LifecycleUpdateTransitionTask', () => {
    let backbeatMetadataProxyClient;
    let gcProducer;
    let objectProcessor;
    let mdObj;
    let actionEntry;
    let oldLocation;
    let newLocation;
    let task;
    beforeEach(() => {
        oldLocation = [{
            key: 'oldLocationKey',
            size: 10,
            start: 0,
            dataStoreName: 'oldLocationName',
            dataStoreType: 'aws_s3',
            dataStoreETag: 'oldETag',
            dataStoreVersionId: 'oldVersionId',
        }];
        newLocation = [{
            key: 'newLocationKey',
            size: 20,
            start: 0,
            dataStoreName: 'newLocationName',
            dataStoreType: 'gcp',
            dataStoreETag: 'newETag',
            dataStoreVersionId: 'newVersionId',
        }];

        backbeatMetadataProxyClient = new BackbeatMetadataProxyMock();
        gcProducer = new GarbageCollectorProducerMock();
        objectProcessor = new ProcessorMock(
            null,
            null,
            null,
            backbeatMetadataProxyClient,
            gcProducer,
            null,
            new werelogs.Logger('test:LifecycleUpdateTransitionTask'));
        mdObj = new ObjectMD();
        mdObj.setLocation(oldLocation)
            .setTransitionInProgress(true)
            .setContentMd5('1ccc7006b902a4d30ec26e9ddcf759d8')
            .setLastModified('1970-01-01T00:00:00.000Z');
        backbeatMetadataProxyClient.setMdObj(mdObj);
        actionEntry = ActionQueueEntry.create('copyLocation')
            .setAttribute('target', {
                bucket: 'somebucket',
                key: 'somekey',
                eTag: '"1ccc7006b902a4d30ec26e9ddcf759d8"',
                lastModified: '1970-01-01T00:00:00.000Z'
            })
            .setSuccess({ location: newLocation });
        task = new LifecycleUpdateTransitionTask(objectProcessor);
    });

    it('should transition location in metadata', done => {
        task.processActionEntry(actionEntry, err => {
            assert.ifError(err);
            const receivedMd = backbeatMetadataProxyClient.getReceivedMd();
            assert.deepStrictEqual(receivedMd.location, newLocation);
            const receivedGcEntry = gcProducer.getReceivedEntry();
            assert.strictEqual(receivedGcEntry.getActionType(), 'deleteData');
            assert.deepStrictEqual(
                receivedGcEntry.getAttribute('target.locations'), oldLocation);
            done();
        });
    });

    it('should not transition location in metadata and should GC new ' +
    'location if eTag does not match', done => {
        actionEntry.setAttribute('target.eTag',
                                 '"6713e7cf89b6b16d5abf11d1fabac587"');
        task.processActionEntry(actionEntry, err => {
            assert.ifError(err);
            const receivedMd = backbeatMetadataProxyClient.getReceivedMd();
            assert.strictEqual(receivedMd, null);
            const receivedGcEntry = gcProducer.getReceivedEntry();
            assert.strictEqual(receivedGcEntry.getActionType(), 'deleteData');
            assert.deepStrictEqual(
                receivedGcEntry.getAttribute('target.locations'), newLocation);
            done();
        });
    });

    it('should not update metadata nor GC anything if location does not change',
    done => {
        mdObj.setLocation(newLocation);
        task.processActionEntry(actionEntry, err => {
            assert.ifError(err);
            const receivedMd = backbeatMetadataProxyClient.getReceivedMd();
            assert.strictEqual(receivedMd, null);
            const receivedGcEntry = gcProducer.getReceivedEntry();
            assert.strictEqual(receivedGcEntry, null);
            done();
        });
    });

    it('should not transition location in metadata and should GC new ' +
    'location if LastModified does not match', done => {
        actionEntry.setAttribute('target.lastModified',
                                 '1970-01-01T00:00:00.001Z');
        task.processActionEntry(actionEntry, err => {
            assert.ifError(err);
            const receivedMd = backbeatMetadataProxyClient.getReceivedMd();
            assert.strictEqual(receivedMd, null);
            const receivedGcEntry = gcProducer.getReceivedEntry();
            assert.strictEqual(receivedGcEntry.getActionType(), 'deleteData');
            assert.deepStrictEqual(
                receivedGcEntry.getAttribute('target.locations'), newLocation);
            done();
        });
    });

    it('should transition location in metadata if LastModified is not ' +
    'provided as a check', done => {
        actionEntry.setAttribute('target', {
            bucket: 'somebucket',
            key: 'somekey',
            eTag: '"1ccc7006b902a4d30ec26e9ddcf759d8"',
            lastModified: undefined,
        });
        task.processActionEntry(actionEntry, err => {
            assert.ifError(err);
            const receivedMd = backbeatMetadataProxyClient.getReceivedMd();
            assert.deepStrictEqual(receivedMd.location, newLocation);
            const receivedGcEntry = gcProducer.getReceivedEntry();
            assert.strictEqual(receivedGcEntry.getActionType(), 'deleteData');
            assert.deepStrictEqual(
                receivedGcEntry.getAttribute('target.locations'), oldLocation);
            done();
        });
    });

    it('should reset transition-in-progress flag when transition fails', done => {
        actionEntry.setError(errors.InternalError);
        task.processActionEntry(actionEntry, err => {
            assert.ifError(err);
            const receivedMd = backbeatMetadataProxyClient.getReceivedMd();
            assert.deepStrictEqual(receivedMd['x-amz-meta-scal-s3-transition-attempt'], 1);
            assert.deepStrictEqual(receivedMd['x-amz-scal-transition-in-progress'], false);
            done();
        });
    });

    it('should increment transition attempt count when transition fails', done => {
        actionEntry.setError(errors.InternalError);
        mdObj.setUserMetadata({ 'x-amz-meta-scal-s3-transition-attempt': 2 });
        task.processActionEntry(actionEntry, err => {
            assert.ifError(err);
            const receivedMd = backbeatMetadataProxyClient.getReceivedMd();
            assert.deepStrictEqual(receivedMd['x-amz-meta-scal-s3-transition-attempt'], 3);
            done();
        });
    });
});
