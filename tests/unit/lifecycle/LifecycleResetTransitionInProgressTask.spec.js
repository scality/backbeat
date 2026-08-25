const assert = require('assert');
const werelogs = require('werelogs');

const { ObjectMD } = require('arsenal').models;
const ActionQueueEntry = require('../../../lib/models/ActionQueueEntry');
const { LifecycleResetTransitionInProgressTask } = require(
    '../../../extensions/lifecycle/tasks/LifecycleResetTransitionInProgressTask');

const {
    BackbeatMetadataProxyMock,
    ProcessorMock,
} = require('../mocks');

describe('LifecycleResetTransitionInProgressTask', () => {
    let backbeatMetadataProxyClient;
    let objectProcessor;
    let task;

    const actionEntry = ActionQueueEntry.create('requeueTransition')
        .setAttribute('target', {
            byAccount: {
                123: {
                    bucket1: [{
                        objectKey: 'obj1',
                        objectVersion: 'v1',
                        eTag: '"etag1"',
                        try: 12,
                    }]
                },
            },
        });

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
    const objectDirectTransitioned = new ObjectMD()
        .setContentMd5('etag1')
        .setTransitionInProgress(true)
        .setAmzStorageClass('location-dmf-v1')
        .setDataStoreName('location-dmf-v1');

    beforeEach(() => {
        backbeatMetadataProxyClient = new BackbeatMetadataProxyMock();

        objectProcessor = new ProcessorMock(
            null,
            null,
            null,
            backbeatMetadataProxyClient,
            null,
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
