const assert = require('assert');
const werelogs = require('werelogs');

const { ObjectMD, ObjectMDArchive } = require('arsenal').models;
const ActionQueueEntry = require('../../../lib/models/ActionQueueEntry');
const { LifecycleRetriggerRestoreTask } = require(
    '../../../extensions/lifecycle/tasks/LifecycleRetriggerRestoreTask');

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
                        try: 12,
                    }]
                },
            },
        });

    const objectNotArchived = new ObjectMD();
    const objectAlreadyRestored = new ObjectMD().setArchive(
        new ObjectMDArchive({
            archiveId: '123456789',
            }, Date.now(), 1, Date.now(), undefined));
    const objectRestoreNotRequested = new ObjectMD().setArchive(
                new ObjectMDArchive({
                    archiveId: '123456789',
                    }));
    const objectRestoreExpired = new ObjectMD().setArchive(
        new ObjectMDArchive({
            archiveId: '123456789',
            }, Date.now() - 1000, 1, Date.now() - 999, Date.now() - 10));
    const archiveInfo = new ObjectMDArchive({
        archiveId: '123456789'
    }, Date.now(), 1, undefined, undefined);
    const objectArchived = new ObjectMD()
        .setArchive(archiveInfo)
        .setUserMetadata({
            'x-amz-meta-scal-s3-restore-attempt': 11,
        });

    beforeEach(() => {
        backbeatMetadataProxyClient = new BackbeatMetadataProxyMock();

        objectProcessor = new ProcessorMock(
            null,
            null,
            backbeatMetadataProxyClient,
            null,
            new werelogs.Logger('test:LifecycleRetriggerRestoreTask'));

        task = new LifecycleRetriggerRestoreTask(objectProcessor);
    });

    it('should skip object not archived', done => {
        backbeatMetadataProxyClient.setMdObj(objectNotArchived);
        task.processActionEntry(actionEntry, err => {
            assert.ifError(err);
            assert.ok(backbeatMetadataProxyClient.receivedMd === null);

            done();
        });
    });

    it('should skip object when restore is expired', done => {
        backbeatMetadataProxyClient.setMdObj(objectRestoreExpired);
        task.processActionEntry(actionEntry, err => {
            assert.ifError(err);
            assert.ok(backbeatMetadataProxyClient.receivedMd === null);

            done();
        });
    });

    it('should skip object already restored', done => {
        backbeatMetadataProxyClient.setMdObj(objectAlreadyRestored);
        task.processActionEntry(actionEntry, err => {
            assert.ifError(err);
            assert.ok(backbeatMetadataProxyClient.receivedMd === null);

            done();
        });
    });

    it('should skip object not requested to restore', done => {
        backbeatMetadataProxyClient.setMdObj(objectRestoreNotRequested);
        task.processActionEntry(actionEntry, err => {
            assert.ifError(err);
            assert.ok(backbeatMetadataProxyClient.receivedMd === null);

            done();
        });
    });

    it('should store current attempt in user metadata', done => {
        backbeatMetadataProxyClient.setMdObj(objectArchived);
        task.processActionEntry(actionEntry, err => {
            assert.ifError(err);

            const md = backbeatMetadataProxyClient.mdObj;
            const umd =  JSON.parse(md.getUserMetadata());
            const attempts = umd['x-amz-meta-scal-s3-restore-attempt'];
            assert.deepStrictEqual(12, attempts);

            done();
        });
    });

    it('should set originOp to s3:ObjectRestore:Retry', done => {
        backbeatMetadataProxyClient.setMdObj(objectArchived);
        task.processActionEntry(actionEntry, err => {
            assert.ifError(err);

            const md = backbeatMetadataProxyClient.mdObj;
            assert.ok(md.getOriginOp() === 's3:ObjectRestore:Retry');

            done();
        });
    });
});
