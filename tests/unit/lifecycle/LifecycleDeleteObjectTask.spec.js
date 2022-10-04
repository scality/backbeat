const assert = require('assert');
const { errors } = require('arsenal');
const werelogs = require('werelogs');
const { ObjectMD } = require('arsenal').models;

const ActionQueueEntry = require('../../../lib/models/ActionQueueEntry');
const LifecycleDeleteObjectTask = require(
    '../../../extensions/lifecycle/tasks/LifecycleDeleteObjectTask');

const day = 1000 * 60 * 60 * 24;

const {
    S3ClientMock,
    BackbeatMetadataProxyMock,
    ProcessorMock,
} = require('../mocks');

describe('LifecycleDeleteObjectTask', () => {
    let s3Client;
    let backbeatClient;
    let objectProcessor;
    let objMd;
    let task;

    beforeEach(() => {
        s3Client = new S3ClientMock();
        backbeatClient = new BackbeatMetadataProxyMock();
        objectProcessor = new ProcessorMock(
            s3Client,
            null,
            backbeatClient,
            null,
            new werelogs.Logger('test:LifecycleDeleteObjectTask'));
        objMd = new ObjectMD();
        backbeatClient.setMdObj(objMd);
        task = new LifecycleDeleteObjectTask(objectProcessor);
    });

    it('should not return error for 404s', done => {
        const entry = ActionQueueEntry.create('deleteObject')
            .setAttribute('target.owner', 'testowner')
            .setAttribute('target.bucket', 'testbucket')
            .setAttribute('target.accountId', 'testid')
            .setAttribute('target.key', 'testkey')
            .setAttribute('details.lastModified', '2022-05-13T17:51:31.261Z');
        const error = errors.NoSuchKey;
        error.statusCode = error.code;

        s3Client.setResponse(error, null);
        task.processActionEntry(entry, done);
    });

    it('should return error non-404 errors', done => {
        const entry = ActionQueueEntry.create('deleteObject')
            .setAttribute('target.owner', 'testowner')
            .setAttribute('target.bucket', 'testbucket')
            .setAttribute('target.accountId', 'testid')
            .setAttribute('target.key', 'testkey')
            .setAttribute('details.lastModified', '2022-05-13T17:51:31.261Z');
        const error = errors.PreconditionFailed;
        error.statusCode = error.code;

        s3Client.setResponse(error, null);
        task.processActionEntry(entry, err => {
            assert(err);
            done();
        });
    });

    it('successful request', done => {
        const entry = ActionQueueEntry.create('deleteObject')
            .setAttribute('target.owner', 'testowner')
            .setAttribute('target.bucket', 'testbucket')
            .setAttribute('target.accountId', 'testid')
            .setAttribute('target.key', 'testkey')
            .setAttribute('details.lastModified', '2022-05-13T17:51:31.261Z');
        s3Client.setResponse(null, {});
        task.processActionEntry(entry, err => {
            assert.strictEqual(s3Client.calls.deleteObject, 1);
            assert.ifError(err);
            done();
        });
    });

    it('should skip locked object: legal hold', done => {
        objMd.setLegalHold(true);
        const entry = ActionQueueEntry.create('deleteObject')
            .setAttribute('target.owner', 'testowner')
            .setAttribute('target.bucket', 'testbucket')
            .setAttribute('target.accountId', 'testid')
            .setAttribute('target.key', 'testkey')
            .setAttribute('target.version', 'testversion')
            .setAttribute('details.lastModified', '2022-05-13T17:51:31.261Z');
        s3Client.setResponse(null, {});
        task.processActionEntry(entry, err => {
            assert.strictEqual(s3Client.calls.deleteObject, 0);
            assert.ifError(err);
            done();
        });
    });

    it('should expire current version of locked object with legal hold',
        done => {
            objMd.setLegalHold(true);
            const entry = ActionQueueEntry.create('deleteObject')
                .setAttribute('target.owner', 'testowner')
                .setAttribute('target.bucket', 'testbucket')
                .setAttribute('target.accountId', 'testid')
                .setAttribute('target.key', 'testkey')
                .setAttribute('details.lastModified', '2022-05-13T17:51:31.261Z');
            s3Client.setResponse(null, {});
            task.processActionEntry(entry, err => {
                assert.strictEqual(s3Client.calls.deleteObject, 1);
                assert.ifError(err);
                done();
            });
        });

    it('should skip locked object: retention date', done => {
        objMd.setRetentionDate(new Date(Date.now() + day));
        objMd.setRetentionMode('GOVERNANCE');
        const entry = ActionQueueEntry.create('deleteObject')
            .setAttribute('target.owner', 'testowner')
            .setAttribute('target.bucket', 'testbucket')
            .setAttribute('target.accountId', 'testid')
            .setAttribute('target.key', 'testkey')
            .setAttribute('target.version', 'testversion')
            .setAttribute('details.lastModified', '2022-05-13T17:51:31.261Z');
        s3Client.setResponse(null, {});
        task.processActionEntry(entry, err => {
            assert.strictEqual(s3Client.calls.deleteObject, 0);
            assert.ifError(err);
            done();
        });
    });

    it('should expire current version of locked object with retention date',
        done => {
            objMd.setRetentionDate(new Date(Date.now() + day));
            objMd.setRetentionMode('GOVERNANCE');
            const entry = ActionQueueEntry.create('deleteObject')
                .setAttribute('target.owner', 'testowner')
                .setAttribute('target.bucket', 'testbucket')
                .setAttribute('target.accountId', 'testid')
                .setAttribute('target.key', 'testkey')
                .setAttribute('details.lastModified', '2022-05-13T17:51:31.261Z');
            s3Client.setResponse(null, {});
            task.processActionEntry(entry, err => {
                assert.strictEqual(s3Client.calls.deleteObject, 1);
                assert.ifError(err);
                done();
            });
        });

    it('should delete locked object with valid date', done => {
        objMd.setRetentionDate(new Date(Date.now() - day));
        objMd.setRetentionMode('GOVERNANCE');
        const entry = ActionQueueEntry.create('deleteObject')
            .setAttribute('target.owner', 'testowner')
            .setAttribute('target.bucket', 'testbucket')
            .setAttribute('target.accountId', 'testid')
            .setAttribute('target.key', 'testkey')
            .setAttribute('target.version', 'testversion')
            .setAttribute('details.lastModified', '2022-05-13T17:51:31.261Z');
        s3Client.setResponse(null, {});
        task.processActionEntry(entry, err => {
            assert.strictEqual(s3Client.calls.deleteObject, 1);
            assert.ifError(err);
            done();
        });
    });
});
