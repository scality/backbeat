const assert = require('assert');
const { errors } = require('arsenal');
const werelogs = require('werelogs');

const ActionQueueEntry = require('../../../lib/models/ActionQueueEntry');
const LifecycleDeleteObjectTask = require(
    '../../../extensions/lifecycle/tasks/LifecycleDeleteObjectTask');

const {
    S3ClientMock,
    ProcessorMock,
} = require('../mocks');

describe('LifecycleDeleteObjectTask', () => {
    let s3Client;
    let objectProcessor;
    let task;

    beforeEach(() => {
        s3Client = new S3ClientMock();
        objectProcessor = new ProcessorMock(
            s3Client,
            null,
            null,
            null,
            new werelogs.Logger('test:LifecycleDeleteObjectTask'));
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
            assert.ifError(err);
            done();
        });
    });
});
