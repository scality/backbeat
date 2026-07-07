const assert = require('assert');
const sinon = require('sinon');
const { errors } = require('arsenal');
const werelogs = require('werelogs');
const { ObjectMD } = require('arsenal').models;

const ActionQueueEntry = require('../../../lib/models/ActionQueueEntry');
const LifecycleDeleteObjectTask = require(
    '../../../extensions/lifecycle/tasks/LifecycleDeleteObjectTask');
const { LifecycleMetrics } = require('../../../extensions/lifecycle/LifecycleMetrics');

const day = 1000 * 60 * 60 * 24;

const invalidBucketStateError = {
    name: 'InvalidBucketState',
    requestId: 'd4c33f72964c85667de4:89ee7213ce42b2a8d420',
    statusCode: 409,
    retryable: false,
};

const {
    S3ClientMock,
    BackbeatMetadataProxyMock,
    ProcessorMock,
    BackbeatClientMock,
} = require('../mocks');

describe('LifecycleDeleteObjectTask', () => {
    let s3Client;
    let backbeatMdProxyClient;
    let objectProcessor;
    let objMd;
    let task;
    let backbeatClient;

    beforeEach(() => {
        s3Client = new S3ClientMock();
        backbeatMdProxyClient = new BackbeatMetadataProxyMock();
        backbeatClient = new BackbeatClientMock();
        objectProcessor = new ProcessorMock(
            null,
            s3Client,
            backbeatClient,
            backbeatMdProxyClient,
            null,
            null,
            null,
            new werelogs.Logger('test:LifecycleDeleteObjectTask'));
        objMd = new ObjectMD();
        backbeatMdProxyClient.setMdObj(objMd);
        task = new LifecycleDeleteObjectTask(objectProcessor);
    });

    afterEach(() => {
        sinon.restore();
        backbeatMdProxyClient.setError(null);
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
        backbeatClient.setResponse(null, {});
        task.processActionEntry(entry, err => {
            assert.strictEqual(backbeatClient.times.deleteObjectFromExpiration, 1);
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

    [
        'COMPLETED',
        'REPLICA',
        undefined,
    ].forEach(status => {
        it(`should delete replicating object with status: ${status}`, done => {
            objMd.setReplicationStatus(status);
            const entry = ActionQueueEntry.create('deleteObject')
                .setAttribute('target.owner', 'testowner')
                .setAttribute('target.bucket', 'testbucket')
                .setAttribute('target.accountId', 'testid')
                .setAttribute('target.key', 'testkey')
                .setAttribute('target.version', 'testversion')
                .setAttribute('details.lastModified', '2022-05-13T17:51:31.261Z');
            s3Client.setResponse(null, {});
            backbeatClient.setResponse(null, {});
            task.processActionEntry(entry, err => {
                assert.strictEqual(backbeatClient.times.deleteObjectFromExpiration, 1);
                assert.ifError(err);
                done();
            });
        });
    });

    [
        'PENDING',
        'PROCESSING',
        'FAILED',
    ].forEach(status => {
        it(`should skip replicating object with status: ${status}`, done => {
            objMd.setReplicationStatus(status);
            const entry = ActionQueueEntry.create('deleteObject')
                .setAttribute('target.owner', 'testowner')
                .setAttribute('target.bucket', 'testbucket')
                .setAttribute('target.accountId', 'testid')
                .setAttribute('target.key', 'testkey')
                .setAttribute('target.version', 'testversion')
                .setAttribute('details.lastModified', '2022-05-13T17:51:31.261Z');
            s3Client.setResponse(null, {});
            backbeatClient.setResponse(null, {});
            task.processActionEntry(entry, err => {
                assert.strictEqual(backbeatClient.times.deleteObjectFromExpiration, 0);
                assert.ifError(err);
                done();
            });
        });
    });

    // TODO: After the implementation of CLDSRV-461, we could remove this test.
    it('should expire non-versioned object',
        done => {
            objMd.setLegalHold(true);
            const entry = ActionQueueEntry.create('deleteObject')
                .setAttribute('target.owner', 'testowner')
                .setAttribute('target.bucket', 'testbucket')
                .setAttribute('target.accountId', 'testid')
                .setAttribute('target.key', 'testkey')
                .setAttribute('details.lastModified', '2022-05-13T17:51:31.261Z');
            s3Client.setResponse(null, {});
            // <!> Only in S3C <!> Backbeat API returns 'InvalidBucketState' error if the bucket is not versioned
            backbeatMdProxyClient.setError(invalidBucketStateError);
            backbeatClient.setResponse(null, {});
            task.processActionEntry(entry, err => {
                assert.strictEqual(backbeatClient.times.deleteObjectFromExpiration, 1);
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
            backbeatClient.setResponse(null, {});
            task.processActionEntry(entry, err => {
                assert.strictEqual(backbeatClient.times.deleteObjectFromExpiration, 1);
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
            backbeatClient.setResponse(null, {});
            task.processActionEntry(entry, err => {
                assert.strictEqual(backbeatClient.times.deleteObjectFromExpiration, 1);
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
        backbeatClient.setResponse(null, {});
        task.processActionEntry(entry, err => {
            assert.strictEqual(backbeatClient.times.deleteObjectFromExpiration, 1);
            assert.ifError(err);
            done();
        });
    });

    it('should expire object using the deleteObjectFromExpiration method', done => {
        const entry = ActionQueueEntry.create('deleteObject')
            .setAttribute('target.owner', 'testowner')
            .setAttribute('target.bucket', 'testbucket')
            .setAttribute('target.accountId', 'testid')
            .setAttribute('target.key', 'testkey')
            .setAttribute('target.version', 'testversion')
            .setAttribute('details.lastModified', '2022-05-13T17:51:31.261Z');
        s3Client.setResponse(null, {});
        backbeatClient.setResponse(null, {});
        task.processActionEntry(entry, err => {
            assert.ifError(err);
            assert.strictEqual(backbeatClient.times.deleteObjectFromExpiration, 1);
            assert.strictEqual(s3Client.calls.deleteObject, 0);
            done();
        });
    });


    it('should emit expiration metrics with the cold location carried by the entry', done => {
        const startedMetric = sinon.stub(LifecycleMetrics, 'onLifecycleStarted');
        const completedMetric = sinon.stub(LifecycleMetrics, 'onLifecycleCompleted');
        // Simulate a restored cold object: the object metadata carries the
        // warm location the restored copy was written to, while the entry
        // carries the cold location resolved at queue time. The metric must
        // use the entry location, not the metadata one.
        objMd.setDataStoreName('warm-restored-location');
        const entry = ActionQueueEntry.create('deleteObject')
            .setAttribute('target.owner', 'testowner')
            .setAttribute('target.bucket', 'testbucket')
            .setAttribute('target.accountId', 'testid')
            .setAttribute('target.key', 'testkey')
            .setAttribute('target.version', 'testversion')
            .setAttribute('details.dataStoreName', 'cold-location')
            .setAttribute('transitionTime', Date.now() - day);
        backbeatClient.setResponse(null, {});
        task.processActionEntry(entry, err => {
            assert.ifError(err);
            assert.strictEqual(startedMetric.firstCall.args[2], 'cold-location');
            assert.strictEqual(completedMetric.firstCall.args[2], 'cold-location');
            done();
        });
    });

    it('should fall back to the metadata location when the entry does not carry one', done => {
        const startedMetric = sinon.stub(LifecycleMetrics, 'onLifecycleStarted');
        const completedMetric = sinon.stub(LifecycleMetrics, 'onLifecycleCompleted');
        // Entries queued without details.dataStoreName (e.g. expired object
        // delete markers from LifecycleTaskV2): the metric falls back to the
        // location from the object metadata fetched during processing.
        objMd.setDataStoreName('md-location');
        const entry = ActionQueueEntry.create('deleteObject')
            .setAttribute('target.owner', 'testowner')
            .setAttribute('target.bucket', 'testbucket')
            .setAttribute('target.accountId', 'testid')
            .setAttribute('target.key', 'testkey')
            .setAttribute('target.version', 'testversion')
            .setAttribute('transitionTime', Date.now() - day);
        backbeatClient.setResponse(null, {});
        task.processActionEntry(entry, err => {
            assert.ifError(err);
            assert.strictEqual(startedMetric.firstCall.args[2], 'md-location');
            assert.strictEqual(completedMetric.firstCall.args[2], 'md-location');
            done();
        });
    });

    it('should fallback to deleteObject method if deleteObjectFromExpiration is not supported', done => {
        const entry = ActionQueueEntry.create('deleteObject')
            .setAttribute('target.owner', 'testowner')
            .setAttribute('target.bucket', 'testbucket')
            .setAttribute('target.accountId', 'testid')
            .setAttribute('target.key', 'testkey')
            .setAttribute('target.version', 'testversion')
            .setAttribute('details.lastModified', '2022-05-13T17:51:31.261Z');
        s3Client.setResponse(null, {});
        const methodNotAllowedErr = new Error('MethodNotAllowed');
        methodNotAllowedErr.statusCode = 405;
        backbeatClient.setResponse(methodNotAllowedErr, {});
        task.processActionEntry(entry, err => {
            assert.ifError(err);
            assert.strictEqual(backbeatClient.times.deleteObjectFromExpiration, 1);
            assert.strictEqual(s3Client.calls.deleteObject, 1);
            done();
        });
    });

    it('should fail to fallback if it fails to get the s3 client', done => {
        sinon.stub(task, 'getS3Client').returns(null);
        const entry = ActionQueueEntry.create('deleteObject')
            .setAttribute('target.owner', 'testowner')
            .setAttribute('target.bucket', 'testbucket')
            .setAttribute('target.accountId', 'testid')
            .setAttribute('target.key', 'testkey')
            .setAttribute('target.version', 'testversion')
            .setAttribute('details.lastModified', '2022-05-13T17:51:31.261Z');
        s3Client.setResponse(null, {});
        const methodNotAllowedErr = new Error('MethodNotAllowed');
        methodNotAllowedErr.statusCode = 405;
        backbeatClient.setResponse(methodNotAllowedErr, {});
        task.processActionEntry(entry, err => {
            assert(err);
            done();
        });
    });

    it('should abort an MPU using the abortMultipartUpload method', done => {
        const entry = ActionQueueEntry.create('deleteMPU')
            .setAttribute('target.owner', 'testowner')
            .setAttribute('target.bucket', 'testbucket')
            .setAttribute('target.accountId', 'testid')
            .setAttribute('target.key', 'testkey')
            .setAttribute('target.version', 'testversion')
            .setAttribute('details.UploadId', 'someUploadId')
            .setAttribute('details.lastModified', '2022-05-13T17:51:31.261Z');
        s3Client.setResponse(null, {});
        backbeatClient.setResponse(null, {});
        task.processActionEntry(entry, err => {
            assert.ifError(err);
            assert.strictEqual(s3Client.calls.abortMultipartUpload, 1);
            assert.strictEqual(s3Client.calls.deleteObject, 0);
            assert.strictEqual(backbeatClient.times.deleteObjectFromExpiration, 0);
            done();
        });
    });

    it('should return an error when it can\'t get the BackbeatClient', done => {
        sinon.stub(task, 'getBackbeatClient').returns(null);
        const entry = ActionQueueEntry.create('deleteObject')
            .setAttribute('target.owner', 'testowner')
            .setAttribute('target.bucket', 'testbucket')
            .setAttribute('target.accountId', 'testid')
            .setAttribute('target.key', 'testkey')
            .setAttribute('target.version', 'testversion')
            .setAttribute('details.lastModified', '2022-05-13T17:51:31.261Z');
        s3Client.setResponse(null, {});
        backbeatClient.setResponse(null, {});
        task.processActionEntry(entry, err => {
            assert(err);
            done();
        });
    });

    it('should return an error when it can\'t get the S3 client', done => {
        sinon.stub(task, 'getS3Client').returns(null);
        const entry = ActionQueueEntry.create('deleteMPU')
            .setAttribute('target.owner', 'testowner')
            .setAttribute('target.bucket', 'testbucket')
            .setAttribute('target.accountId', 'testid')
            .setAttribute('target.key', 'testkey')
            .setAttribute('target.version', 'testversion')
            .setAttribute('details.UploadId', 'someUploadId')
            .setAttribute('details.lastModified', '2022-05-13T17:51:31.261Z');
        s3Client.setResponse(null, {});
        backbeatClient.setResponse(null, {});
        task.processActionEntry(entry, err => {
            assert(err);
            done();
        });
    });
});
