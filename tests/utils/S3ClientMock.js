'use strict'; // eslint-disable-line

const assert = require('assert');
const { errors } = require('arsenal');

class S3ClientMock {
    constructor(failures) {
        this.failures = failures;

        this.calls = {};
        this.stubMethod('deleteObject', {});
        this.stubMethod('abortMultipartUpload', {});
        this.stubMethod('getBucketVersioning', {});
        this.stubMethod('listObjects', {
            Contents: [
                {
                    Key: 'obj1',
                    LastModified: '2021-10-04T21:46:49.157Z',
                },
            ],
        });
        this.stubMethod('headObject', {
            LastModified: '2021-10-04T21:46:49.157Z',
        });
        this.stubMethod('listMultipartUploads', {
            Uploads: [],
        });
        this.stubMethod('getBucketLifecycleConfiguration', {
            Rules: [
                {
                    Expiration: {
                        Days: 1,
                    },
                    ID: 'id',
                    Prefix: '',
                    Status: 'Enabled',
                },
            ],
        });
        this.stubMethod('getObjectTagging', {
            TagSet: [{ Key: 'key', Value: 'val' }],
        });
        this.stubMethod('listObjectVersions', {
            IsTruncated: true,
            DeleteMarkers: [],
            Versions: [],
        });
    }

    makeRetryableError() {
        const err = errors.ServiceUnavailable.customizeDescription('failing on purpose');
        err.retryable = true;
        return err;
    }

    stubMethod(methodName, successResult, stubError) {
        this.calls[methodName] = 0;

        this[methodName] = (params, done) => {
            this.calls[methodName]++;

            if (this.failures[methodName] >= this.calls[methodName]) {
                const error = stubError || this.makeRetryableError();

                if (done) {
                    return process.nextTick(done, error);
                }

                return {
                    send: cb => process.nextTick(cb, error),
                    on: () => {},
                };
            }

            if (done) {
                return process.nextTick(done, null, successResult);
            }

            return {
                send: cb => process.nextTick(cb, null, successResult),
                on: () => {},
            };
        };
    }

    verifyRetries() {
        Object.keys(this.failures).forEach(f => {
            assert.strictEqual(this.calls[f], this.failures[f] + 1,
                `did not retry ${this.failures[f]} times`);
        });
    }

    verifyNoRetries() {
        Object.keys(this.failures).forEach(f => {
            assert.strictEqual(this.calls[f], 1,
                `called ${this.calls[f]} times, expected 1`);
        });
    }

    stubListObjectsTruncated() {
        this.stubMethod('listObjects', {
            IsTruncated: true,
            Contents: [
                {
                    Key: 'obj1',
                    LastModified: '2021-10-04T21:46:49.157Z',
                    ETag: '1:3749f52bb326ae96782b42dc0a97b4c1',
                    Size: 1,
                    StorageClass: 'site1',
                },
            ],
        });
        return this;
    }

    stubListVersionsTruncated() {
        this.stubMethod('listObjectVersions', {
            IsTruncated: true,
            DeleteMarkers: [],
            Versions: [
                {
                    Key: 'v1',
                    ETag: '1:3749f52bb326ae96782b42dc0a97b4c1',
                    Size: 1,
                    StorageClass: 'site1',
                    IsLatest: true,
                    LastModified: '2021-10-04T21:46:49.157Z',
                },
            ],
        });
        return this;
    }

    stubListMpuTruncated() {
        this.stubMethod('listMultipartUploads', {
            IsTruncated: true,
            Uploads: [{
                Initiated: '2021-10-04T21:46:49.157Z',
                Key: 'mpu1',
            }],
            UploadIdMarker: 'id',
            NextKeyMarker: 'mpu2',
        });
        return this;
    }

    stubGetBucketLcWithTag() {
        this.stubMethod('getBucketLifecycleConfiguration', {
            Rules: [
                {
                    Expiration: {
                        Days: 1,
                    },
                    Filter: {
                        Tag: { Key: 'key', Value: 'val' },
                    },
                    ID: 'id',
                    Prefix: '',
                    Status: 'Enabled',
                },
            ],
        });
        return this;
    }
}

module.exports = {
    S3ClientMock,
};
