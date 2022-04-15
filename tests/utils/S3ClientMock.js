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
            TagSet: [],
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

    stubMethod(methodName, successResult) {
        this.calls[methodName] = 0;

        this[methodName] = (params, done) => {
            this.calls[methodName]++;

            if (this.failures[methodName] >= this.calls[methodName]) {
                if (done) {
                    return process.nextTick(done, this.makeRetryableError());
                }
                return {
                    send: cb => process.nextTick(cb, this.makeRetryableError()),
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

    stubListObjectsTruncated() {
        this.stubMethod('listObjects', {
            IsTruncated: true,
            Contents: [
                {
                    Key: 'obj1',
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
                    LastModified: '2021-04-18T21:46:49.157Z',
                },
            ],
        });
        return this;
    }

    stubListMpuTruncated() {
        this.stubMethod('listMultipartUploads', {
            IsTruncated: true,
            Uploads: [{
                Initiated: '2021-04-18T21:46:49.157Z',
                Key: 'mpu1',
            }],
            UploadIdMarker: 'id',
            NextKeyMarker: 'mpu2',
        });
        return this;
    }
}

module.exports = {
    S3ClientMock,
};
