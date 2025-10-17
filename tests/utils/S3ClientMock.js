'use strict';

const assert = require('assert');
const { errors } = require('arsenal');

class S3ClientMock {
    constructor(failures) {
        this.failures = failures;

        this.calls = {};
        this.stubCommand('DeleteObjectCommand', {});
        this.stubCommand('AbortMultipartUploadCommand', {});
        this.stubCommand('GetBucketVersioningCommand', {});
        this.stubCommand('ListObjectsCommand', {
            Contents: [
                {
                    Key: 'obj1',
                    LastModified: new Date('2021-10-04T21:46:49.157Z'),
                },
            ],
        });
        this.stubCommand('HeadObjectCommand', {
            LastModified: new Date('2021-10-04T21:46:49.157Z'),
        });
        this.stubCommand('ListMultipartUploadsCommand', {
            Uploads: [],
        });
        this.stubCommand('GetBucketLifecycleConfigurationCommand', {
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
        this.stubCommand('GetObjectTaggingCommand', {
            TagSet: [{ Key: 'key', Value: 'val' }],
        });
        this.stubCommand('ListObjectVersionsCommand', {
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

    send(command) {
        const commandName = command.constructor.name;
        
        if (!this.calls[commandName]) {
            this.calls[commandName] = 0;
        }
        
        this.calls[commandName]++;
        
        const stubData = this[`_${commandName}Result`];
        const stubError = this[`_${commandName}Error`];
        
        if (this.failures[commandName] >= this.calls[commandName]) {
            const error = stubError || this.makeRetryableError();
            return Promise.reject(error);
        }
        
        return Promise.resolve(stubData);
    }

    stubCommand(commandName, successResult, stubError) {
        this.calls[commandName] = 0;
        this[`_${commandName}Result`] = successResult;
        this[`_${commandName}Error`] = stubError;
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
        this.stubCommand('ListObjectsCommand', {
            IsTruncated: true,
            Contents: [
                {
                    Key: 'obj1',
                    LastModified: new Date('2021-10-04T21:46:49.157Z'),
                    ETag: '1:3749f52bb326ae96782b42dc0a97b4c1',
                    Size: 1,
                    StorageClass: 'site1',
                },
            ],
        });
        return this;
    }

    stubListVersionsTruncated() {
        this.stubCommand('ListObjectVersionsCommand', {
            IsTruncated: true,
            DeleteMarkers: [],
            Versions: [
                {
                    Key: 'v1',
                    ETag: '1:3749f52bb326ae96782b42dc0a97b4c1',
                    Size: 1,
                    StorageClass: 'site1',
                    IsLatest: true,
                    LastModified: new Date('2021-10-04T21:46:49.157Z'),
                },
            ],
        });
        return this;
    }

    stubListMpuTruncated() {
        this.stubCommand('ListMultipartUploadsCommand', {
            IsTruncated: true,
            Uploads: [{
                Initiated: new Date('2021-10-04T21:46:49.157Z'),
                Key: 'mpu1',
            }],
            UploadIdMarker: 'id',
            NextKeyMarker: 'mpu2',
        });
        return this;
    }

    stubGetBucketLcWithTag() {
        this.stubCommand('GetBucketLifecycleConfigurationCommand', {
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
