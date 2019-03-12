'use strict'; // eslint-disable-line strict

class MetricsModel {
    /**
     * constructor
     * @param {Number} ops - number of operations
     * @param {Number} bytes - data size in bytes
     * @param {String} extension - extension
     * @param {String} type - operation indicator (queued, completed, failed)
     * @param {String} site - site name
     * @param {String} bucketName - bucket name
     * @param {String} objectKey - object key
     * @param {String} versionId - object version ID
     */

    constructor(ops, bytes, extension, type, site, bucketName, objectKey,
        versionId) {
        this._timestamp = Date.now();
        this._ops = ops;
        this._bytes = bytes;
        this._extension = extension;
        this._type = type;
        this._site = site;
        this._bucketName = bucketName;
        this._objectKey = objectKey;
        this._versionId = versionId;
    }

    withOps(ops) {
        this._ops = ops;
        return this;
    }

    withBytes(bytes) {
        this._bytes = bytes;
        return this;
    }

    withExtension(extension) {
        this._extension = extension;
        return this;
    }

    withMetricType(type) {
        this._type = type;
        return this;
    }

    withSite(site) {
        this._site = site;
        return this;
    }

    withBucketName(bucketName) {
        this._bucketName = bucketName;
        return this;
    }

    withObjectKey(objectKey) {
        this._objectKey = objectKey;
        return this;
    }

    withVersionId(versionId) {
        this._versionId = versionId;
        return this;
    }

    serialize() {
        return JSON.stringify({
            timestamp: this._timestamp,
            ops: this._ops,
            bytes: this._bytes,
            extension: this._extension,
            type: this._type,
            site: this._site,
            bucketName: this._bucketName,
            objectKey: this._objectKey,
            versionId: this._versionId,
        });
    }
}

module.exports = MetricsModel;
