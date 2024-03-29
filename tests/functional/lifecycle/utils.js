const assert = require('assert');

const ONE_DAY_IN_SEC = 60 * 60 * 24 * 1000;

class TestKafkaEntry {
    constructor(state) {
        const {
            objectTopic,
            transitionTopic,
            bucketTopic,
            dataMoverTopic,
            ownerId,
            accountId,
            bucketName,
        } = state;

        this.objectTopic = objectTopic;
        this.transitionTopic = transitionTopic;
        this.bucketTopic = bucketTopic;
        this.dataMoverTopic = dataMoverTopic;
        this.ownerId = ownerId;
        this.accountId = accountId;
        this.bucketName = bucketName;
    }

    expectMPUExpirationEntry(e, { keyName, uploadId }) {
        assert(e);
        assert.strictEqual(e.topicName, this.objectTopic);
        assert.strictEqual(e.entry.length, 1);

        const message = JSON.parse(e.entry[0].message);
        assert.strictEqual(message.action, 'deleteMPU');

        const contextInfo = message.contextInfo;
        assert.strictEqual(contextInfo.origin, 'lifecycle');
        assert.strictEqual(contextInfo.ruleType, 'expiration');

        const target = message.target;
        assert.strictEqual(target.owner, this.ownerId);
        assert.strictEqual(target.accountId, this.accountId);
        assert.strictEqual(target.bucket, this.bucketName);
        assert.strictEqual(target.key, keyName);

        const details = message.details;
        assert.strictEqual(details.UploadId, uploadId);
    }

    expectObjectExpirationEntry(e, { keyName, versionId, lastModified }) {
        assert(e);
        assert.strictEqual(e.topicName, this.objectTopic);
        assert.strictEqual(e.entry.length, 1);

        const message = JSON.parse(e.entry[0].message);
        assert.strictEqual(message.action, 'deleteObject');

        const contextInfo = message.contextInfo;
        assert.strictEqual(contextInfo.origin, 'lifecycle');
        assert.strictEqual(contextInfo.ruleType, 'expiration');

        const target = message.target;
        assert.strictEqual(target.owner, this.ownerId);
        assert.strictEqual(target.accountId, this.accountId);
        assert.strictEqual(target.bucket, this.bucketName);
        assert.strictEqual(target.key, keyName);
        assert.strictEqual(target.version, versionId);

        if (lastModified) {
            const details = message.details;
            assert.strictEqual(details.lastModified, lastModified);
        }
    }

    expectObjectTransitionEntry(e, { keyName, versionId, lastModified, eTag, contentLength,
        sourceLocation, destinationLocation }) {
        assert(e);
        assert.strictEqual(e.topicName, this.dataMoverTopic);
        assert.strictEqual(e.entry.length, 1);

        const message = JSON.parse(e.entry[0].message);
        assert.strictEqual(message.action, 'copyLocation');

        const contextInfo = message.contextInfo;
        assert.strictEqual(contextInfo.origin, 'lifecycle');
        assert.strictEqual(contextInfo.ruleType, 'transition');

        const target = message.target;
        assert.strictEqual(target.owner, this.ownerId);
        assert.strictEqual(target.accountId, this.accountId);
        assert.strictEqual(target.bucket, this.bucketName);
        assert.strictEqual(target.key, keyName);
        assert.strictEqual(target.version, versionId);
        assert.strictEqual(target.eTag, eTag);
        assert.strictEqual(target.lastModified, lastModified);

        const metrics = message.metrics;
        assert.strictEqual(metrics.origin, 'lifecycle');
        assert.strictEqual(metrics.fromLocation, sourceLocation);
        assert.strictEqual(metrics.contentLength, contentLength);

        assert.strictEqual(message.toLocation, destinationLocation);
        assert.strictEqual(message.resultsTopic, this.transitionTopic);
    }

    expectBucketEntry(e, {
        listType,
        hasBeforeDate,
        prefix,
        marker,
        keyMarker,
        versionIdMarker,
        uploadIdMarker,
        storageClass,
    }) {
        assert(e);
        assert.strictEqual(e.topicName, this.bucketTopic);
        assert.strictEqual(e.entry.length, 1);

        const message = JSON.parse(e.entry[0].message);
        assert.strictEqual(message.action, 'processObjects');

        const target = message.target;
        assert.strictEqual(target.bucket, this.bucketName);
        assert.strictEqual(target.owner, this.ownerId);
        assert.strictEqual(target.accountId, this.accountId);

        const details = message.details;
        assert.strictEqual(!!details.beforeDate, hasBeforeDate);
        assert.strictEqual(details.prefix, prefix);
        assert.strictEqual(details.marker, marker);
        assert.strictEqual(details.keyMarker, keyMarker);
        assert.strictEqual(details.versionIdMarker, versionIdMarker);
        assert.strictEqual(details.uploadIdMarker, uploadIdMarker);
        assert.strictEqual(details.listType, listType);
        assert.strictEqual(details.storageClass, storageClass);
    }
}

class KeyMock {
    constructor(state) {
        const {
            owner,
            sourceLocation,
        } = state;

        this.owner = owner;
        this.sourceLocation = sourceLocation;
    }

    nonCurrent({ keyName, versionId, daysEarlier, size }) {
        const currentDate = Date.now();
        const staleDate = (new Date(currentDate - (daysEarlier * ONE_DAY_IN_SEC))).toISOString();
        return {
            Key: keyName,
            LastModified: '2023-03-13T16:43:59.200Z',
            ETag: 'd41d8cd98f00b204e9800998ecf8427e',
            Owner: this.owner,
            Size: size !== undefined ? size : 64,
            StorageClass: 'STANDARD',
            staleDate,
            VersionId: versionId,
            TagSet: [],
            DataStoreName: this.sourceLocation,
            ListType: 'noncurrent',
        };
    }

    current({ keyName, daysEarlier, tagSet, size }) {
        const currentDate = Date.now();
        const lastModified = (new Date(currentDate - (daysEarlier * ONE_DAY_IN_SEC))).toISOString();
        return {
            Key: keyName,
            LastModified: lastModified,
            ETag: 'd41d8cd98f00b204e9800998ecf8427e',
            Owner: this.owner,
            Size: size !== undefined ? size : 64,
            StorageClass: 'STANDARD',
            TagSet: tagSet,
            DataStoreName: this.sourceLocation,
            ListType: 'current'
        };
    }

    orphanDeleteMarker({ keyName, versionId, daysEarlier }) {
        const currentDate = Date.now();
        const lastModified = (new Date(currentDate - (daysEarlier * ONE_DAY_IN_SEC))).toISOString();
        return {
            Key: keyName,
            LastModified: lastModified,
            Owner: this.owner,
            Size: 64,
            VersionId: versionId,
            ListType: 'orphan',
        };
    }
}

class BackbeatMetadataProxyMock {
    constructor() {
        this.objMD = null;
        this.contents = [];
        this.isTruncated = false;
        this.markerInfo = null;
        this.listType = null;
        this.listParams = null;
    }

    set objectMetadata(objMD) {
        this.objMD = objMD;
    }

    set listLifecycleResponse({ contents, isTruncated, markerInfo }) {
        this.contents = contents;
        this.isTruncated = isTruncated;
        this.markerInfo = markerInfo;
    }

    get listLifecycleType() {
        return this.listType;
    }

    get listLifecycleParams() {
        return this.listParams;
    }

    listLifecycle(listType, params, log, cb) {
        this.listType = listType;
        this.listParams = params;
        return cb(null, this.contents, this.isTruncated, this.markerInfo);
    }

    getMetadata(params, log, cb) {
        return cb(null, { Body: this.objMD.getSerialized() });
    }

    putMetadata(params, log, cb) {
        return cb();
    }

    getBucketIndexes(bucket, log, cb) {
        return cb(null, []);
    }

    putBucketIndexes(bucket, indexes, log, cb) {
        return cb();
    }

    deleteBucketIndexes(bucket, indexes, log, cb) {
        return cb();
    }
}

function expectNominalListingParams(bucketName, params) {
    assert.strictEqual(params.Bucket, bucketName);
    assert.strictEqual(params.Prefix, '');
    assert(!!params.BeforeDate);
    assert(!params.ExcludedDataStoreName);
}

module.exports = {
    TestKafkaEntry,
    BackbeatMetadataProxyMock,
    KeyMock,
    expectNominalListingParams,
};
