'use strict'; // eslint-disable-line

const VersionIDUtils = require('arsenal').versioning.VersionID;

const VID_SEP = require('arsenal').versioning.VersioningConstants
          .VersionId.Separator;

class QueueEntry {

    /**
     * @constructor
     * @param {string} bucket - bucket name for entry's object (may be
     *   source bucket or destination bucket depending on replication
     *   status)
     * @param {string} objectKey - entry's object key without version
     *   suffix
     * @param {object} objMd - entry's object metadata as a parsed JS
     *   object
     */
    constructor(bucket, objectKey, objMd) {
        this.bucket = bucket;
        this.objectKey = objectKey;
        this.objMd = objMd;
    }

    checkSanity() {
        if (typeof this.bucket !== 'string') {
            return { message: 'missing bucket name' };
        }
        if (typeof this.objectKey !== 'string') {
            return { message: 'missing object key' };
        }
        if (typeof this.objMd.replicationInfo !== 'object' ||
            typeof this.objMd.replicationInfo.destination !== 'string') {
            return { message: 'malformed source metadata: ' +
                     'missing destination info' };
        }
        if (typeof this.objMd.versionId !== 'string') {
            return { message: 'malformed source metadata: ' +
                     'bad or missing versionId' };
        }
        if (typeof this.objMd['content-length'] !== 'number') {
            return { message: 'malformed source metadata: ' +
                     'bad or missing content-length' };
        }
        if (typeof this.objMd['content-md5'] !== 'string') {
            return { message: 'malformed source metadata: ' +
                     'bad or missing content-md5' };
        }
        return undefined;
    }

    static _extractVersionedBaseKey(key) {
        return key.split(VID_SEP)[0];
    }

    static createFromKafkaEntry(kafkaEntry) {
        try {
            const record = JSON.parse(kafkaEntry.value);
            const objMd = JSON.parse(record.value);
            const entry = new QueueEntry(
                record.bucket,
                QueueEntry._extractVersionedBaseKey(record.key),
                objMd);
            const err = entry.checkSanity();
            if (err) {
                return { error: err };
            }
            return entry;
        } catch (err) {
            return { error: { message: 'malformed JSON in kafka entry',
                              description: err.message } };
        }
    }

    isDeleteMarker() {
        return this.objMd.isDeleteMarker;
    }

    getBucket() {
        return this.bucket;
    }

    getObjectKey() {
        return this.objectKey;
    }

    getVersionId() {
        return this.objMd.versionId;
    }

    getEncodedVersionId() {
        return VersionIDUtils.encode(this.getVersionId());
    }

    getMetadataBlob() {
        return JSON.stringify(this.objMd);
    }

    getContentLength() {
        return this.objMd['content-length'];
    }

    getContentMD5() {
        return this.objMd['content-md5'];
    }

    getReplicationStatus() {
        return this.objMd.replicationInfo.status;
    }

    getReplicationContent() {
        return this.objMd.replicationInfo.content;
    }

    getReplicationRoles() {
        return this.objMd.replicationInfo.role;
    }

    getReplicationDestBucket() {
        const destBucketArn = this.objMd.replicationInfo.destination;
        return destBucketArn.split(':').slice(-1)[0];
    }

    getOwnerCanonicalId() {
        return this.objMd['owner-id'];
    }

    getLogInfo() {
        return {
            bucket: this.getBucket(),
            objectKey: this.getObjectKey(),
            versionId: this.getVersionId(),
            isDeleteMarker: this.isDeleteMarker(),
        };
    }

    setOwner(ownerCanonicalId, ownerDisplayName) {
        this.objMd['owner-id'] = ownerCanonicalId;
        this.objMd['owner-display-name'] = ownerDisplayName;
    }

    setLocation(location) {
        this.objMd.location = location;
    }

    _convertEntry(bucket, repStatus) {
        const replicationInfo = Object.assign({}, this.objMd.replicationInfo);
        const replicaMd = Object.assign({}, this.objMd);
        replicaMd.replicationInfo = replicationInfo;
        replicaMd.replicationInfo.status = repStatus;
        return new QueueEntry(bucket, this.objectKey, replicaMd);
    }

    toReplicaEntry() {
        const destBucket = this.getReplicationDestBucket();
        return this._convertEntry(destBucket, 'REPLICA');
    }

    toCompletedEntry() {
        return this._convertEntry(this.getBucket(), 'COMPLETED');
    }

    toFailedEntry() {
        return this._convertEntry(this.getBucket(), 'FAILED');
    }
}

module.exports = QueueEntry;
