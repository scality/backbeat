'use strict'; // eslint-disable-line

const ObjectMD = require('arsenal').models.ObjectMD;
const VID_SEP = require('arsenal').versioning.VersioningConstants
          .VersionId.Separator;
const { isMasterKey } = require('arsenal/lib/versioning/Version');

function _extractVersionedBaseKey(key) {
    return key.split(VID_SEP)[0];
}

class QueueEntry extends ObjectMD {

    /**
     * @constructor
     * @param {string} bucket - bucket name for entry's object (may be
     *   source bucket or destination bucket depending on replication
     *   status)
     * @param {string} objectVersionedKey - entry's object key with
     *   version suffix
     * @param {string} entryType - entry's type (put/delete)
     * @param {object} objMd - entry's object metadata
     */
    constructor(bucket, objectVersionedKey, entryType, objMd) {
        super(objMd);
        this.bucket = bucket;
        this.objectVersionedKey = objectVersionedKey;
        this.objectKey = _extractVersionedBaseKey(objectVersionedKey);
        this.entryType = entryType;
    }

    clone() {
        return new QueueEntry(this.bucket, this.objectVersionedKey,
            this.entryType, this);
    }

    checkSanity() {
        if (typeof this.bucket !== 'string') {
            return { message: 'missing bucket name' };
        }
        if (typeof this.objectKey !== 'string') {
            return { message: 'missing object key' };
        }
        return undefined;
    }

    static createFromKafkaEntry(kafkaEntry) {
        try {
            const record = JSON.parse(kafkaEntry.value);
            const objMd = JSON.parse(record.value);
            const entry = new QueueEntry(record.bucket, record.key,
                record.type, objMd);
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

    getBucket() {
        return this.bucket;
    }

    setBucket(bucket) {
        this.bucket = bucket;
        return this;
    }

    getObjectKey() {
        return this.objectKey;
    }

    getObjectVersionedKey() {
        return this.objectVersionedKey;
    }

    isObjectMasterKey() {
        return isMasterKey(this.objectVersionedKey);
    }

    getEntryType() {
        return this.entryType;
    }


    getUserMetadata() {
        const metaHeaders = {};
        const data = this.getValue();
        Object.keys(data).forEach(key => {
            if (key.startsWith('x-amz-meta-')) {
                metaHeaders[key] = data[key];
            }
        });
        if (Object.keys(metaHeaders).length > 0) {
            return JSON.stringify(metaHeaders);
        }
        return undefined;
    }

    getLogInfo() {
        return {
            bucket: this.getBucket(),
            objectKey: this.getObjectKey(),
            versionId: this.getVersionId(),
            isDeleteMarker: this.getIsDeleteMarker(),
        };
    }

    toReplicaEntry() {
        const newEntry = this.clone();
        newEntry.setBucket(this.getReplicationTargetBucket());
        newEntry.setReplicationStatus('REPLICA');
        return newEntry;
    }

    toCompletedEntry() {
        const newEntry = this.clone();
        newEntry.setReplicationStatus('COMPLETED');
        return newEntry;
    }

    toFailedEntry() {
        const newEntry = this.clone();
        newEntry.setReplicationStatus('FAILED');
        return newEntry;
    }
}

module.exports = QueueEntry;
