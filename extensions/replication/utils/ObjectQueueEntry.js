const ObjectMD = require('arsenal').models.ObjectMD;
const VID_SEP = require('arsenal').versioning.VersioningConstants
          .VersionId.Separator;

function _extractVersionedBaseKey(key) {
    return key.split(VID_SEP)[0];
}

class ObjectQueueEntry extends ObjectMD {

    /**
     * @constructor
     * @param {string} bucket - bucket name for entry's object (may be
     *   source bucket or destination bucket depending on replication
     *   status)
     * @param {string} objectVersionedKey - entry's object key with
     *   version suffix
     * @param {object} objMd - entry's object metadata
     */
    constructor(bucket, objectVersionedKey, objMd) {
        super(objMd);
        this.bucket = bucket;
        this.objectVersionedKey = objectVersionedKey;
        this.objectKey = _extractVersionedBaseKey(objectVersionedKey);
    }

    clone() {
        return new ObjectQueueEntry(this.bucket, this.objectVersionedKey, this);
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


    getBucket() {
        return this.bucket;
    }

    setBucket(bucket) {
        this.bucket = bucket;
        return this;
    }

    getCanonicalKey() {
        return `${this.getBucket()}/${this.getObjectKey()}`;
    }

    getObjectKey() {
        return this.objectKey;
    }

    getObjectVersionedKey() {
        return this.objectVersionedKey;
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

module.exports = ObjectQueueEntry;
