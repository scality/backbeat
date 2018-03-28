const ObjectMD = require('arsenal').models.ObjectMD;
const VID_SEP = require('arsenal').versioning.VersioningConstants
          .VersionId.Separator;

function _extractVersionedBaseKey(key) {
    if (key) {
        return key.split(VID_SEP)[0];
    }
    return '';
}

function _getGlobalReplicationStatus(data) {
    // Check the global status relative to the other backends
    if (Array.isArray(data.replicationInfo.backends)) {
        const statuses = data.replicationInfo.backends.map(backend =>
            backend.status);
        // If any site replication failed, set the global status to FAILED.
        if (statuses.includes('FAILED')) {
            return 'FAILED';
        }
        if (statuses.includes('PENDING')) {
            return 'PROCESSING';
        }
    }
    return 'COMPLETED';
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
        this.site = null;
    }

    setSite(site) {
        this.site = site;
        return this;
    }

    getSite() {
        return this.site;
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

    toReplicaEntry(site) {
        const newEntry = this.clone();
        newEntry.setBucket(this.getReplicationTargetBucket());
        newEntry.setReplicationSiteStatus(site, 'REPLICA');
        newEntry.setReplicationStatus('REPLICA');
        return newEntry;
    }

    toCompletedEntry(site) {
        const newEntry = this.clone();
        newEntry.setReplicationSiteStatus(site, 'COMPLETED');
        const status = _getGlobalReplicationStatus(this.getValue());
        newEntry.setReplicationStatus(status);
        return newEntry;
    }

    toFailedEntry(site) {
        const newEntry = this.clone();
        newEntry.setReplicationSiteStatus(site, 'FAILED');
        newEntry.setReplicationStatus('FAILED');
        return newEntry;
    }

    toKafkaEntry(site) {
        return { key: encodeURIComponent(
            `${this.getBucket()}/${this.getObjectKey()}`),
                 message: JSON.stringify({
                     bucket: this.getBucket(),
                     key: this.getObjectVersionedKey(),
                     value: JSON.stringify(this.getValue()),
                     site,
                 }),
               };
    }
}

module.exports = ObjectQueueEntry;
