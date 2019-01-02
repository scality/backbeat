const ObjectMD = require('arsenal').models.ObjectMD;
const VID_SEP = require('arsenal').versioning.VersioningConstants
          .VersionId.Separator;

function _extractVersionedBaseKey(key) {
    if (key) {
        return key.split(VID_SEP)[0];
    }
    return '';
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

    isVersion() {
        return this.getObjectKey() === this.getObjectVersionedKey();
    }

    getLogInfo() {
        return {
            bucket: this.getBucket(),
            objectKey: this.getObjectKey(),
            versionId: this.getVersionId(),
            isDeleteMarker: this.getIsDeleteMarker(),
        };
    }

    _getGlobalReplicationStatus() {
        const data = this.getValue();
        // Check the global status relative to the other backends
        if (Array.isArray(data.replicationInfo.backends)) {
            const statuses = data.replicationInfo.backends.map(
                backend => backend.status);
            // If any site replication failed, set the global status
            // to FAILED.
            if (statuses.includes('FAILED')) {
                return 'FAILED';
            }
            if (statuses.includes('PENDING')) {
                return 'PROCESSING';
            }
        }
        return 'COMPLETED';
    }

    toReplicaEntry(site) {
        const newEntry = this.clone();
        newEntry
            .setBucket(this.getReplicationTargetBucket())
            .setReplicationSiteStatus(site, 'REPLICA')
            .setReplicationStatus('REPLICA');
        return newEntry;
    }

    toCompletedEntry(site) {
        return this.clone()
            .setReplicationSiteStatus(site, 'COMPLETED')
            .setReplicationStatus(this._getGlobalReplicationStatus());
    }

    toFailedEntry(site) {
        return this.clone()
            .setReplicationSiteStatus(site, 'FAILED')
            .setReplicationStatus('FAILED');
    }

    toPendingEntry(site) {
        return this.clone()
            .setReplicationSiteStatus(site, 'PENDING')
            .setReplicationStatus(this._getGlobalReplicationStatus());
    }

    toRetryEntry(site) {
        return this.clone()
            .setReplicationBackends([{
                site,
                status: 'PENDING',
                dataStoreVersionId: '',
            }])
            .setReplicationStorageClass(site)
            .setReplicationStatus('PENDING');
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
