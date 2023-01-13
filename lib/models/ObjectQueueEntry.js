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
     * @param {string} workflowId - workflow UUID
     * @param {number} workflowVersion - workflow version
     * @param {string} nodeId - node UUID in the workflow engine
     * @param {string} uniqueId - unique UUID during workflow traversal
     * @param {boolean} ignore - ignore this message (used for synchronization)
     */
    constructor(bucket, objectVersionedKey, objMd,
                workflowId = undefined,
                workflowVersion = undefined,
                nodeId = undefined,
                uniqueId = undefined,
                ignore = false) {
        super(objMd);
        this.bucket = bucket;
        this.objectVersionedKey = objectVersionedKey;
        this.objectKey = _extractVersionedBaseKey(objectVersionedKey);
        this.site = null;
        this.workflowId = workflowId;
        this.workflowVersion = workflowVersion;
        this.nodeId = nodeId;
        this.uniqueId = uniqueId;
        this.ignore = ignore;
        this._startProcessing = Date.now();
        // used to keep a reference of replayCount when cloning.
        this.replayCount = objMd.replayCount;
    }

    getStartProcessing() {
        return this._startProcessing;
    }

    setReplayCount(count) {
        this.replayCount = count;
        return this;
    }

    decReplayCount() {
        this.replayCount--;
        return this;
    }

    getReplayCount() {
        return this.replayCount;
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

    getWorkflowId() {
        return this.workflowId;
    }

    getWorkflowVersion() {
        return this.workflowVersion;
    }

    getNodeId() {
        return this.nodeId;
    }

    getUniqueId() {
        return this.uniqueId;
    }

    getIgnore() {
        return this.ignore;
    }

    setIgnore(ignore) {
        this.ignore = ignore;
    }

    getLogInfo() {
        return {
            bucket: this.getBucket(),
            objectKey: this.getObjectKey(),
            versionId: this.getVersionId(),
            isDeleteMarker: this.getIsDeleteMarker(),
            workflowId: this.getWorkflowId(),
            workflowVersion: this.getWorkflowVersion(),
            nodeId: this.getNodeId(),
            uniqueId: this.getUniqueId(),
            ignore: this.getIgnore()
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
            .setReplicationBackends(this.getReplicationBackends().filter(o => o.site === site))
            .setReplicationSiteStatus(site, 'PENDING')
            .setReplicationStorageClass(site)
            .setReplicationStatus('PENDING');
    }

    toKafkaEntry(site) {
        return { key: encodeURIComponent(
            `${this.getBucket()}/${this.getObjectKey()}`),
                 message: JSON.stringify({
                     bucket: this.getBucket(),
                     key: this.getObjectVersionedKey(),
                     value: JSON.stringify(
                         this.getIgnore() ? {} : this.getValue()),
                     site,
                     workflowId: this.getWorkflowId(),
                     workflowVersion: this.getWorkflowVersion(),
                     nodeId: this.getNodeId(),
                     uniqueId: this.getUniqueId(),
                     ignore: this.getIgnore(),
                     replayCount: this.getReplayCount(),
                 }),
               };
    }
}

module.exports = ObjectQueueEntry;
