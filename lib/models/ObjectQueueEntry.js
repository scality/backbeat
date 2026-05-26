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
     * @param {object} [opts] - optional fields
     * @param {string} [opts.workflowId] - workflow UUID
     * @param {number} [opts.workflowVersion] - workflow version
     * @param {string} [opts.nodeId] - node UUID in the workflow engine
     * @param {string} [opts.uniqueId] - unique UUID during workflow traversal
     * @param {boolean} [opts.ignore=false] - ignore this message (used for
     *   synchronization)
     */
    constructor(bucket, objectVersionedKey, objMd, opts = {}) {
        super(objMd);
        this.bucket = bucket;
        this.objectVersionedKey = objectVersionedKey;
        this.objectKey = _extractVersionedBaseKey(objectVersionedKey);
        this.workflowId = opts.workflowId;
        this.workflowVersion = opts.workflowVersion;
        this.nodeId = opts.nodeId;
        this.uniqueId = opts.uniqueId;
        this.ignore = opts.ignore ?? false;
        this.site = null;
        this.destination = undefined;
        this.role = undefined;
        this._startProcessing = Date.now();
        // used to keep a reference of replayCount when cloning.
        this.replayCount = objMd.replayCount;
        this.accountId = null;
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

    setDestination(destination) {
        this.destination = destination;
        return this;
    }

    getDestination() {
        return this.destination;
    }

    setRole(role) {
        this.role = role;
        return this;
    }

    getRole() {
        return this.role;
    }

    /**
     * Return the replication backend this entry instance currently
     * represents — a `{site, destination, role}` triple that uniquely
     * identifies one entry in `replicationInfo.backends[]`. This is a
     * per-task-instance annotation, not part of the persisted metadata.
     */
    getReplicationBackend() {
        return {
            site: this.site,
            destination: this.destination,
            role: this.role,
        };
    }

    /**
     * Stamp which replication backend this entry instance represents.
     * Required after `clone()` / kafka deserialize / metadata refresh,
     * where the in-memory identity would otherwise be lost.
     */
    setReplicationBackend({ site, destination, role } = {}) {
        this.site = site;
        this.destination = destination;
        this.role = role;
        return this;
    }

    clone() {
        const newEntry = new ObjectQueueEntry(
            this.bucket, this.objectVersionedKey, this, {
                workflowId: this.workflowId,
                workflowVersion: this.workflowVersion,
                nodeId: this.nodeId,
                uniqueId: this.uniqueId,
                ignore: this.ignore,
            });
        return newEntry.setReplicationBackend(this.getReplicationBackend());
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

    setAccountId(accountId) {
        this.accountId = accountId;
        return this;
    }

    getAccountId() {
        return this.accountId;
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

    toReplicaEntry(backend) {
        const newEntry = this.clone();
        newEntry
            .setAccountId(this.getAccountId())
            .setBucket(this.getReplicationTargetBucket(backend))
            .setReplicationSiteStatus(backend, 'REPLICA')
            .setReplicationStatus('REPLICA');
        return newEntry;
    }

    toCompletedEntry(backend) {
        return this.clone()
            .setAccountId(this.getAccountId())
            .setReplicationSiteStatus(backend, 'COMPLETED')
            .setReplicationStatus(this._getGlobalReplicationStatus())
            .setOriginOp('s3:Replication:OperationCompletedReplication');
    }

    toFailedEntry(backend) {
        return this.clone()
            .setAccountId(this.getAccountId())
            .setReplicationSiteStatus(backend, 'FAILED')
            .setReplicationStatus('FAILED')
            .setOriginOp('s3:Replication:OperationFailedReplication');
    }

    toPendingEntry(backend) {
        return this.clone()
            .setAccountId(this.getAccountId())
            .setReplicationSiteStatus(backend, 'PENDING')
            .setReplicationStatus(this._getGlobalReplicationStatus())
            .setOriginOp('s3:Replication:OperationPendingReplication');
    }

    toRetryEntry(backend) {
        const matched = this._findBackend(backend);
        return this.clone()
            .setAccountId(this.getAccountId())
            .setReplicationBackends(matched ? [matched] : [])
            .setReplicationSiteStatus(backend, 'PENDING')
            .setReplicationStatus('PENDING');
    }

    toKafkaEntry(backend) {
        return { key: encodeURIComponent(
            `${this.getBucket()}/${this.getObjectKey()}`),
                 message: JSON.stringify({
                     bucket: this.getBucket(),
                     key: this.getObjectVersionedKey(),
                     value: JSON.stringify(
                         this.getIgnore() ? {} : this.getValue()),
                     site: backend?.site,
                     destination: backend?.destination,
                     role: backend?.role,
                     workflowId: this.getWorkflowId(),
                     workflowVersion: this.getWorkflowVersion(),
                     nodeId: this.getNodeId(),
                     uniqueId: this.getUniqueId(),
                     ignore: this.getIgnore(),
                     replayCount: this.getReplayCount(),
                     accountId: this.getAccountId(),
                 }),
               };
    }
}

module.exports = ObjectQueueEntry;
