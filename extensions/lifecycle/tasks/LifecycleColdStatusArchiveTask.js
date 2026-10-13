const { callbackify, promisify } = require('util');

const ObjectMDArchive = require('@scality/arsenal').models.ObjectMDArchive;
const LifecycleUpdateTransitionTask = require('./LifecycleUpdateTransitionTask');
const { LifecycleMetrics } = require('../LifecycleMetrics');
const { garbageCollectArchivedSource } = require('../util/garbageCollectArchivedSource');
const { TRANSITION_ATTEMPT_MD } = require('../../../lib/util/transitionAttempt');

class LifecycleColdStatusArchiveTask extends LifecycleUpdateTransitionTask {
    getTargetAttribute(entry) {
        const {
            bucketName: bucket,
            objectKey: key,
            objectVersion: version,
            accountId,
            owner,
        } = entry.target;
        return { bucket, key, version, accountId, owner };
    }

    /**
     * Requests the deletion of a cold object by pushing
     * a message into the cold GC topic
     * @param {string} coldLocation cold location name
     * @param {ColdStorageStatusQueueEntry} entry entry received
     * from the cold location status topic
     * @param {Object} archiveInfo archive to delete
     * @param {Logger} log logger instance
     * @return {Promise<undefined>} resolved once the deletion is requested
     */
    async _deleteColdObject(coldLocation, entry, archiveInfo, log) {
        const coldGcTopic = `${this.lcConfig.coldStorageGCTopicPrefix}${coldLocation}`;
        const gcMessage = JSON.stringify({
            bucketName: entry.target.bucketName,
            objectKey: entry.target.objectKey,
            objectVersion: entry.target.objectVersion,
            archiveInfo,
            requestId: entry.requestId,
            transitionTime: new Date().toISOString(),
        });

        try {
            await promisify(cb => this.coldProducer.sendToTopic(coldGcTopic, [{ message: gcMessage }], cb))();
        } catch (err) {
            log.error('error sending cold object deletion entry', {
                error: err,
                entry: entry.getLogInfo(),
                method: 'LifecycleColdStatusArchiveTask._deleteColdObject',
            });
            throw err;
        }
    }

    /**
     * Process a status entry received once the cold backend has archived the object
     *
     * @param {string} coldLocation cold location name
     * @param {ColdStorageStatusQueueEntry} entry entry received from the cold status topic
     * @param {Function} [done] callback, for callers not using the returned promise
     * @return {Promise<undefined>} resolved once the entry is processed
     */
    async processEntry(coldLocation, entry, done) {
        if (done) {
            return callbackify(() => this.processEntry(coldLocation, entry))(done);
        }

        const log = this.logger.newRequestLogger();

        let objectMD;
        try {
            objectMD = await promisify(cb => this._getMetadata(entry, log, cb))();
            LifecycleMetrics.onS3Request(log, 'getMetadata', 'archive', null);
        } catch (err) {
            LifecycleMetrics.onS3Request(log, 'getMetadata', 'archive', err);
            if (err.name !== 'ObjNotFound') {
                throw err;
            }

            log.info('object metadata not found, cleaning orphan cold object', {
                entry: entry.getLogInfo(),
                method: 'LifecycleColdStatusArchiveTask.processEntry',
            });

            return await this._deleteColdObject(coldLocation, entry, entry.archiveInfo, log);
        }

        const archive = objectMD.getArchive();
        const isSameArchive = (a, b) => a.archiveId === b.archiveId && a.archiveVersion === b.archiveVersion;
        if (archive?.archiveInfo && !isSameArchive(archive.archiveInfo, entry.archiveInfo)) {
            // Keep the archive the metadata already references: status entries may be
            // redelivered, and switching would point the object to an archive a previous status
            // already reclaimed.
            const restoring = archive.restoreRequestedAt || archive.restoreCompletedAt;
            log.info('object is already archived, deleting the redundant archive', {
                entry: entry.getLogInfo(),
                method: 'LifecycleColdStatusArchiveTask.processEntry',
                existingArchiveId: archive.archiveInfo.archiveId,
                newArchiveId: entry.archiveInfo.archiveId,
            });

            if (restoring) {
                // Overwriting the metadata would also drop the restored location, making the
                // object unreadable until the next restore.
                return await this._deleteColdObject(coldLocation, entry, entry.archiveInfo, log);
            }

            return await this._deleteColdObject(coldLocation, entry, entry.archiveInfo, log);
        }

        // read before setTransitionInProgress(false) clears it
        const transitionTime = objectMD.getTransitionTime();

        // set new ObjectMDArchive to ObjectMD, but make sure to keep any (deferred)
        // restore request
        objectMD.setArchive(new ObjectMDArchive(
            entry.archiveInfo,
            archive?.restoreRequestedAt,
            archive?.restoreRequestedDays,
        ));
        objectMD.setOriginOp('s3:LifecycleTransition:SetArchive');

        const locations = objectMD.getLocation();
        const skipLocationDeletion = !locations || (Array.isArray(locations) && locations.length === 0);
        if (skipLocationDeletion) {
            // Only a direct transition can declare the cold class at this point.
            const isDirectToCold = objectMD.getAmzStorageClass() === coldLocation;

            objectMD.setDataStoreName(coldLocation)
                .setAmzStorageClass(coldLocation)
                .setTransitionInProgress(false)
                .setOriginOp(isDirectToCold ? 's3:LifecycleTransition:Direct' : 's3:LifecycleTransition')
                .setUserMetadata({ [TRANSITION_ATTEMPT_MD]: undefined });
        }

        try {
            await promisify(cb => this._putMetadata(entry, objectMD, log, cb))();
            LifecycleMetrics.onS3Request(log, 'putMetadata', 'archive', null);
        } catch (err) {
            LifecycleMetrics.onS3Request(log, 'putMetadata', 'archive', err);
            throw err;
        } finally {
            LifecycleMetrics.onLifecycleCompleted(log, 'archive',
                coldLocation, Date.now() - Date.parse(transitionTime));
        }

        if (!skipLocationDeletion) {
            const oldLocation = objectMD.getDataStoreName();
            garbageCollectArchivedSource(this.gcProducer, entry.target, oldLocation, coldLocation, log);
        }

        return undefined;
    }
}

module.exports = LifecycleColdStatusArchiveTask;
