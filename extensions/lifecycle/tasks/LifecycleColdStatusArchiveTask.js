const async = require('async');

const ObjectMDArchive = require('@scality/arsenal').models.ObjectMDArchive;
const LifecycleUpdateTransitionTask = require('./LifecycleUpdateTransitionTask');
const { LifecycleMetrics } = require('../LifecycleMetrics');
const { garbageCollectArchivedSource } = require('../util/garbageCollectArchivedSource');
const { TRANSITION_ATTEMPT_MD } = require('../../../lib/util/transitionAttempt');

class SkipMdUpdateError extends Error {}

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

    _garbageCollectArchivedSource(entry, oldLocation, newLocation, log) {
        garbageCollectArchivedSource(this.gcProducer, this.getTargetAttribute(entry),
            oldLocation, newLocation, log);
    }

    /**
     * Requests the deletion of a cold object by pushing
     * a message into the cold GC topic
     * @param {string} coldLocation cold location name
     * @param {ColdStorageStatusQueueEntry} entry entry received
     * from the cold location status topic
     * @param {Logger} log logger instance
     * @param {function} cb callback
     * @return {undefined}
     */
    _deleteColdObject(coldLocation, entry, log, cb) {
        const coldGcTopic = `${this.lcConfig.coldStorageGCTopicPrefix}${coldLocation}`;
        const gcMessage = JSON.stringify({
            bucketName: entry.target.bucketName,
            objectKey: entry.target.objectKey,
            objectVersion: entry.target.objectVersion,
            archiveInfo: entry.archiveInfo,
            requestId: entry.requestId,
            transitionTime: new Date().toISOString(),
        });
        this.coldProducer.sendToTopic(coldGcTopic, [{ message: gcMessage }], err => {
            if (err) {
                log.error('error sending cold object deletion entry', {
                    error: err,
                    entry: entry.getLogInfo(),
                    method: 'LifecycleColdStatusArchiveTask._deleteColdObject',
                });
                return cb(err);
            }
            return cb(new SkipMdUpdateError('cold object deleted'));
        });
    }

    processEntry(coldLocation, entry, done) {
        const log = this.logger.newRequestLogger();
        let objectMD;
        let oldLocation;
        let skipLocationDeletion = false;

        return async.series([
            next => this._getMetadata(entry, log, (err, res) => {
                LifecycleMetrics.onS3Request(log, 'getMetadata', 'archive', err);
                if (err) {
                    if (err.name === 'ObjNotFound') {
                        log.info('object metadata not found, cleaning orphan cold object', {
                            entry: entry.getLogInfo(),
                            method: 'LifecycleColdStatusArchiveTask.processEntry',
                        });
                        return this._deleteColdObject(coldLocation, entry, log, next);
                    }
                    return next(err);
                }

                const locations = res.getLocation();
                objectMD = res;
                skipLocationDeletion = !locations ||
                    (Array.isArray(locations) && locations.length === 0);
                oldLocation = objectMD.getDataStoreName();

                return next();
            }),
            next => {
                const transitionTime = objectMD.getTransitionTime();

                // set new ObjectMDArchive to ObjectMD, but make sure to keep any (deferred)
                // restore request
                const archive = objectMD.getArchive();
                objectMD.setArchive(new ObjectMDArchive(
                    entry.archiveInfo,
                    archive?.restoreRequestedAt,
                    archive?.restoreRequestedDays,
                ));
                objectMD.setOriginOp('s3:LifecycleTransition:SetArchive');

                if (skipLocationDeletion) {
                    // Only a direct transition can declare the cold class at this point.
                    const isDirectToCold = objectMD.getAmzStorageClass() === coldLocation;

                    objectMD.setDataStoreName(coldLocation)
                        .setAmzStorageClass(coldLocation)
                        .setTransitionInProgress(false)
                        .setOriginOp(isDirectToCold ? 's3:LifecycleTransition:Direct' : 's3:LifecycleTransition')
                        .setUserMetadata({ [TRANSITION_ATTEMPT_MD]: undefined });
                }

                this._putMetadata(entry, objectMD, log, err => {
                    LifecycleMetrics.onS3Request(log, 'putMetadata', 'archive', err);
                    LifecycleMetrics.onLifecycleCompleted(log, 'archive',
                        coldLocation, Date.now() - Date.parse(transitionTime));
                    return next(err);
                });
            },
            next => {
                if (!skipLocationDeletion) {
                    this._garbageCollectArchivedSource(entry, oldLocation, coldLocation, log);
                }

                return process.nextTick(next);
            },
        ], err => {
            if (err && !(err instanceof SkipMdUpdateError)) {
                return done(err);
            }
            return done();
        });
    }
}

module.exports = LifecycleColdStatusArchiveTask;
