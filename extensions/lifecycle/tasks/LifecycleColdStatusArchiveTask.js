const async = require('async');

const ObjectMDArchive = require('arsenal').models.ObjectMDArchive;
const ActionQueueEntry = require('../../../lib/models/ActionQueueEntry');
const LifecycleUpdateTransitionTask = require('./LifecycleUpdateTransitionTask');
const { LifecycleMetrics } = require('../LifecycleMetrics');

class SkipMdUpdateError extends Error {}

class LifecycleColdStatusArchiveTask extends LifecycleUpdateTransitionTask {
    getTargetAttribute(entry) {
        const {
            bucketName: bucket,
            objectKey: key,
            objectVersion: version,
            accountId,
            ownerId,
        } = entry.target;
        return { bucket, key, version, accountId, ownerId };
    }

    _garbageCollectArchivedSource(entry, oldLocation, newLocation, log) {
        const { bucket, key, version, accountId, ownerId } = this.getTargetAttribute(entry);
        const gcEntry = ActionQueueEntry.create('deleteArchivedSourceData')
              .addContext({
                  origin: 'lifecycle',
                  ruleType: 'archive',
                  reqId: log.getSerializedUids(),
                  bucketName: bucket,
                  objectKey: key,
                  versionId: version,
              })
              .setAttribute('serviceName', 'lifecycle-transition')
              .setAttribute('target.oldLocation', oldLocation)
              .setAttribute('target.newLocation', newLocation)
              .setAttribute('target.bucket', bucket)
              .setAttribute('target.key', key)
              .setAttribute('target.version', version)
              .setAttribute('target.accountId', accountId)
              .setAttribute('target.ownerId', ownerId);
        this.gcProducer.publishActionEntry(gcEntry, err => {
            LifecycleMetrics.onKafkaPublish(log, 'GCTopic', 'archive', err, 1);
        });
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
                    if (err.code === 'ObjNotFound') {
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

                // set new ObjectMDArchive to ObjectMD
                objectMD.setArchive(new ObjectMDArchive(entry.archiveInfo));
                objectMD.setOriginOp('s3:LifecycleTransition:SetArchive');

                if (skipLocationDeletion) {
                    objectMD.setDataStoreName(coldLocation)
                        .setAmzStorageClass(coldLocation)
                        .setTransitionInProgress(false)
                        .setOriginOp('s3:LifecycleTransition')
                        .setUserMetadata({
                            'x-amz-meta-scal-s3-transition-attempt': undefined,
                        });
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
                // if error occurs, do not commit offset
                return done(err, { committable: false });
            }
            return done();
        });
    }
}

module.exports = LifecycleColdStatusArchiveTask;
