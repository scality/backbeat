const async = require('async');
const assert = require('assert');

const errors = require('arsenal').errors;
const BackbeatTask = require('../../../lib/tasks/BackbeatTask');
const ActionQueueEntry = require('../../../lib/models/ActionQueueEntry');
const ObjectMD = require('arsenal').models.ObjectMD;
const { LifecycleMetrics, getCopyLocationMetricsType } = require('../LifecycleMetrics');
const {
    TRANSITION_ATTEMPT_MD,
    getTransitionAttempt,
} = require('../../../lib/util/transitionAttempt');
const locationsConfig = require('../../../conf/locationConfig.json') || {};
/** @typedef { import('../objectProcessor/LifecycleObjectProcessor.js') } LifecycleObjectProcessor */

class LifecycleUpdateTransitionTask extends BackbeatTask {
    /**
     * Process a lifecycle object entry
     *
     * @constructor
     * @param {LifecycleObjectProcessor} proc - object processor instance
     */
    constructor(proc) {
        const procState = proc.getStateVars();
        super(procState.processConfig?.retry);
        Object.assign(this, procState);
    }

    getTargetAttribute(entry) {
        return entry.getAttribute('target');
    }

    _getMetadata(entry, log, done) {
        const { accountId } = this.getTargetAttribute(entry);
        const backbeatClient = this.getBackbeatMetadataProxy(accountId);
        if (!backbeatClient) {
            log.error('failed to get backbeat client', { accountId });
            return done(errors.InternalError
                .customizeDescription('Unable to obtain client'));
        }

        const { bucket, key, version } = this.getTargetAttribute(entry);
        return backbeatClient.getMetadata({
            bucket,
            objectKey: key,
            versionId: version,
        }, log, (err, blob) => {
            if (err) {
                log.error('error getting metadata blob from S3', Object.assign({
                    method: 'LifecycleUpdateTransitionTask._getMetadata',
                    error: err.message,
                }, entry.getLogInfo()));
                return done(err);
            }

            const res = ObjectMD.createFromBlob(blob.Body);
            if (res.error) {
                log.error('error parsing metadata blob', Object.assign({
                    error: res.error,
                    method: 'LifecycleUpdateTransitionTask._getMetadata',
                }, entry.getLogInfo()));
                return done(
                    errors.InternalError.
                        customizeDescription('error parsing metadata blob'));
            }
            return done(null, res.result);
        });
    }

    _updateMdWithTransition(entry, objMD) {
        // FIXME there should be a common method to set all location
        // fields, shared with cloudserver
        const { location } = entry.getAttribute('results');
        const newLocationName = entry.getAttribute('toLocation');
        objMD.setLocation(location)
            .setDataStoreName(newLocationName)
            .setAmzStorageClass(newLocationName)
            .setOriginOp('s3:LifecycleTransition')
            .setUserMetadata({ [TRANSITION_ATTEMPT_MD]: undefined })
            .setTransitionInProgress(false);
    }

    _putMetadata(entry, objMD, log, done) {
        const { accountId } = this.getTargetAttribute(entry);
        const backbeatClient = this.getBackbeatMetadataProxy(accountId);
        if (!backbeatClient) {
            log.error('failed to get backbeat client', { accountId });
            return done(errors.InternalError
                .customizeDescription('Unable to obtain client'));
        }
        //
        // TODO add a condition on metadata cookie and retry if needed
        const { bucket, key, version } = this.getTargetAttribute(entry);
        return backbeatClient.putMetadata({
            bucket,
            objectKey: key,
            versionId: version,
            mdBlob: objMD.getSerialized(),
        }, log, err => {
            LifecycleMetrics.onS3Request(log, 'putMetadata', 'transition', err);

            if (err) {
                log.error(
                    'an error occurred when updating metadata for transition',
                    Object.assign({
                        method: 'LifecycleUpdateTransitionTask._putMetadata',
                        error: err.message,
                    }, entry.getLogInfo()));
                return done(err);
            }

            return done();
        });
    }

    _garbageCollectLocation(entry, locations, log, done) {
        const { bucket, key, version, eTag, accountId, owner } = this.getTargetAttribute(entry);
        // Data stored on a CRR location belongs to the remote site: the copy we
        // just made is an extra local copy, the source must be left untouched.
        const { dataStoreName } = locations[0] || {};
        if (locationsConfig[dataStoreName]?.isCRR) {
            log.info('skipping garbage collection of data on a CRR location', {
                method: 'LifecycleUpdateTransitionTask._garbageCollectLocation',
                bucket,
                objectKey: key,
                versionId: version,
                dataStoreName,
            });
            return process.nextTick(done);
        }
        const gcEntry = ActionQueueEntry.create('deleteData')
              .addContext({
                  origin: 'lifecycle',
                  ruleType: 'transition',
                  reqId: log.getSerializedUids(),
                  bucketName: bucket,
                  objectKey: key,
                  versionId: version,
                  eTag,
              })
              .setAttribute('source', entry.getAttribute('source'))
              .setAttribute('serviceName', 'lifecycle-transition')
              .setAttribute('target.accountId', accountId)
              .setAttribute('target.owner', owner)
              .setAttribute('target.locations', locations);
        this.gcProducer.publishActionEntry(gcEntry);
        return process.nextTick(done);
    }

    _wasObjectModified(entry, objMD, log) {
        const { lastModified } = entry.getAttribute('target');
        if (!lastModified) {
            return false;
        }
        const objectWasModified = lastModified !== objMD.getLastModified();
        if (objectWasModified) {
            log.info('object LastModified date changed during lifecycle ' +
                     'transition processing',
            Object.assign({
                method: 'LifecycleUpdateTransitionTask._wasObjectModified',
            }, entry.getLogInfo()));
        }
        return objectWasModified;
    }

    /**
     * Updates metadata after a lifecycle transition has been successfully
     * And initiates garbage collection of the data
     * @param {ActionQueueEntry} entry - action entry to execute
     * @param {Logger} log - logger instance
     * @param {Function} done - callback funtion
     * @return {undefined}
     */
    handleSuccessfullTransition(entry, log, done) {
        let locationToGC;
        return async.waterfall([
            next => this._getMetadata(entry, log, (err, objMD) => {
                LifecycleMetrics.onS3Request(log, 'getMetadata', 'transition', err);
                next(err, objMD);
            }),
            (objMD, next) => {
                const oldLocation = objMD.getLocation();
                const newLocation = entry.getAttribute('results.location');
                if (this._wasObjectModified(entry, objMD, log)) {
                    locationToGC = newLocation;
                    return next();
                }
                const eTag = entry.getAttribute('target.eTag');
                // commit if MD5 did not change after transition
                // started and location has effectively been
                // updated, rollback if MD5 changed
                if (eTag !== `"${objMD.getContentMd5()}"`) {
                    log.info('object ETag has changed during lifecycle ' +
                             'transition processing',
                    Object.assign({
                        method:
                        'LifecycleUpdateTransitionTask.processActionEntry',
                    }, entry.getLogInfo()));
                    locationToGC = newLocation;
                    return next();
                }
                try {
                    assert.notDeepStrictEqual(oldLocation, newLocation);
                } catch {
                    log.info('duplicate location update, skipping',
                    Object.assign({
                        method:
                        'LifecycleUpdateTransitionTask.processActionEntry',
                    }, entry.getLogInfo()));
                    return next();
                }
                locationToGC = oldLocation;

                this._updateMdWithTransition(entry, objMD);
                return this._putMetadata(entry, objMD, log, err => {
                    const transitionTime = entry.getAttribute('metrics.transitionTime') ||
                        objMD.getTransitionTime();
                    const locationName = entry.getAttribute('toLocation');
                    LifecycleMetrics.onLifecycleCompleted(log, getCopyLocationMetricsType(entry),
                        locationName, Date.now() - Date.parse(transitionTime));
                    next(err);
                });
            },
            next => {
                log.end().info('metadata updated for transition',
                    entry.getLogInfo());

                if (!locationToGC) {
                    return next();
                }
                return this._garbageCollectLocation(
                    entry, locationToGC, log, next);
            },
        ], done);
    }

    /**
     * Requeue the object to get transitioned again
     * @param {ActionQueueEntry} entry - action entry to execute
     * @param {Logger} log - logger instance
     * @param {Function} done - callback funtion
     * @return {undefined}
     */
    handleFailedTransition(entry, log, done) {
        return async.waterfall([
            next => this._getMetadata(entry, log, (err, objMD) => {
                LifecycleMetrics.onS3Request(log, 'getMetadata', 'transition', err);
                next(err, objMD);
            }),
            (objMD, next) => {
                const tryCount = (getTransitionAttempt(objMD) || 0) + 1;

                objMD.setTransitionInProgress(false)
                    .setUserMetadata({ [TRANSITION_ATTEMPT_MD]: tryCount });

                return this._putMetadata(entry, objMD, log, next);
            },
        ], done);
    }

    /**
     * Actions published by the lifecycle conductor carry the account id; those
     * published by the queue populator (pull replication) only know the object
     * owner's canonical id. Resolve it once, up-front, so the rest of the task
     * -and the garbage collection entry it emits- can use `target.accountId`
     * as usual.
     * @param {ActionQueueEntry} entry - action entry to execute
     * @param {Logger} log - logger instance
     * @param {Function} cb - callback function
     * @return {undefined}
     */
    _resolveAccountId(entry, log, cb) {
        const { accountId, owner } = this.getTargetAttribute(entry);
        if (accountId) {
            return process.nextTick(cb);
        }

        if (!owner) {
            // Every publisher sets one or the other, so this is a malformed
            // entry: log it, and let the task fail on its own further down
            // rather than retrying something that cannot be fixed.
            log.error('cannot resolve account id: entry has no account id nor owner');
            return process.nextTick(cb);
        }

        log.debug('no account id in entry, resolving from canonical id', { owner });
        return this.getAccountId(owner, log, (err, resolvedAccountId) => {
            if (err) {
                return cb(err);
            }
            if (resolvedAccountId) {
                entry.setAttribute('target.accountId', resolvedAccountId);
            }
            return cb();
        });
    }

    /**
     *
     * @param {ActionQueueEntry} entry - action entry to execute
     * @param {Function} done - callback funtion
     * @return {undefined}
     */
    processActionEntry(entry, done) {
        const log = this.logger.newRequestLogger();
        entry.addLoggedAttributes({
            bucketName: 'target.bucket',
            objectKey: 'target.key',
            versionId: 'target.version',
            eTag: 'target.eTag',
            lastModified: 'target.lastModified',
        });
        log.addDefaultFields(entry.getLogInfo());

        return this._resolveAccountId(entry, log, err => {
            if (err) {
                return done(err);
            }

            if (entry.getStatus() === 'success') {
                return this.handleSuccessfullTransition(entry, log, done);
            }

            return this.handleFailedTransition(entry, log, done);
        });
    }
}

module.exports = LifecycleUpdateTransitionTask;
