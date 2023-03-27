const async = require('async');
const assert = require('assert');

const errors = require('arsenal').errors;
const BackbeatTask = require('../../../lib/tasks/BackbeatTask');
const ActionQueueEntry = require('../../../lib/models/ActionQueueEntry');
const ObjectMD = require('arsenal').models.ObjectMD;
const { LifecycleMetrics } = require('../LifecycleMetrics');


class LifecycleUpdateTransitionTask extends BackbeatTask {
    /**
     * Process a lifecycle object entry
     *
     * @constructor
     * @param {LifecycleObjectProcessor} proc - object processor instance
     */
    constructor(proc) {
        const procState = proc.getStateVars();
        super();
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
            LifecycleMetrics.onS3Request(log, 'getMetadata', 'transition', err);

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
            .setOriginOp('s3:LifecycleTransition');
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
     * Execute the action specified in action entry to update metadata
     * after an object data has been transitioned to a new storage
     * class
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
        if (entry.getStatus() === 'success') {
            let locationToGC;
            return async.waterfall([
                next => this._getMetadata(entry, log, next),
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
                    } catch (err) {
                        log.info('duplicate location update, skipping',
                        Object.assign({
                            method:
                            'LifecycleUpdateTransitionTask.processActionEntry',
                        }, entry.getLogInfo()));
                        return next();
                    }
                    locationToGC = oldLocation;
                    this._updateMdWithTransition(entry, objMD);
                    return this._putMetadata(entry, objMD, log, next);
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
        // don't update metadata if the copy failed
        return process.nextTick(done);
    }
}

module.exports = LifecycleUpdateTransitionTask;
