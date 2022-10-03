const async = require('async');
const errors = require('arsenal').errors;
const { ObjectMD } = require('arsenal').models;

const { attachReqUids } = require('../../../lib/clients/utils');
const BackbeatTask = require('../../../lib/tasks/BackbeatTask');

class GarbageCollectorTask extends BackbeatTask {
    /**
     * Process a lifecycle object entry
     *
     * @constructor
     * @param {GarbageCollector} gc - garbage collector instance
     */
    constructor(gc) {
        super();
        const gcState = gc.getStateVars();
        Object.assign(this, gcState);
    }

    _getMetadata(entry, log, done) {
        const { accountId } = entry.getAttribute('target');
        const backbeatClient = this.getBackbeatMetadataProxy(accountId);
        if (!backbeatClient) {
            log.error('failed to get backbeat client', { accountId });
            return done(errors.InternalError
                .customizeDescription('Unable to obtain client'));
        }

        const { bucket, key, version } = entry.getAttribute('target');
        return backbeatClient.getMetadata({
            bucket,
            objectKey: key,
            versionId: version,
        }, log, (err, blob) => {
            if (err) {
                log.error('error getting metadata blob from S3', Object.assign({
                    method: 'GarbageCollectorTask._getMetadata',
                    error: err.message,
                }, entry.getLogInfo()));
                return done(err);
            }

            const res = ObjectMD.createFromBlob(blob.Body);
            if (res.error) {
                log.error('error parsing metadata blob', Object.assign({
                    error: res.error,
                    method: 'GarbageCollectorTask._getMetadata',
                }, entry.getLogInfo()));
                return done(
                    errors.InternalError.
                        customizeDescription('error parsing metadata blob'));
            }
            return done(null, res.result);
        });
    }

    _putMetadata(entry, objMD, log, done) {
        const { accountId } = entry.getAttribute('target');
        const backbeatClient = this.getBackbeatMetadataProxy(accountId);
        if (!backbeatClient) {
            log.error('failed to get backbeat client', { accountId });
            return done(errors.InternalError
                .customizeDescription('Unable to obtain client'));
        }

        const { bucket, key, version } = entry.getAttribute('target');
        return backbeatClient.putMetadata({
            bucket,
            objectKey: key,
            versionId: version,
            mdBlob: objMD.getSerialized(),
        }, log, err => {
            if (err) {
                log.error(
                    'an error occurred when updating metadata for transition',
                    Object.assign({
                        method: 'GarbageCollectorTask._putMetadata',
                        error: err.message,
                    }, entry.getLogInfo()));
                return done(err);
            }

            return done();
        });
    }

    _batchDeleteData(params, entry, log, done) {
        log.debug('action execution starts', entry.getLogInfo());
        const { accountId } = entry.getAttribute('target');
        const backbeatClient = this.getBackbeatClient(accountId);

        if (!backbeatClient) {
            log.error('failed to get backbeat client', { accountId });
            return done(errors.InternalError
                .customizeDescription('Unable to obtain client'));
        }

        const req = backbeatClient.batchDelete(params);
        attachReqUids(req, log);
        return req.send(done);
    }

    _executeDeleteData(entry, log, done) {
        const { locations } = entry.getAttribute('target');
        const params = {
            Locations: locations.map(location => ({
                key: location.key,
                dataStoreName: location.dataStoreName,
                size: location.size,
                dataStoreVersionId: location.dataStoreVersionId,
            })),
            IfUnmodifiedSince: entry.getAttribute('source.lastModified'),
            Bucket: entry.getAttribute('source.bucket'),
            Key: entry.getAttribute('source.objectKey'),
            StorageClass: entry.getAttribute('source.storageClass'),
            Tags: JSON.stringify({
                'scal-delete-marker': 'true',
                'scal-delete-service': entry.getAttribute('serviceName'),
            }),
        };

        this._batchDeleteData(params, entry, log, err => {
            entry.setEnd(err);
            log.info('action execution ended', entry.getLogInfo());
            if (err && err.statusCode === 412) {
                log.info('precondition for garbage collection was not met',
                    Object.assign({
                        method: 'LifecycleObjectTask._executeDeleteData',
                        lastModified: entry.getAttribute('source.lastModified'),
                    }, entry.getLogInfo()));
                return done();
            }
            if (err) {
                log.error('an error occurred on deleteData method to ' +
                    'backbeat route',
                    Object.assign({
                        method: 'LifecycleObjectTask._executeDeleteData',
                        error: err.message,
                        httpStatus: err.statusCode,
                    }, entry.getLogInfo()));
                return done(err);
            }
            return done();
        });
    }

    _deleteArchivedSourceData(entry, log, done) {
        const { bucket, key, version, oldLocation, newLocation } = entry.getAttribute('target');

        async.waterfall([
            next => this._getMetadata(entry, log, next),
            (objMD, next) => {
                const locations = objMD.getLocation();

                const params = {
                    Locations: locations.map(location => ({
                        key: location.key,
                        dataStoreName: location.dataStoreName,
                        size: location.size,
                        dataStoreVersionId: location.dataStoreVersionId,
                    })),
                    Bucket: bucket,
                    Key: key,
                    StorageClass: oldLocation,
                    Tags: JSON.stringify({
                        'scal-delete-marker': 'true',
                        'scal-delete-service': entry.getAttribute('serviceName'),
                    }),
                };

                this._batchDeleteData(params, entry, log, err => {
                    entry.setEnd(err);
                    log.info('action execution ended', entry.getLogInfo());

                    if (err && err.statusCode === 404) {
                        log.info('unable to find data to delete',
                            Object.assign({
                                method: 'GarbageCollectorTask._deleteArchivedSourceData',
                                bucket,
                                key,
                                version,
                            }, entry.getLogInfo));
                        return next(null, objMD);
                    }

                    if (err) {
                        log.error('an error occurred on batchDelete backbeat route',
                            Object.assign({
                                method: 'GarbageCollectorTask._deleteArchivedSourceData',
                                error: err.message,
                                httpStatus: err.statusCode,
                            }, entry.getLogInfo()));
                        return next(err);
                    }
                    return next(null, objMD);
                });
            },
            (objMD, next) => {
                log.debug('successfully deleted location data', {
                    bucket,
                    key,
                    version,
                });

                objMD.setLocation()
                    .setDataStoreName(newLocation)
                    .setAmzStorageClass(newLocation)
                    .setTransitionInProgress(false)
                    .setUserMetadata({
                        'x-amz-meta-scal-s3-transition-attempt': undefined,
                    });
                return this._putMetadata(entry, objMD, log, err => {
                    if (!err) {
                        log.end().info('completed expiration of archived data',
                            entry.getLogInfo());
                    }

                    next(err);
                });
            },
        ], err => {
            if (err) {
                // if error occurs, do not commit offset
                return done(err, { committable: false });
            }
            return done();
        });
    }

    /**
     * Execute the action specified in kafka queue entry
     *
     * @param {ActionQueueEntry} entry - kafka queue entry object
     * @param {String} entry.action - entry action name (e.g. 'deleteData')
     * @param {Object} entry.target - entry action target object
     * @param {Function} done - callback funtion
     * @return {undefined}
     */

    processActionEntry(entry, done) {
        const log = this.logger.newRequestLogger();

        switch (entry.getActionType()) {
        case 'deleteData':
            return this._executeDeleteData(entry, log, done);
        case 'deleteArchivedSourceData':
            return this._deleteArchivedSourceData(entry, log, done);
        default:
            log.warn('skipped unsupported action', entry.getLogInfo());
            return process.nextTick(done);
        }
    }
}

module.exports = GarbageCollectorTask;
