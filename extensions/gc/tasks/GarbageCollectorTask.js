const async = require('async');
const errors = require('arsenal').errors;
const { ObjectMD } = require('arsenal').models;

const BackbeatTask = require('../../../lib/tasks/BackbeatTask');
const { BatchDeleteCommand } = require('@scality/cloudserverclient');
const { GarbageCollectorMetrics } = require('../GarbageCollectorMetrics');
const { isCRRLocation } = require('../../../lib/util/locations');
/** @typedef { import('../GarbageCollector.js') } GarbageCollector */

class GarbageCollectorTask extends BackbeatTask {
    /**
     * Process a lifecycle object entry
     *
     * @constructor
     * @param {GarbageCollector} gc - garbage collector instance
     */
    constructor(gc) {
        const gcState = gc.getStateVars();
        super(gcState.gcConfig?.consumer.retry);
        Object.assign(this, gcState);
    }

    // helper method needed for replication generated entries in which the
    // account id information is missing.
    // TODO BB-367: replace once replication services uses assume role
    _getAccountId(entry, log, cb) {
        const { accountId, owner } = entry.getAttribute('target');

        if (accountId) {
            return process.nextTick(cb, null, accountId);
        }

        log.debug('unable to find account id in entry; performing vault request', {
            owner,
        });
        return this.getAccountId(owner, log, cb);
    }

    _getMetadata(entry, log, done) {
        this._getAccountId(entry, log, (err, accountId) => {
            if (err) {
                return done(err);
            }

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
        });
    }

    _putMetadata(entry, objMD, log, done) {
        this._getAccountId(entry, log, (err, accountId) => {
            if (err) {
                return done(err);
            }

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
        });
    }

    _batchDeleteData(params, entry, log, done) {
        log.debug('action execution starts', entry.getLogInfo());
        this._getAccountId(entry, log, (err, accountId) => {
            if (err) {
                return done(err);
            }

            const backbeatClient = this.getBackbeatClient(accountId);
            if (!backbeatClient) {
                log.error('failed to get backbeat client', { accountId });
                return done(errors.InternalError
                    .customizeDescription('Unable to obtain client'));
            }

            const command = new BatchDeleteCommand({
                ...params,
                RequestUids: log.getSerializedUids(),
            });

            return backbeatClient.send(command)
                .then(() => done())
                .catch(err => done(err));
        });
    }

    _executeDeleteDataOnce(entry, log, done) {
        const { locations } = entry.getAttribute('target');
        const ruleType = entry.getContextAttribute('ruleType');
        // Last line of defense: whoever published this entry, data on a CRR
        // location belongs to the remote site and must never be deleted.
        if (locations.some(location => isCRRLocation(location.dataStoreName))) {
            log.warn('refusing to delete data on a CRR location', Object.assign({
                method: 'GarbageCollectorTask._executeDeleteDataOnce',
                dataStoreName: locations[0]?.dataStoreName,
                ruleType,
            }, entry.getLogInfo()));
            entry.setEnd(null);
            return process.nextTick(done);
        }
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

        return this._batchDeleteData(params, entry, log, err => {
            // ruleType can be either `transition` or `restore` (for restore-expiration)
            GarbageCollectorMetrics.onS3Request(log, 'batchdelete', ruleType, err);
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

            GarbageCollectorMetrics.onGcCompleted(log, ruleType,
                locations[0]?.dataStoreName, Date.now() - entry.getAttribute('timestamp'));
            return done();
        });
    }

    _executeDeleteData(entry, log, done) {
        const logFields = {
            bucket: entry.getAttribute('source.bucket'),
            objectKey: entry.getAttribute('source.objectKey'),
            storageClass: entry.getAttribute('source.storageClass'),
            ruleType: entry.getContextAttribute('ruleType'),
        };

        this.retry({
            actionDesc: 'execute delete data',
            logFields,
            actionFunc: cb => this._executeDeleteDataOnce(entry, log, cb),
            shouldRetryFunc: err => err.retryable,
            log,
        }, err => {
            if (err) {
                const ruleType = entry.getContextAttribute('ruleType');
                log.error('task failed permanently after retries, committing offset', {
                    method: 'GarbageCollectorTask._executeDeleteData',
                    error: err.message,
                    locations: entry.getAttribute('target.locations'),
                    ...entry.getLogInfo(),
                });
                GarbageCollectorMetrics.onGcFailed(log, ruleType,
                    entry.getAttribute('source.storageClass'));
            }
            return done(err);
        });
    }

    _deleteArchivedSourceDataOnce(entry, log, done) {
        const { bucket, key, version, oldLocation, newLocation } = entry.getAttribute('target');

        async.waterfall([
            next => this._getMetadata(entry, log, (err, objMD) => {
                GarbageCollectorMetrics.onS3Request(log, 'getMetadata', 'archive', err);
                return next(err, objMD);
            }),
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
                    GarbageCollectorMetrics.onS3Request(log, 'batchdelete', 'archive', err);
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
                        // Stash the parts on the entry so the outer
                        // retry-wrapper's failure log (in
                        // _deleteArchivedSourceData) can surface them for ops
                        // to reclaim manually if the delete never succeeds.
                        entry.setAttribute('target.locations', locations);
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
                    .setOriginOp('s3:LifecycleTransition')
                    .setTransitionInProgress(false)
                    .setUserMetadata({
                        'x-amz-meta-scal-s3-transition-attempt': undefined,
                    });
                this._putMetadata(entry, objMD, log, err => {
                    GarbageCollectorMetrics.onS3Request(log, 'putMetadata', 'archive', err);
                    if (!err) {
                        log.end().info('completed expiration of archived data',
                            entry.getLogInfo());
                        GarbageCollectorMetrics.onGcCompleted(log, 'archive',
                            entry.getAttribute('target.oldLocation'),
                            Date.now() - entry.getAttribute('timestamp'));
                    }

                    next(err);
                });
            },
        ], err => done(err));
    }

    _deleteArchivedSourceData(entry, log, done) {
        this.retry({
            actionDesc: 'delete archived source data',
            logFields: { bucket: entry.getAttribute('target').bucket,
                key: entry.getAttribute('target').key, version: entry.getAttribute('target').version },
            actionFunc: cb => this._deleteArchivedSourceDataOnce(entry, log, cb),
            shouldRetryFunc: err => err.retryable,
            log,
        }, err => {
            if (err && err.name !== 'ObjNotFound' && err.name !== 'NoSuchBucket') {
                log.error('task failed permanently after retries, committing offset', {
                    method: 'GarbageCollectorTask._deleteArchivedSourceData',
                    error: err.message,
                    locations: entry.getAttribute('target.locations'),
                    ...entry.getLogInfo(),
                });
                GarbageCollectorMetrics.onGcFailed(log, 'archive',
                    entry.getAttribute('target.oldLocation'));
            }
            return done(err);
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
