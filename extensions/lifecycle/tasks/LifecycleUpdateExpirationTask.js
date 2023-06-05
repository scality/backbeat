const async = require('async');

const { errors } = require('arsenal');
const ObjectMD = require('arsenal').models.ObjectMD;
const BackbeatTask = require('../../../lib/tasks/BackbeatTask');
const ActionQueueEntry = require('../../../lib/models/ActionQueueEntry');
const locations = require('../../../conf/locationConfig.json') || {};

class LifecycleUpdateExpirationTask extends BackbeatTask {
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

    _getMetadata(entry, log, done) {
        const {
            accountId,
            bucket,
            key,
            version,
        } = entry.getAttribute('target');

        const backbeatClient = this.getBackbeatMetadataProxy(accountId);
        if (!backbeatClient) {
            log.error('failed to get backbeat client', { accountId });
            done(errors.InternalError.customizeDescription(
                'Unable to obtain client',
            ));
            return;
        }

        backbeatClient.getMetadata({
            bucket,
            objectKey: key,
            versionId: version,
        }, log, (err, blob) => {
            if (err) {
                log.error('error getting metadata blob from S3', Object.assign({
                    method: 'LifecycleUpdateExpirationTask._getMetadata',
                    error: err.message,
                }, entry.getLogInfo()));
                done(err);
                return;
            }
            const res = ObjectMD.createFromBlob(blob.Body);
            if (res.error) {
                log.error('error parsing metadata blob', Object.assign({
                    error: res.error,
                    method: 'LifecycleUpdateExpirationTask._getMetadata',
                }, entry.getLogInfo()));
                done(errors.InternalError.customizeDescription(
                    'error parsing metadata blob'
                ));
            } else {
                done(null, res.result);
            }
        });
        return;
    }

    _putMetadata(entry, objMD, log, done) {
        const {
            accountId,
            bucket,
            key,
            version,
        } = entry.getAttribute('target');

        const backbeatClient = this.getBackbeatMetadataProxy(accountId);
        if (!backbeatClient) {
            log.error('failed to get backbeat client', { accountId });
            done(errors.InternalError.customizeDescription(
                'Unable to obtain client',
            ));
            return;
        }

        backbeatClient.putMetadata({
            bucket,
            objectKey: key,
            versionId: version,
            mdBlob: objMD.getSerialized(),
        }, log, err => {
            if (err) {
                log.error(
                    'an error occurred when updating metadata for transition',
                    Object.assign(
                        {
                            method: 'LifecycleUpdateExpirationTask._putMetadata',
                            error: err.message,
                        },
                        entry.getLogInfo(),
                    )
                );
                done(err);
                return;
            } else {
                log.end().info('metadata updated for transition', entry.getLogInfo());
                done();
                return;
            }
        });
    }

    _garbageCollectLocation(entry, locations, log, done) {
        const { bucket, key, version, eTag } = entry.getAttribute('target');
        const gcEntry = ActionQueueEntry.create('deleteData')
              .addContext({
                  origin: 'lifecycle',
                  ruleType: 'restore',
                  reqId: log.getSerializedUids(),
                  bucketName: bucket,
                  objectKey: key,
                  versionId: version,
                  eTag,
              })
              .setAttribute('source', entry.getAttribute('source'))
              .setAttribute('serviceName', 'lifecycle-transition')
              .setAttribute('target.locations', locations);
        this.gcProducer.publishActionEntry(gcEntry);
        return process.nextTick(done);
    }

    /**
     * Execute the action specified in action entry to update expirations on an object
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
        });

        async.waterfall([
            next => this._getMetadata(entry, log, next),
            (objMD, next) => {
                const coldLocation = entry.getAttribute('target.location') ||
                    // If location not specified, use the first (and only) location
                    // This is a temporary fix, until Sorbet is fixed to provide the information
                    Object.keys(locations).find(name => locations[name].isCold);

                const archive = objMD.getArchive();
                // eslint-disable-next-line
                console.log(Object.getOwnPropertyNames(archive).filter(item => typeof archive[item] === 'function'));

                // Confirm the object has indeed expired: it can happen that the
                // expiration date is updated while the expiry was "in-flight" (e.g.
                // queued for expiry but not yet expired)
                if (new Date(archive.getRestoreWillExpireAt()) > new Date()) {
                    return process.nextTick(done);
                }

                // Reset archive flags to no longer show it as restored
                objMD.setArchive({
                    archiveInfo: archive.archiveInfo,
                });
                objMD.setAmzRestore();
                objMD.setDataStoreName(coldLocation);
                objMD.setAmzStorageClass(coldLocation);
                objMD.setTransitionInProgress(false);
                objMD.setOriginOp('s3:ObjectRestore:Delete');
                return this._putMetadata(entry, objMD, log, err => next(err, objMD));
            },
            (objMD, next) => this._garbageCollectLocation(
                entry, objMD.getLocation(), log, next,
            ),
        ], done);
    }
}

module.exports = LifecycleUpdateExpirationTask;
