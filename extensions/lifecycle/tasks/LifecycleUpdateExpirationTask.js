const async = require('async');

const BackbeatTask = require('../../../lib/tasks/BackbeatTask');
const ObjectMD = require('arsenal').models.ObjectMD;

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
        const { bucket, key, version } = entry.getAttribute('target');
        this.backbeatClient.getMetadata({
            bucket,
            objectKey: key,
            versionId: version,
        }, log, (err, blob) => {
            if (err) {
                log.error('error getting metadata blob from S3', Object.assign({
                    method: 'LifecycleUpdateExpirationTask._getMetadata',
                    error: err.message,
                }, entry.getLogInfo()));
                return done(err);
            }
            const res = ObjectMD.createFromBlob(blob.Body);
            if (res.error) {
                log.error('error parsing metadata blob', Object.assign({
                    error: res.error,
                    method: 'LifecycleUpdateExpirationTask._getMetadata',
                }, entry.getLogInfo()));
                return done(
                    errors.InternalError.
                        customizeDescription('error parsing metadata blob'));
            }
            return done(null, res.result);
        });
    }

    _putMetadata(entry, objMD, log, done) {
        const { bucket, key, version } = entry.getAttribute('target');
        this.backbeatClient.putMetadata({
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
                return done(err);
            }
            log.end().info('metadata updated for transition', entry.getLogInfo());
            return done();
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
                objMD.setArchive(undefined);
                this._putMetadata(entry, objMD, log, err => next(err, objMD));
            },
            (objMD, next) => this._garbageCollectLocation(
                entry, objMD.getLocation(), log, next,
            ),
        ], done);
    }
}

module.exports = LifecycleUpdateExpirationTask;
