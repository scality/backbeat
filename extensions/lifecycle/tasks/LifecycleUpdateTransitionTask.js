const async = require('async');

const errors = require('arsenal').errors;
const BackbeatTask = require('../../../lib/tasks/BackbeatTask');
const ActionQueueEntry = require('../../../lib/models/ActionQueueEntry');
const ObjectMD = require('arsenal').models.ObjectMD;


class LifecycleUpdateTransitionTask extends BackbeatTask {
    /**
     * Process a lifecycle object entry
     *
     * @constructor
     * @param {QueueProcessor} qp - queue processor instance
     */
    constructor(qp) {
        const qpState = qp.getStateVars();
        super();
        Object.assign(this, qpState);
    }

    _getMetadata(entry, log, done) {
        this.backbeatClient.getMetadata({
            bucket: entry.getAttribute('target.bucket'),
            objectKey: entry.getAttribute('target.key'),
            versionId: entry.getAttribute('target.version'),
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
            .setAmzStorageClass(newLocationName);
    }

    _putMetadata(entry, objMD, log, done) {
        // TODO add a condition on metadata cookie and retry if needed
        this.backbeatClient.putMetadata({
            bucket: entry.getAttribute('target.bucket'),
            objectKey: entry.getAttribute('target.key'),
            versionId: entry.getAttribute('target.version'),
            mdBlob: objMD.getSerialized(),
        }, log, err => {
            if (err) {
                log.error(
                    'an error occurred when updating metadata for transition',
                    Object.assign({
                        method: 'LifecycleUpdateTransitionTask._putMetadata',
                        error: err.message,
                    }, entry.getLogInfo()));
                return done(err);
            }
            log.end().info('metadata updated for transition',
                           entry.getLogInfo());
            return done();
        });
    }

    _garbageCollectLocation(entry, locations, log, done) {
        const gcEntry = ActionQueueEntry.create('deleteData')
              .addContext({
                  origin: 'lifecycle',
                  ruleType: 'transition',
                  reqId: log.getSerializedUids(),
                  bucketName: entry.getAttribute('target.bucket'),
                  objectKey: entry.getAttribute('target.key'),
                  versionId: entry.getAttribute('target.version'),
              })
              .setAttribute('target.locations', locations);
        this.gcProducer.publishActionEntry(gcEntry, done);
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
        });
        if (entry.getStatus() === 'success') {
            let oldLocation;
            return async.waterfall([
                next => this._getMetadata(entry, log, next),
                (objMD, next) => {
                    oldLocation = objMD.getLocation();
                    this._updateMdWithTransition(entry, objMD, log);
                    this._putMetadata(entry, objMD, log, next);
                },
                next => this._garbageCollectLocation(
                    entry, oldLocation, log, next),
            ], done);
        }
        // don't update metadata if the copy failed
        return process.nextTick(done);
    }
}

module.exports = LifecycleUpdateTransitionTask;
