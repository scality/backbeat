'use strict'; // eslint-disable-line

const async = require('async');

const { errors } = require('arsenal');
const ObjectMD = require('arsenal').models.ObjectMD;

const BackbeatTask = require('../../../lib/tasks/BackbeatTask');
const { LifecycleMetrics } = require('../LifecycleMetrics');

class LifecycleRequeueTask extends BackbeatTask {

    /**
     * Process a lifecycle object entry
     *
     * @constructor
     * @param {LifecycleObjectProcessor} proc - object processor instance
     * @param {string} processName - name of the process
     */
    constructor(proc, processName) {
        const procState = proc.getStateVars();
        super();
        Object.assign(this, procState);
        this.processName = processName;
    }

    requeueObjectVersion(accountId, bucketName, objectKey, objectVersion, etag, try_, bucketLogger, cb) {
        const client = this.getBackbeatMetadataProxy(accountId);
        if (!client) {
            return cb(errors.InternalError.customizeDescription(
                `Unable to obtain client for account ${accountId}`,
            ));
        }

        const params = {
            bucket: bucketName,
            objectKey,
        };
        if (objectVersion) {
            params.versionId = objectVersion;
        }

        const log = this.logger.newRequestLogger(bucketLogger.getUids());
        log.addDefaultFields({
            accountId,
            bucketName,
            objectKey,
            objectVersion,
            etag,
            try: try_,
        });

        return client.getMetadata(params, log, (err, blob) => {
            LifecycleMetrics.onS3Request(log, 'getMetadata', this.processName, err);
            if (err) {
                return cb(err);
            }

            const { result: md, error } = ObjectMD.createFromBlob(blob.Body);
            if (error) {
                return cb(error);
            }

            if (!this.updateObjectMD(md, try_, log, etag)) {
                return cb(null, 0);
            }

            return client.putMetadata({ ...params, mdBlob: md.getSerialized() }, log,
                err => {
                    LifecycleMetrics.onS3Request(log, 'putMetadata', this.processName, err);
                    if (err) {
                        return cb(err);
                    }

                    return cb(null, 1);
                }
            );
        });
    }

    /**
     * Execute the action specified in action entry
     *
     * @param {ActionQueueEntry} entry - action entry to execute
     * @param {Function} done - callback funtion
     * @return {undefined}
     */
    processActionEntry(entry, done) {
        const log = this.logger.newRequestLogger(entry.actionId);
        const { byAccount } = entry.getAttribute('target') || {};

        async.reduce(
            Object.keys(byAccount),
            0,
            (objsPerBatch, accountId, done) => {
                const buckets = byAccount[accountId] || {};
                const bucketNames = Object.keys(buckets);
                this.handleBatch(
                    accountId,
                    buckets,
                    bucketNames,
                    log,
                    (err, res) => {
                        if (err) {
                            return done(err);
                        }

                        const sum = res.reduce((acc, v) => acc + v, 0);
                        return done(null, sum + objsPerBatch);
                    }
                );
            },
            (err, objectCount) => {
                if (err) {
                    log.error('could not process message', { error: err, objectCount });
                } else {
                    log.info('processed requeue message', { objectCount });
                }

                done(err);
            }
        );
    }

    handleBatch(accountId, buckets, bucketNames, log, cb) {
        async.map(
            bucketNames,
            (bucketName, next) =>
                async.reduce(
                    buckets[bucketName] || [],
                    0,
                    (objsPerBucket, { objectKey, objectVersion, eTag, ...rest }, nextObject) =>
                        this.requeueObjectVersion(
                            accountId,
                            bucketName,
                            objectKey,
                            objectVersion,
                            eTag,
                            rest.try,
                            log,
                            (err, res) => {
                                if (err) {
                                    return nextObject(err);
                                }

                                return nextObject(null, res + objsPerBucket);
                            }
                        ),
                    (err, res) => {
                        if (err) {
                            return next(err);
                        }

                        return next(null, res);
                    }
                ),
            (err, res) => {
                if (err) {
                    return cb(err);
                }

                return cb(null, res);
            }
        );
    }
}

module.exports = {
    LifecycleRequeueTask
};
