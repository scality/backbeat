'use strict';

const { callbackify, promisify } = require('util');

const { errors } = require('@scality/arsenal');
const ObjectMD = require('@scality/arsenal').models.ObjectMD;

const BackbeatTask = require('../../../lib/tasks/BackbeatTask');
const { LifecycleMetrics } = require('../LifecycleMetrics');
/** @typedef { import('../objectProcessor/LifecycleObjectProcessor.js') } LifecycleObjectProcessor */

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
        super(procState.processConfig?.retry);
        Object.assign(this, procState);
        this.processName = processName;
    }

    /**
     * Get a promisified metadata client for an account, recording the request metrics
     *
     * @param {string} accountId - account id
     * @return {Object} client with async getMetadata(params, log) and putMetadata(params, log)
     */
    _getClient(accountId) {
        const client = this.getBackbeatMetadataProxy(accountId);
        if (!client) {
            throw errors.InternalError.customizeDescription(
                `Unable to obtain client for account ${accountId}`,
            );
        }

        const wrap = method => promisify((params, log, cb) =>
            client[method](params, log, (err, res) => {
                LifecycleMetrics.onS3Request(log, method, this.processName, err);
                cb(err, res);
            }));
        return {
            getMetadata: wrap('getMetadata'),
            putMetadata: wrap('putMetadata'),
        };
    }

    /**
     * Requeue a single object version
     *
     * @param {Object} client - metadata client, see _getClient
     * @param {Object} target - object to requeue
     * @param {string} target.accountId - account id
     * @param {string} target.bucketName - bucket name
     * @param {string} target.objectKey - object key
     * @param {string} [target.objectVersion] - object version id
     * @param {string} target.eTag - expected etag, as a JSON string
     * @param {number} [target.try] - attempt count
     * @param {string} [target.location] - cold location the object is transitioning to
     * @param {Logger} bucketLogger - logger
     * @return {Promise<number>} 1 if the object metadata was updated, 0 otherwise
     */
    async requeueObjectVersion(client, target, bucketLogger) {
        const { bucketName, objectKey, objectVersion } = target;

        const params = {
            bucket: bucketName,
            objectKey,
        };
        if (objectVersion) {
            params.versionId = objectVersion;
        }

        const log = this.logger.newRequestLogger(bucketLogger.getUids());
        log.addDefaultFields(target);

        const blob = await client.getMetadata(params, log);
        const { result: md, error } = ObjectMD.createFromBlob(blob.Body);
        if (error) {
            throw error;
        }

        if (!this.updateObjectMD(md, log, target)) {
            return 0;
        }

        await client.putMetadata({ ...params, mdBlob: md.getSerialized() }, log);
        return 1;
    }

    /**
     * Execute the action specified in action entry
     *
     * @param {ActionQueueEntry} entry - action entry to execute
     * @param {Function} [done] - callback, for callers not using the returned promise
     * @return {Promise<undefined>} resolved once the entry is processed
     */
    async processActionEntry(entry, done) {
        if (done) {
            return callbackify(() => this.processActionEntry(entry))(done);
        }

        const log = this.logger.newRequestLogger(entry.actionId);
        const { byAccount, location } = entry.getAttribute('target') || {};

        let objectCount = 0;
        try {
            for (const [accountId, buckets] of Object.entries(byAccount)) {
                objectCount += await this.handleBatch(accountId, buckets, location, log);
            }
        } catch (err) {
            log.error('could not process message', { error: err, objectCount });
            throw err;
        }

        log.info('processed requeue message', { objectCount });
        return undefined;
    }

    /**
     * Requeue the objects of an account: buckets in parallel, objects of a bucket in sequence
     *
     * @param {string} accountId - account id
     * @param {Object} buckets - objects to requeue, by bucket name
     * @param {string} [location] - cold location the objects are transitioning to
     * @param {Logger} log - request logger
     * @return {Promise<number>} number of requeued objects
     */
    async handleBatch(accountId, buckets, location, log) {
        const client = this._getClient(accountId);

        const counts = await Promise.all(Object.entries(buckets).map(async ([bucketName, objects]) => {
            let objsPerBucket = 0;
            for (const object of objects) {
                const target = { ...object, accountId, bucketName, location };
                try {
                    objsPerBucket += await this.requeueObjectVersion(client, target, log);
                } catch (err) {
                    if (err.name !== 'ObjNotFound' && err.name !== 'NoSuchBucket') {
                        throw err;
                    }

                    const { objectKey, objectVersion } = object;
                    log.warn(`${err.name}, skipping`, { bucketName, objectKey, objectVersion });
                }
            }
            return objsPerBucket;
        }));

        return counts.reduce((acc, v) => acc + v, 0);
    }
}

module.exports = {
    LifecycleRequeueTask
};
