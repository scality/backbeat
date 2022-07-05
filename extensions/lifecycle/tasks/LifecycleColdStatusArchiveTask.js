const async = require('async');
const { errors } = require('arsenal');

const ObjectMDArchive = require('arsenal').models.ObjectMDArchive;
const LifecycleUpdateTransitionTask = require('./LifecycleUpdateTransitionTask');

class LifecycleColdStatusArchiveTask extends LifecycleUpdateTransitionTask {
    getTargetAttribute(entry) {
        const {
            bucketName: bucket,
            objectKey: key,
            objectVersion: version,
            accountId
        } = entry.target;
        return { bucket, key, version, accountId };
    }

    /**
     * delete data in set in ObjectMD location
     * @param {ColdStorageStatusQeueuEntry} entry - cold storage status entry
     * used for garbage collecting archived location info
     * @param {ObjectMD} objMD - object metadata
     * @param {object} log - logger
     * @param {function} done - callback
     * @return {undefined}
     */
    _executeDeleteData(entry, objMD, log, done) {
        log.debug('action execution starts', entry.getLogInfo());
        const { bucketName, objectKey, accountId } = entry.target;
        const backbeatClient = this.getBackbeatClient(accountId);

        if (!backbeatClient) {
            log.error('failed to get backbeat client', { accountId });
            return done(errors.InternalError
                .customizeDescription('Unable to obtain client'));
        }

        const locations = objMD.getLocation();
        const req = backbeatClient.batchDelete({
            Locations: locations.map(location => ({
                key: location.key,
                dataStoreName: location.dataStoreName,
                size: location.size,
                dataStoreVersionId: location.dataStoreVersionId,
            })),
            Bucket: bucketName,
            Key: objectKey,
            StorageClass: objMD.getDataStoreName(),
            Tags: JSON.stringify({
                'scal-delete-marker': 'true',
                'scal-delete-service': 'lifecycle-transition',
            }),
        });
        return req.send(err => {
            log.info('action execution ended', entry.getLogInfo());

            if (err && err.statusCode === 404) {
                log.info('unable to find data to delete',
                    Object.assign({
                        method: 'LifecycleColdStatusArchiveTask._executeDeleteData',
                        bucket: bucketName,
                        key: objectKey,
                    }, entry.getLogInfo));
                return done();
            }

            if (err) {
                log.error('an error occurred on deleteData method to backbeat route',
                    Object.assign({
                        method: 'LifecycleColdStatusArchiveTask._executeDeleteData',
                        error: err.message,
                        httpStatus: err.statusCode,
                    }, entry.getLogInfo()));
                return done(err);
            }
            return done();
        });
    }

    processEntry(entry, done) {
        const log = this.logger.newRequestLogger();
        let objectMD;
        return async.series([
            next => this._getMetadata(entry, log, (err, res) => {
                if (err) {
                    return next(err);
                }

                objectMD = res;
                return next();
            }),
            next => {
                // set new ObjectMDArchive to ObjectMD
                objectMD.setArchive(new ObjectMDArchive(entry.archiveInfo));
                this._putMetadata(entry, objectMD, log, next);
            },
            next => {
                const locationToGC = objectMD.getLocation();
                if (!locationToGC) {
                    return process.nextTick(next);
                }

                return this._executeDeleteData(entry, objectMD, this._log, err => {
                    if (err) {
                        return next(err);
                    }

                    log.debug('successfully deleted location data', {
                        bucketName: entry.target.bucketName,
                        objectKey: entry.target.objectKey,
                        objectVersion: entry.target.objectVersion,
                    });
                    // set location to null
                    objectMD.setLocation();
                    // TODO: set different data store name?
                    return this._putMetadata(entry, objectMD, log, next);
                });
            },
        ], done);
    }
}

module.exports = LifecycleColdStatusArchiveTask;
