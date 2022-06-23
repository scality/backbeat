const async = require('async');

const ObjectMDArchive = require('arsenal').models.ObjectMDArchive;
const LifecycleUpdateTransitionTask = require('./LifecycleUpdateTransitionTask');

class LifecycleColdStatusArchiveTask extends LifecycleUpdateTransitionTask {
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
        const {
            bucketName,
            objectKey,
            objectVersion,
            accountId
        } = entry.getTarget();
        return {
            bucket: bucketName,
            key: objectKey,
            version: objectVersion,
            accountId
        };
    }

    getSourceAttribute(entry) {
        const { bucketName, objectKey, objectVersion } = entry.getTarget();
        return {
            bucket: bucketName,
            objectKey,
            version: objectVersion,
            storageClass: entry.get('sourceStorageClass'),
            lastModified: entry.get('sourceLastModified'),
        };
    }

    _updateMdWithArchiveInfo(entry, objMD) {
        const archiveInfo = entry.getArchiveInfo();
        objMD.setArchive(new ObjectMDArchive(archiveInfo));
        // set location to empty
        objMD.setLocation();
    }

    processEntry(entry, done) {
        const log = this.logger.newRequestLogger();
        let locationToGC;
        return async.waterfall([
            next => this._getMetadata(entry, log, next),
            (objMD, next) => {
                locationToGC = objMD.getLocation();
                entry.set('sourceStorageClass', objMD.getDataStoreName());
                entry.set('sourceLastModified', objMD.getLastModified());
                this._updateMdWithArchiveInfo(entry, objMD);
                return this._putMetadata(entry, objMD, log, next);
            },
            next => {
                if (!locationToGC) {
                    return next();
                }
                return this._garbageCollectLocation(
                    entry, locationToGC, log, next);
            },
        ], done);
    }
}

module.exports = LifecycleColdStatusArchiveTask;
