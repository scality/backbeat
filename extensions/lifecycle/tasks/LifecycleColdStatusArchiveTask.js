const async = require('async');

const ObjectMDArchive = require('arsenal').models.ObjectMDArchive;
const ActionQueueEntry = require('../../../lib/models/ActionQueueEntry');
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

    _garbageCollectArchivedSource(entry, oldLocation, newLocation, log) {
        const { bucket, key, version, accountId, owner } = this.getTargetAttribute(entry);
        const gcEntry = ActionQueueEntry.create('deleteArchivedSourceData')
              .addContext({
                  origin: 'lifecycle',
                  ruleType: 'archive',
                  reqId: log.getSerializedUids(),
                  bucketName: bucket,
                  objectKey: key,
                  versionId: version,
              })
              .setAttribute('serviceName', 'lifecycle-transition')
              .setAttribute('target.oldLocation', oldLocation)
              .setAttribute('target.newLocation', newLocation)
              .setAttribute('target.bucket', bucket)
              .setAttribute('target.key', key)
              .setAttribute('target.version', version)
              .setAttribute('target.accountId', accountId)
              .setAttribute('target.owner', owner);
        this.gcProducer.publishActionEntry(gcEntry);
    }

    processEntry(coldLocation, entry, done) {
        const log = this.logger.newRequestLogger();
        let objectMD;
        let oldLocation;
        let skipLocationDeletion = false;

        return async.series([
            next => this._getMetadata(entry, log, (err, res) => {
                if (err) {
                    return next(err);
                }

                const locations = res.getLocation();
                objectMD = res;
                skipLocationDeletion = !locations ||
                    (Array.isArray(locations) && locations.length === 0);
                oldLocation = objectMD.getDataStoreName();

                return next();
            }),
            next => {
                // set new ObjectMDArchive to ObjectMD
                objectMD.setArchive(new ObjectMDArchive(entry.archiveInfo));

                if (skipLocationDeletion) {
                    objectMD.setDataStoreName(coldLocation)
                        .setAmzStorageClass(coldLocation)
                        .setTransitionInProgress(false);
                }

                this._putMetadata(entry, objectMD, log, next);
            },
            next => {
                if (!skipLocationDeletion) {
                    this._garbageCollectArchivedSource(entry, oldLocation, coldLocation, log);
                }

                return process.nextTick(next);
            },
        ], err => {
            if (err) {
                // if error occurs, do not commit offset
                return done(err, { committable: false });
            }
            return done();
        });
    }
}

module.exports = LifecycleColdStatusArchiveTask;