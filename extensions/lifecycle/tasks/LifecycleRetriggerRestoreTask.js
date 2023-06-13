'use strict'; // eslint-disable-line

const { LifecycleRequeueTask } = require('./LifecycleRequeueTask');

class LifecycleRetriggerRestoreTask extends LifecycleRequeueTask {
    /**
     * Process a lifecycle object entry
     *
     * @constructor
     * @param {LifecycleObjectProcessor} proc - object processor instance
     */
     constructor(proc) {
        super(proc, 'restore');
    }

    updateObjectMD(md, try_, log) {
        if (this.shouldSkipObject(md, log)) {
            return false;
        }
        md.setUserMetadata({
            'x-amz-meta-scal-s3-restore-attempt': try_,
        });
        md.setOriginOp('s3:ObjectRestore:Retry');
        return true;
    }

    shouldSkipObject(md, log) {
        const isObjectAlreadyRestored = md.getArchive()
            && md.getArchive().restoreCompletedAt;

        if (!md.getArchive()?.archiveInfo?.archiveId) {
            log.error('object is not archived, skipping');
            return true;
        }

        if (isObjectAlreadyRestored) {
                log.error('object is already restored, skipping');
                return true;
        }

        if (new Date(md.getArchive().restoreWillExpireAt) < new Date()) {
            log.error('object restore has expired, skipping');
            return true;
        }

        if (!md.getArchive()?.restoreRequestedAt || !md.getArchive()?.restoreRequestedDays) {
            log.error('object restore was not requested, skipping');
            return true;
        }

        return false;
    }
}

module.exports = {
    LifecycleRetriggerRestoreTask
};
