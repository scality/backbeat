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
        super(proc);
        this.processName = 'retriggerRestore';
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
            && md.getArchive().restoreCompletedAt
            && new Date(md.getArchive().restoreWillExpireAt) >= new Date();

        if (!md.getArchive()?.restoreRequestedAt ||
            !md.getArchive()?.restoreRequestedDays || isObjectAlreadyRestored) {
                log.error('object is already restored, skipping');
                return true;
        }

        if (!md.getArchive()?.archiveInfo?.archiveId) {
            log.error('object is not archived, skipping');
            return true;
        }

        return false;
    }
}

module.exports = {
    LifecycleRetriggerRestoreTask
};
