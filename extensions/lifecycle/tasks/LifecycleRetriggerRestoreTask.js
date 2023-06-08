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
        if (md.getArchive()?.archiveInfo?.archiveId === undefined) {
            log.error('object is not archived, skipping');
            return true;
        }

        return false;
    }
}

module.exports = {
    LifecycleRetriggerRestoreTask
};
