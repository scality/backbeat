'use strict'; // eslint-disable-line

const { LifecycleRequeueTask } = require('./LifecycleRequeueTask');

class LifecycleResetTransitionInProgressTask extends LifecycleRequeueTask {
    /**
     * Process a lifecycle object entry
     *
     * @constructor
     * @param {LifecycleObjectProcessor} proc - object processor instance
     */
     constructor(proc) {
        super(proc);
        this.processName = 'transition';
    }

    updateObjectMD(md, try_, log, etag) {
        if (this.shouldSkipObject(md, etag, log)) {
            return false;
        }
        md.setTransitionInProgress(false);
        md.setUserMetadata({
                'x-amz-meta-scal-s3-transition-attempt': try_,
        });
        return true;
    }

    shouldSkipObject(md, expectedEtag, log) {
        try {
            const etag = JSON.parse(expectedEtag);
            if (etag !== md.getContentMd5()) {
                log.debug('different etag, skipping object', {
                    currentETag: md.getContentMd5(),
                    requeueEtag: etag,
                });
                return true;
            }
        } catch (error) {
            log.error('unparseable etag, skipping object', { errorMessage: error.message });
            return true;
        }

        if (!md.getTransitionInProgress()) {
            log.debug('not transitioning, skipping object');
            return true;
        }

        return false;
    }
}

module.exports = {
    LifecycleResetTransitionInProgressTask
};
