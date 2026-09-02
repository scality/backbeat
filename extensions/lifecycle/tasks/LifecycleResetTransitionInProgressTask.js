'use strict';

const { LifecycleRequeueTask } = require('./LifecycleRequeueTask');
const locationsConfig = require('../../../conf/locationConfig.json') || {};

class LifecycleResetTransitionInProgressTask extends LifecycleRequeueTask {
    /**
     * Process a lifecycle object entry
     *
     * @constructor
     * @param {LifecycleObjectProcessor} proc - object processor instance
     */
     constructor(proc) {
        super(proc, 'transition');
    }

    updateObjectMD(md, try_, log, etag) {
        if (this.shouldSkipObject(md, etag, log)) {
            return false;
        }
        md.setOriginOp('s3:LifecycleTransition:Retry');
        if (!this._isDirectToCold(md)) {
            // Keep the flag as the queue populator keys on it to trigger the next attempt
            md.setTransitionInProgress(false);
        }
        md.setUserMetadata({
            'x-amz-meta-scal-s3-transition-attempt': try_,
        });
        return true;
    }

    /**
     * Check if object transition was initiated by direct-to-cold request instead of lifecycle rule.
     *
     * @param {ObjectMD} md - object metadata
     * @return {boolean} true if this is a pending direct transition
     */
    _isDirectToCold(md) {
        return locationsConfig[md.getAmzStorageClass()]?.isCold
            && !locationsConfig[md.getDataStoreName()]?.isCold;
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
