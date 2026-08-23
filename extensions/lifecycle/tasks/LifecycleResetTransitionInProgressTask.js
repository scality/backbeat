'use strict';

const { LifecycleRequeueTask } = require('./LifecycleRequeueTask');
const locationsConfig = require('../../../conf/locationConfig.json') || {};

const isColdLocation = locationName => !!(locationName && locationsConfig[locationName]
    && locationsConfig[locationName].isCold);

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
        // For a direct transition, the "transition in progress" flag is what the queue populator
        // keys on to retry the archival: clearing it would both hide this update from the
        // populator and, since shouldSkipObject() bails when the flag is unset, prevent any
        // further requeue. There is no lifecycle scan to re-pick these objects, so the flag must
        // stay set until the transition actually completes.
        if (!this._isDirectTransition(md)) {
            md.setTransitionInProgress(false);
        }
        md.setUserMetadata({
            'x-amz-meta-scal-s3-transition-attempt': try_,
        });
        return true;
    }

    /**
     * Whether the object transition was requested directly in the PUT request (as opposed to
     * being triggered by a lifecycle rule): in that case the requested cold storage class is
     * declared in the object metadata, while the data still lies in a hot location.
     *
     * @param {ObjectMD} md - object metadata
     * @return {boolean} true if this is a pending direct transition
     */
    _isDirectTransition(md) {
        return isColdLocation(md.getAmzStorageClass()) && !isColdLocation(md.getDataStoreName());
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
