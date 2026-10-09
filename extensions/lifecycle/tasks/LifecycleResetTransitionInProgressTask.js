'use strict';

const { LifecycleRequeueTask } = require('./LifecycleRequeueTask');
const { TRANSITION_ATTEMPT_MD } = require('../../../lib/util/transitionAttempt');
const { garbageCollectArchivedSource } = require('../util/garbageCollectArchivedSource');
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

    updateObjectMD(md, try_, log, etag, target) {
        if (this.shouldSkipObject(md, etag, log)) {
            return false;
        }

        if (md.getArchive()?.archiveInfo) {
            // Only the completion is missing: nothing to archive again
            return this._completeArchivedTransition(md, target, log);
        }

        md.setOriginOp('s3:LifecycleTransition:Retry');
        if (!this._isDirectToCold(md)) {
            // Keep the flag as the queue populator keys on it to trigger the next attempt
            md.setTransitionInProgress(false);
        }
        md.setUserMetadata({ [TRANSITION_ATTEMPT_MD]: try_ });
        return true;
    }

    /**
     * Complete the transition of an archived object, like the cold status processor would have:
     * directly when there is no data to release, through the garbage collector otherwise.
     *
     * @param {ObjectMD} md - object metadata
     * @param {Object} target - requeued object, see LifecycleRequeueTask.requeueObjectVersion
     * @param {Logger} log - request logger
     * @return {boolean} true if the metadata must be written back
     */
    _completeArchivedTransition(md, target, log) {
        const { location } = target;
        const logFields = {
            method: 'LifecycleResetTransitionInProgressTask._completeArchivedTransition',
            location,
            dataStoreName: md.getDataStoreName(),
        };

        if (!location || !locationsConfig[location]?.isCold) {
            log.error('object already archived but no cold location given, skipping', logFields);
            return false;
        }

        if (!md.getLocation()?.length) {
            log.info('object already archived with no data to release, completing the transition',
                logFields);
            const isDirectToCold = md.getAmzStorageClass() === location;
            md.setDataStoreName(location)
                .setAmzStorageClass(location)
                .setTransitionInProgress(false)
                .setOriginOp(isDirectToCold ? 's3:LifecycleTransition:Direct' : 's3:LifecycleTransition')
                .setUserMetadata({ [TRANSITION_ATTEMPT_MD]: undefined });
            return true;
        }

        // The garbage collector updates the metadata itself once the data is released, which
        // re-drives any restore deferred until then.
        log.info('object already archived, releasing hot data', logFields);
        garbageCollectArchivedSource(this.gcProducer, { ...target, owner: md.getOwnerId() },
            md.getDataStoreName(), location, log);
        return false;
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
