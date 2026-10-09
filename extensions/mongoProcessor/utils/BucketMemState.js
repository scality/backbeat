'use strict';

// get Bucket Info from Mongo for given location every 1 minute per consumer
const REFRESH_TIMER = 60000;

/**
 * @class BucketMemState
 *
 * @classdesc Memoize bucket metadata state in order to reduce I/O operations
 * when ingestion consumers are backlogged with entries.
 */
class BucketMemState {
    constructor() {
        // i.e.: { bucketName: BucketInfo() }
        this._memo = {};
    }

    /**
     * save bucket info in memory up to REFRESH_TIMER
     * @param {String} bucketName - bucket name
     * @param {BucketInfo} bucketInfo - instance of BucketInfo
     * @return {undefined}
     */
    memoize(bucketName, bucketInfo) {
        this._memo[bucketName] = bucketInfo;
        // add expiry
        setTimeout(() => {
            delete this._memo[bucketName];
        }, REFRESH_TIMER);
    }

    getBucketInfo(bucketName) {
        return this._memo[bucketName];
    }
}

module.exports = BucketMemState;
