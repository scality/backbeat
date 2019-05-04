'use strict'; // eslint-disable-line

// get Bucket Info from Mongo for given location every 1 minute per consumer
const REFRESH_TIMER = 60000;

/**
 * @class BucketMemState
 *
 * @classdesc Memoize bucket metadata state in order to reduce I/O operations
 * when ingestion consumers are backlogged with entries.
 */
class BucketMemState {
    constructor(config) {
        this._config = config;

        // i.e.: { bucketName: BucketInfo() }
        this._memo = {};

        this._config.on('bootstrap-list-update', this._cleanup.bind(this));
    }

    /**
     * if the user somehow removes the ingestion location while kafka entries
     * for the given location are still queued to be processed, we should
     * remove any in-mem state for the location. This will lead to an error
     * propagated by `MongoQueueProcessor.processKafkaEntry` where mongoClient
     * makes a call to `getBucketAttributes`
     * @return {undefined}
     */
    _cleanup() {
        const bootstrapList = this._config.getBootstrapList();
        const sites = bootstrapList.map(b => b.site);

        // all locations in memo should have an associated bootstrapList site
        Object.keys(this._memo).forEach(bucket => {
            if (sites.indexOf(bucket.getLocationConstraint()) === -1) {
                delete this._memo[bucket];
            }
        });
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
