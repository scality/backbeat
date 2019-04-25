
// TODO: based on location constraint changes

// TODO:
const REFRESH_TIMER = 60000;

class ProcessorMemState {
    constructor(config) {
        this._config = config;
        /*
            {
                bucket: BucketInfo,
            }
        */
        this._memo = {};

        this._config.on('bootstrap-list-update', this._cleanup.bind(this));
    }

    _cleanup() {
        // array of { site, type }
        const bootstrapList = this._config.getBootstrapList();
        const sites = bootstrapList.map(b => b.site);

        // all locations in memo should have an associated bootstrapList site
        Object.keys(this._memo).forEach(bucket => {
            if (sites.indexOf(bucket.getLocationConstraint()) === -1) {
                delete this._memo[bucket];
            }
        });
    }

    memoize(bucket, bucketInfo) {
        this._memo[bucket] = bucketInfo;
        // add expiry
        setTimeout(() => {
            delete this._memo[bucket];
        }, REFRESH_TIMER);
    }

    getBucketInfo(bucket) {
        return this._memo[bucket];
    }

    getLocation(bucket) {
        return this._memo[bucket] &&
               this._memo[bucket].getLocationConstraint();
    }

    getReplicationConfiguration(bucket) {
        return this._memo[bucket] &&
               this._memo[bucket].getReplicationConfiguration();
    }
}

module.exports = ProcessorMemState;
