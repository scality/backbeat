const { wildCardForAllBuckets } = require('../constants');
const PipelineFactory = require('./PipelineFactory');

/**
 * @class MultipleBucketsPipelineFactory
 *
 * @classdesc Generates a mongodb connector pipeline
 * given a list of buckets.
 */
class MultipleBucketsPipelineFactory extends PipelineFactory {
    /**
     * @constructor
     * @param {number} locationStrippingThreshold threshold for stripping location data
     */
    constructor(locationStrippingThreshold = 100) {
        super(locationStrippingThreshold);
        // getPipeline is used standalone later, make sure its this binds to us.
        this.getPipeline = this.getPipeline.bind(this);
    }

    /**
     * Checks if an existing pipeline is valid against the current
     * factory.
     * @param {string[]} bucketList pipeline
     * @returns {boolean} true if the pipeline is valid
     */
    isValid(bucketList) {
        if (!bucketList?.length) {
            return false;
        }
        return !bucketList.includes(wildCardForAllBuckets);
    }

    /**
     * Makes new connector pipeline that includes
     * buckets assigned to this connector.
     * @param {string[] | undefined} buckets buckets assigned to this connector
     * @returns {string} new connector pipeline
     */
    getPipeline(buckets) {
        if (!buckets || !buckets.length) {
            return JSON.stringify([]);
        }
        return JSON.stringify([
            {
                $match: {
                    'ns.coll': {
                        $in: buckets,
                    }
                }
            },
            this._getLocationStrippingStage(),
        ]);
    }
}

module.exports = MultipleBucketsPipelineFactory;
