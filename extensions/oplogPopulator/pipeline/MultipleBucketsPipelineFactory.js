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
    constructor(locationStrippingThreshold) {
        super(locationStrippingThreshold);
        // getPipeline is used standalone later, make sure its `this` reference binds to us.
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
     * Makes connector pipeline stage, that includes buckets assigned to this connector.
     * @param {string[] | undefined} buckets buckets assigned to this connector
     * @returns {object} connector pipeline stage
     */
    getPipelineStage(buckets) {
        if (!buckets || !buckets.length) {
            return null;
        }
        return {
            $match: {
                'ns.coll': {
                    $in: buckets,
                }
            }
        };
    }
}

module.exports = MultipleBucketsPipelineFactory;
