const { constants } = require('arsenal');
const { wildCardForAllBuckets } = require('../constants');
const PipelineFactory = require('./PipelineFactory');

/**
 * @class WildcardPipelineFactory
 *
 * @classdesc Generates a static mongodb connector pipeline,
 * that listens to all buckets.
 */
class WildcardPipelineFactory extends PipelineFactory {
    /**
     * @constructor
     * @param {number} locationStrippingBytesThreshold threshold for stripping location data
     */
    constructor(locationStrippingBytesThreshold) {
        super(locationStrippingBytesThreshold);
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
        return bucketList.includes(wildCardForAllBuckets);
    }

    /**
     * Makes connector pipeline stage, to listen to all non-special collections.
     * @param {string[] | undefined} buckets buckets assigned to this connector
     * @returns {object} connector pipeline stage
     */
    getPipelineStage(buckets) { // eslint-disable-line no-unused-vars
        return {
            $match: {
                'ns.coll': {
                    $not: {
                        $regex: `^(${constants.mpuBucketPrefix}|__).*`,
                    },
                }
            }
        };
    }
}

module.exports = WildcardPipelineFactory;
