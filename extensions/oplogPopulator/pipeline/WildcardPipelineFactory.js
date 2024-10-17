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
     * Makes new connector pipeline that includes
     * buckets assigned to this connector. If the
     * allocation strategy is not set, we listen to
     * all non-special collections.
     * @param {string[] | undefined} buckets buckets assigned to this connector
     * @returns {string} new connector pipeline
     */
    getPipeline(buckets) { // eslint-disable-line no-unused-vars
        return JSON.stringify([
            {
                $match: {
                    'ns.coll': {
                        $not: {
                            $regex: `^(${constants.mpuBucketPrefix}|__).*`,
                        },
                    }
                }
            }
        ]);
    }
}

module.exports = WildcardPipelineFactory;
