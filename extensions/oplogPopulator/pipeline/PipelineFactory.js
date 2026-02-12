const { errors } = require('arsenal');
const { wildCardForAllBuckets } = require('../constants');

/**
 * @class PipelineFactory
 *
 * @classdesc base class for the pipeline factories
 */
class PipelineFactory {
    /**
     * @constructor
     * @param {number} locationStrippingThreshold threshold for stripping location data
     */
    constructor(locationStrippingThreshold) {
        this._locationStrippingThreshold = locationStrippingThreshold;
    }

    /**
     * Checks if an existing pipeline is valid against the current
     * factory.
     * @param {string} pipeline pipeline
     * @returns {boolean} true if the pipeline is valid
     */
    isValid(pipeline) { // eslint-disable-line no-unused-vars
        throw errors.NotImplemented;
    }

    /**
     * Extracts the list of buckets from an existing pipeline
     * @param {Object} connectorConfig connector configuration
     * @returns {string[]} list of buckets
     */
    extractBucketsFromConfig(connectorConfig) {
        const pipeline = connectorConfig.pipeline ?
            JSON.parse(connectorConfig.pipeline) : null;
        if (!pipeline || pipeline.length === 0) {
            return [];
        }
        if (pipeline[0].$match['ns.coll']?.$not) {
            return [wildCardForAllBuckets];
        }
        return pipeline[0].$match['ns.coll'].$in;
    }

    /**
     * Process an old connector configuration, and return
     * the list of buckets if the bucket list is valid against
     * the current pipeline factory.
     * @param {Object} oldConfig old configuration
     * @returns {string[] | null} old configuration if valid
     */
    getOldConnectorBucketList(oldConfig) {
        const bucketList = this.extractBucketsFromConfig(oldConfig);
        if (this.isValid(bucketList)) {
            return bucketList;
        }
        return null;
    }

    /**
     * Makes new connector pipeline that includes
     * buckets assigned to this connector.
     * @param {string[] | undefined} buckets buckets assigned to this connector
     * @returns {string} new connector pipeline
     */
    getPipeline(buckets) {
        const stage = this.getPipelineStage(buckets);
        if (!stage) {
            return JSON.stringify([]);
        }
        const pipeline = [
            stage,
        ];
        if (this._locationStrippingThreshold >= 0) {
            pipeline.push({
                $set: {
                    'fullDocument.value.location':
                        this._locationStrippingExpression('fullDocument.value.location'),
                    'updateDescription.updatedFields.value.location':
                        this._locationStrippingExpression('updateDescription.updatedFields.value.location'),
                }
            });
        }
        return JSON.stringify(pipeline);
    }

    _locationStrippingExpression(field) {
        return {
            $switch: {
                branches: [
                    { case: { $not: [{ $isArray: `$${field}` }] }, then: '$$REMOVE' },
                    { case: { $gte: [{ $size: `$${field}` }, this._locationStrippingThreshold] }, then: '$$REMOVE' },
                ],
                default: `$${field}`,
            }
        };
    }

    /**
     * Makes connector pipeline stage, to then be used by getPipeline.
     * @param {string[] | undefined} buckets buckets assigned to this connector
     * @returns {object} connector pipeline stage
     */
    getPipelineStage(buckets) { // eslint-disable-line no-unused-vars
        throw errors.NotImplemented;
    }
}

module.exports = PipelineFactory;
