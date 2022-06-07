const AllocationStrategy = require('./AllocationStrategy');

/**
 * @class LeastFullConnector
 *
 * @classdesc LeastFullConnector is an allocation
 * strategy that assigns buckets to connectors based
 * on the number of buckets assigned to each connector.
 * Connectors with the fewest buckets are filled first
 */
class LeastFullConnector extends AllocationStrategy {

    /**
     * @constructor
     * @param {Object} params params
     * @param {Logger} params.logger logger object
     */
    constructor(params) {
        super(params.logger);
    }

    /**
     * Get best connector for assigning a bucket
     * @param {Connector[]} connectors available connectors
     * @returns {Connector} connector
     */
    getConnector(connectors) {
        return connectors.reduce((prev, elt) => (elt.bucketCount < prev.bucketCount ? elt : prev));
    }
}

module.exports = LeastFullConnector;
