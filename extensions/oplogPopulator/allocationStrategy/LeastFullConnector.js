const AllocationStrategy = require('./AllocationStrategy');

/**
 * @class LeastFullConnector
 *
 * @classdesc LeastFullConnector is an allocation
 * strategy that assigns buckets to connectors based
 * on the number of buckets assigned to each connector.
 * Connectors with the fewest buckets are filled first.
 * If a connector reached the maximum number of buckets,
 * a new connector is created.
 */
class LeastFullConnector extends AllocationStrategy {
    /**
     * Get best connector for assigning a bucket
     * @param {Connector[]} connectors available connectors
     * @returns {Connector} connector
     */
    getConnector(connectors) {
        const connector = connectors.reduce((prev, elt) => (elt.bucketCount < prev.bucketCount ? elt : prev));
        if (connector.buckets.length >= this._maximumBucketsPerConnector) {
            return this._addConnector();
        }
        return connector;
    }
}

module.exports = LeastFullConnector;
