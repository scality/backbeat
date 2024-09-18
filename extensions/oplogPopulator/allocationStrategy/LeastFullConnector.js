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
     * @param {Number} params.maximumBucketsPerConnector maximum number of buckets per connector
     * @param {Logger} params.logger logger object
     */
    constructor(params) {
        super(params);
        this._maximumBucketsPerConnector = params.maximumBucketsPerConnector;
    }

    /**
     * Get best connector to assign a bucket to.
     * If no connector is available, null is returned.
     * @param {Array<Connector>} connectors connectors
     * @param {String} bucket bucket name
     * @returns {Connector | null} connector
     */
    getConnector(connectors, bucket) { // eslint-disable-line no-unused-vars
        if (!connectors.length) {
            return null;
        }
        const connector = connectors.reduce((prev, elt) => (elt.bucketCount < prev.bucketCount ? elt : prev));
        if (connector.buckets.length >= this._maximumBucketsPerConnector) {
            return null;
        }
        return connector;
    }

    /**
     * Assess if a pipeline can be updated.
     * @param {Connector} connector connector
     * @returns {true} true
     */
    canUpdate(connector) { // eslint-disable-line no-unused-vars
        return true;
    }
}

module.exports = LeastFullConnector;
