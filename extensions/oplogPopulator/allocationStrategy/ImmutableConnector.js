const AllocationStrategy = require('./AllocationStrategy');

/**
 * @class ImmutableConnector
 *
 * @classdesc ImmutableConnector is an allocation
 * strategy that always requires a new connector to be
 * created for each bucket.
 */
class ImmutableConnector extends AllocationStrategy {
    /**
     * Get best connector to assign a bucket to.
     * If no connector is available, null is returned.
     * @param {Array<Connector>} connectors connectors
     * @param {String} bucket bucket name
     * @returns {Connector | null} connector
     */
    getConnector(connectors, bucket) { // eslint-disable-line no-unused-vars
        return null;
    }

    /**
     * Assess if a pipeline can be updated. With the immutable
     * strategy, a connector cannot be updated.
     * @returns {false} false
     */
    canUpdate() {
        return false;
    }

    /**
     * Getter for the maximum number of buckets per connector
     * @returns {Number} maximum number of buckets per connector
     */
    get maximumBucketsPerConnector() {
        return 1;
    }
}

module.exports = ImmutableConnector;
