const constants = require('../constants');
const AllocationStrategy = require('./AllocationStrategy');

/**
 * @class UniqueConnector
 *
 * @classdesc UniqueConnector is an allocation
 * strategy where each bucket is assigned to a unique
 * connector.
 */
class UniqueConnector extends AllocationStrategy {

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
        // if the bucket is already listed in a connector, return that connector
        // otherwise, return the connector with a wildcard
        const connector = connectors.find(conn => conn.buckets.includes(bucket));
        if (connector) {
            return connector;
        }

        return connectors.find(conn => conn.buckets.includes(constants.wildCardForAllBuckets)) || null;
    }

    /**
     * Assess if a pipeline can be updated.
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

module.exports = UniqueConnector;

const AllocationStrategy = require('./AllocationStrategy');

/**
 * @class UniqueConnector
 *
 * @classdesc UniqueConnector is an allocation
 * strategy where each bucket is assigned to a unique
 * connector.
 */
class UniqueConnector extends AllocationStrategy {

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
        // if the bucket is already listed in a connector, return that connector
        // otherwise, return the connector with a wildcard
        const connector = connectors.find(conn => conn.buckets.includes(bucket));
        if (connector) {
            return connector;
        }

        return connectors.find(conn => conn.buckets.includes(constants.wildCardForAllBuckets)) || null;
    }

    /**
     * Assess if a pipeline can be updated.
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

module.exports = UniqueConnector;
