const AllocationStrategy = require('./AllocationStrategy');

/**
 * @class RetainBucketsDecorator
 *
 * @classdesc RetainBucketsDecorator is a decorator
 * that retains buckets that are removed from the
 * connector but still in use.
 */
class RetainBucketsDecorator extends AllocationStrategy {
    /**
     * @constructor
     * @param {AllocationStrategy} strategy the strategy to decorate
     * @param {Object} params params
     * @param {Logger} params.logger logger object
     */
    constructor(strategy, params) {
        super(params);
        this._strategy = strategy;

        // Stores buckets that should are removed from the connector
        // but still in use
        this._retainedBuckets = new Map();
    }

    /**
     * Get the number of retained buckets
     * @returns {Number} number of retained buckets
     */
    get retainedBucketsNb() { return this._retainedBuckets.size; }

    /**
     * Callback when a bucket is removed from a connector
     * @param {String} bucket bucket name
     * @param {Connector} connector connector
     * @returns {undefined}
     */
    onBucketRemoved(bucket, connector) {
        // When a bucket is removed, it is not immediately removed
        // from the connector. This allows to retain the bucket
        // in the connector until it is removed.
        this._retainedBuckets.set(bucket, connector);
    }

    /**
     * Callback when a connector is destroyed.
     * @param {Connector} connector connector
     * @returns {undefined}
     */
    onConnectorDestroyed(connector) {
        this._cleanupRetainedBucket(connector);
    }

    /**
     * Cleanup retained buckets for a connector
     * @param {Connector} connector connector
     * @returns {undefined}
     */
    _cleanupRetainedBucket(connector) {
        // When a connector is updated or destroyed, the retained
        // buckets are removed from the connector
        this._retainedBuckets.forEach((conn, bucket) => {
            if (conn === connector) {
                this._retainedBuckets.delete(bucket);
            }
        });
    }

    /**
     * Get best connector to assign a bucket to.
     * If no connector is available, null is returned.
     * If the bucket is retained, the associated connector
     * is returned.
     * @param {Array<Connector>} connectors connectors
     * @param {String} bucket bucket name
     * @returns {Connector | null} connector
     */
    getConnector(connectors, bucket) {
        if (this._retainedBuckets.has(bucket)) {
            return this._retainedBuckets.get(bucket);
        }

        return this._strategy.getConnector(connectors, bucket);
    }

    /**
     * Assess if a pipeline can be updated. If the connector can
     * be updated, the bucket is added as retained.
     * @param {Connector} connector connector
     * @returns {Boolean} true if the connector can be updated
     */
    canUpdate(connector) {
        const res = this._strategy.canUpdate();
        if (res) {
            this._cleanupRetainedBucket(connector);
        }
        return res;
    }
}

module.exports = RetainBucketsDecorator;
