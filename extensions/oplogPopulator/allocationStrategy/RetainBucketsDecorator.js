const constants = require('../constants');
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

        // Stores buckets that should be removed from the connector
        // but still in use
        this._retainedBuckets = new Map();
    }

    bindConnectorEvents(connectorsManager, metricsHandler) {
        // Bind events from the connector manager to the strategy
        connectorsManager.on(constants.connectorUpdatedEvent, connector =>
            this._strategy.onConnectorUpdatedOrDestroyed(connector));

        this._allocator.on(constants.bucketRemovedFromConnectorEvent, (bucket, connector) =>
            this._strategy.onBucketRemoved(bucket, connector));

        connectorsManager.on(constants.connectorsReconciledEvent, bucketsExceedingLimit => {
            metricsHandler.onConnectorsReconciled(
                bucketsExceedingLimit,
                this._strategy.retainedBucketsCount,
            );
        });
    }

    /**
     * Get the number of retained buckets
     * @returns {Number} number of retained buckets
     */
    get retainedBucketsCount() { return this._retainedBuckets.size; }

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
     * Callback when a connector is destroyed or
     * updated.
     * @param {Connector} connector connector
     * @returns {undefined}
     */
    onConnectorUpdatedOrDestroyed(connector) {
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
        return this._retainedBuckets.get(bucket) ||
            this._strategy.getConnector(connectors, bucket);
    }

    /**
     * Assess if a pipeline can be updated.
     * @returns {Boolean} true if the connector can be updated
     */
    canUpdate() {
        return this._strategy.canUpdate();
    }

    /**
     * Getter for the maximum number of buckets per connector
     * @returns {Number} maximum number of buckets per connector
     */
    get maximumBucketsPerConnector() {
        return this._strategy.maximumBucketsPerConnector;
    }
}

module.exports = RetainBucketsDecorator;
