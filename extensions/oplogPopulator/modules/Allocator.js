const joi = require('joi');
const { errors } = require('arsenal');

const OplogPopulatorMetrics = require('../OplogPopulatorMetrics');
const AllocationStrategy = require('../allocationStrategy/AllocationStrategy');
const { EventEmitter } = require('./ConnectorsManager');

const paramsJoi = joi.object({
    connectorsManager: joi.object().required(),
    metricsHandler: joi.object()
        .instance(OplogPopulatorMetrics).required(),
    allocationStrategy: joi.object()
        .instance(AllocationStrategy).required(),
    logger: joi.object().required(),
}).required();

/**
 * @class Allocator
 *
 * @classdesc Allocator handles listening to buckets by assigning
 * a connector to them.
 */
class Allocator extends EventEmitter {

    /**
     * @constructor
     * @param {Object} params Allocator param
     * @param {ConnectorsManager} params.connectorsManager connectorsManager
     * @param {OplogPopulatorMetrics} params.metricsHandler metrics handler
     * @param {AllocationStrategy} params.allocationStrategy allocation strategy
     * @param {Logger} params.logger logger object
     */
    constructor(params) {
        super();
        joi.attempt(params, paramsJoi);
        this._connectorsManager = params.connectorsManager;
        this._logger = params.logger;
        this._allocationStrategy = params.allocationStrategy;
        this._metricsHandler = params.metricsHandler;
        // Stores connector assigned for each bucket
        this._bucketsToConnectors = new Map();
        this._initConnectorToBucketMap();
    }

    /**
     * Initializes connectorsManager map
     * @returns {undefined}
     */
    _initConnectorToBucketMap() {
        const connectors = this._connectorsManager.connectors;
        connectors.forEach(connector => {
            connector.buckets
                .forEach(bucket => {
                    this._bucketsToConnectors.set(bucket, connector);
                    this._metricsHandler.onConnectorConfigured(connector, 'add');
                });
        });
    }

    /**
     * Is bucket already added to a connector
     * @param {string} bucket bucket name
     * @returns {boolean} true if a connector is
     * listening to the bucket
     */
    has(bucket) {
        return this._bucketsToConnectors.has(bucket);
    }

    /**
     * Starts listening to bucket by
     * adding and assigning a connector to it
     * @param {string} bucket bucket name
     * @param {Date|null} eventDate oplog event date
     * @returns {Promise|undefined} undefined
     * @throws {InternalError}
     */
    async listenToBucket(bucket, eventDate = null) {
        try {
            if (!this.has(bucket)) {
                const connectors = this._connectorsManager.connectors;
                let connector = this._allocationStrategy.getConnector(connectors, bucket);
                if (!connector) {
                    // the connector is not available (too many buckets)
                    // we need to create a new connector
                    connector = this._connectorsManager.addConnector();
                }
                // In the initial startup of the oplog populator
                // we fetch the buckets directly from mongo.
                // We don't have an event date in this case.
                if (eventDate) {
                    connector.setResumePoint(eventDate);
                }
                await connector.addBucket(bucket);
                this._bucketsToConnectors.set(bucket, connector);
                this._metricsHandler.onConnectorConfigured(connector, 'add');
                this._logger.info('Started listening to bucket', {
                    method: 'Allocator.listenToBucket',
                    bucket,
                    connector: connector.name,
                });
            }
        } catch (err) {
            this._logger.error('Error when starting to listen to bucket', {
                method: 'Allocator.listenToBucket',
                bucket,
                error: err.description,
            });
            throw errors.InternalError.customizeDescription(err.description);
        }
    }

    /**
     * Stops listening to bucket by removing
     * the bucket from the connector assigned
     * to it
     * @param {string} bucket bucket name
     * @returns {Promise|undefined} undefined
     * @throws {InternalError}
     */
    async stopListeningToBucket(bucket) {
        try {
            const connector = this._bucketsToConnectors.get(bucket);
            if (connector) {
                this.emit('bucket-removed', bucket, connector);
                await connector.removeBucket(bucket);
                this._bucketsToConnectors.delete(bucket);
                this._metricsHandler.onConnectorConfigured(connector, 'delete');
                this._logger.info('Stopped listening to bucket', {
                    method: 'Allocator.stopListeningToBucket',
                    bucket,
                    connector: connector.name,
                });
            }
        } catch (err) {
            this._logger.error('Error when stopping listening to bucket', {
                method: 'Allocator.stopListeningToBucket',
                bucket,
                error: err.description,
            });
            throw errors.InternalError.customizeDescription(err.description);
        }
    }
}

module.exports = Allocator;
