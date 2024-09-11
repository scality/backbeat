const joi = require('joi');
const { errors } = require('arsenal');

const OplogPopulatorMetrics = require('../OplogPopulatorMetrics');
const LeastFullConnector = require('../allocationStrategy/LeastFullConnector');
const constants = require('../constants');

const paramsJoi = joi.object({
    connectorsManager: joi.object().required(),
    maximumBucketsPerConnector: joi.number().default(constants.maxBucketPerConnector),
    metricsHandler: joi.object()
        .instance(OplogPopulatorMetrics).required(),
    logger: joi.object().required(),
}).required();

/**
 * @class Allocator
 *
 * @classdesc Allocator handles listening to buckets by assigning
 * a connector to them.
 */
class Allocator {

    /**
     * @constructor
     * @param {Object} params Allocator param
     * @param {ConnectorsManager} params.connectorsManager connectorsManager
     * @param {Logger} params.logger logger object
     */
    constructor(params) {
        joi.attempt(params, paramsJoi);
        this._connectorsManager = params.connectorsManager;
        this._logger = params.logger;
        this._allocationStrategy = new LeastFullConnector({
            logger: params.logger,
            maximumBucketsPerConnector: params.maximumBucketsPerConnector,
            addConnector: this._connectorsManager.addConnector.bind(this._connectorsManager),
        });
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
            if (!this._bucketsToConnectors.has(bucket)) {
                const connectors = this._connectorsManager.connectors;
                const connector = this._allocationStrategy.getConnector(connectors);
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
