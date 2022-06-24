const joi = require('joi');
const async = require('async');
const uuid = require('uuid');
const { errors } = require('arsenal');
const constants = require('../constants');
const KafkaConnectWrapper = require('../../../lib/wrappers/KafkaConnectWrapper');
const Connector = require('./Connector');

const paramsJoi = joi.object({
    nbConnectors: joi.number().required(),
    database: joi.string().required(),
    mongoUrl: joi.string().required(),
    oplogTopic: joi.string().required(),
    prefix: joi.string(),
    kafkaConnectHost: joi.string().required(),
    kafkaConnectPort: joi.number().required(),
    logger: joi.object().required(),
}).required();

/**
 * @class ConnectorsManager
 *
 * @classdesc ConnectorsManager handles connector logic
 * for spawning connectors and retreiving old ones
 */
class ConnectorsManager {

    /**
     * @constructor
     * @param {Object} params params
     * @param {number} params.nbConnectors number of connectors to have
     * @param {string} params.database MongoDB database to use (for connector)
     * @param {string} params.mongoUrl MongoDB connection url
     * @param {string} params.oplogTopic topic to use for oplog
     * @param {string} params.kafkaConnectHost kafka connect host
     * @param {number} params.kafkaConnectPort kafka connect port
     * @param {Logger} params.logger logger object
     */
    constructor(params) {
        joi.attempt(params, paramsJoi);
        this._nbConnectors = params.nbConnectors;
        this._logger = params.logger;
        this._kafkaConnectHost = params.kafkaConnectHost;
        this._kafkaConnectPort = params.kafkaConnectPort;
        this._kafkaConnect = new KafkaConnectWrapper({
            kafkaConnectHost: this._kafkaConnectHost,
            kafkaConnectPort: this._kafkaConnectPort,
            logger: this._logger,
        });
        this._database = params.database;
        this._mongoUrl = params.mongoUrl;
        this._oplogTopic = params.oplogTopic;
        this._prefix = params.prefix || '';
        this._connectors = [];
        // used for initial clean up of old connector pipelines
        this._oldConnectors = [];
    }

    /**
     * get default connector configuration
     * @param {string} connectorName connector name
     * @returns {Object} connector configuration
     */
    _getDefaultConnectorConfiguration(connectorName) {
        const connectorConfig = {
            'name': connectorName,
            'database': this._database,
            'connection.uri': this._mongoUrl,
            'topic.namespace.map': JSON.stringify({
                '*': this._oplogTopic,
            })
        };
        return {
            ...constants.defaultConnectorConfig,
            ...connectorConfig
        };
    }

    /**
     * generates a random connector name
     * @returns {string} generated connector name
     */
    _generateConnectorName() {
        return `${this._prefix}${constants.defaultConnectorName}-${uuid.v4()}`;
    }

    /**
     * Creates a connector
     * @param {boolean} spawn should connector be spawned
     * @returns {Promise|Connector} created connector
     * @throws {InternalError}
     */
    async addConnector(spawn = true) {
        try {
            // generate connector name
            const connectorName = this._generateConnectorName();
            // get connector config
            const config = this._getDefaultConnectorConfiguration(connectorName);
            // initialize connector
            const connector = new Connector({
                name: connectorName,
                config,
                buckets: [],
                logger: this._logger,
                kafkaConnectHost: this._kafkaConnectHost,
                kafkaConnectPort: this._kafkaConnectPort,
            });
            if (spawn) {
                await connector.spawn();
            }
            return connector;
        } catch (err) {
            this._logger.error('An error occurred while creating connector', {
                method: 'ConnectorsManager.addConnector',
                error: err.description || err.message,
            });
            throw errors.InternalError.customizeDescription(err.description);
        }
    }

    /**
     * Extracts buckets from a connector config pipeline
     * @param {Object} connectorConfig connector config
     * @returns {string[]} list of buckets
     */
     _extractBucketsFromConfig(connectorConfig) {
        const pipeline = connectorConfig.pipeline ?
            JSON.parse(connectorConfig.pipeline) : null;
        if (!pipeline || pipeline.length === 0) {
            return [];
        }
        return pipeline[0].$match['ns.coll'].$in;
    }

    /**
     * Gets old connector configs and initializes connector
     * instances
     * @param {string[]} connectorNames connector names
     * @returns {Promise|Connector[]} list of connectors
     */
    async _getOldConnectors(connectorNames) {
        try {
            const connectors = Promise.all(connectorNames.map(async connectorName => {
                // get old connector config
                const config = await this._kafkaConnect.getConnectorConfig(connectorName);
                // extract buckets from old connector config and filter them
                // only leaving currently valid buckets
                const buckets = this._extractBucketsFromConfig(config);
                // initializing connector
                const connector = new Connector({
                    name: connectorName,
                    config,
                    buckets,
                    logger: this._logger,
                    kafkaConnectHost: this._kafkaConnectHost,
                    kafkaConnectPort: this._kafkaConnectPort,
                });
                return connector;
            }));
            return connectors;
        } catch (err) {
            this._logger.error('An error occurred while getting old connectors', {
                method: 'ConnectorsManager._getOldConnectors',
                error: err.description || err.message,
            });
            throw errors.InternalError.customizeDescription(err.description);
        }
    }

    /**
     * Initialize previously created connector instances and
     * creates new connectors based on configuration
     * @returns {Promise|Connector[]} list connectors
     * @throws {InternalError}
     */
    async initializeConnectors() {
        try {
            // get and initialize old connectors
            const oldConnectorNames = await this._kafkaConnect.getConnectors();
            if (oldConnectorNames) {
                const oldConnectors = await this._getOldConnectors(oldConnectorNames);
                this._connectors.push(...oldConnectors);
                this._oldConnectors.push(...oldConnectors);
            }
            // Add connectors if required number of connectors not reached
            const nbConnectorsToAdd = this._nbConnectors - this._connectors.length;
            for (let i = 0; i < nbConnectorsToAdd; i++) {
                // eslint-disable-next-line no-await-in-loop
                const newConnector = await this.addConnector();
                this._connectors.push(newConnector);
            }
            return this._connectors;
        } catch (err) {
            this._logger.error('An error occurred while initializing connectors', {
                method: 'ConnectorsManager.initializeConnectors',
                error: err.description || err.message,
            });
            throw errors.InternalError.customizeDescription(err.description);
        }
    }

    /**
     * Removes invalid buckets from connector config
     * @param {Connector} connector connector
     * @param {string[]} buckets valid bucket names
     * @returns {Promise|undefined} undefined
     * @throws {InternalError}
     */
    async removeConnectorInvalidBuckets(connector, buckets) {
        try {
            // getting connector's invalid buckets
            const invalidBuckets = connector.buckets.filter(bucket =>
                !buckets.includes(bucket));
            // removing invalid buckets
            await async.eachLimit(invalidBuckets, 10, async bucket =>
                connector.removeBucket(bucket, false));
            // updating connector pipeline
            await connector.updatePipeline(true);
        } catch (err) {
            this._logger.error('An error occurred while removing invalid buckets from a connector', {
                method: 'ConnectorsManager.removeConnectorInvalidBuckets',
                error: err.description || err.message,
                connector: connector.name,
            });
            throw errors.InternalError.customizeDescription(err.description);
        }
    }

    /**
     * Removes invalid buckets from old connectors
     * @param {string[]} buckets valid bucket names
     * @returns {Promise|undefined} undefined
     * @throws {InternalError}
     */
    async removeInvalidBuckets(buckets) {
        try {
            await Promise.all(this._oldConnectors.map(connector =>
                this.removeConnectorInvalidBuckets(connector, buckets)));
        } catch (err) {
            this._logger.error('An error occurred while removing invalid buckets from connectors', {
                method: 'ConnectorsManager.removeInvalidBuckets',
                error: err.description || err.message,
            });
            throw errors.InternalError.customizeDescription(err.description);
        }
    }

    /**
     * Get currently active connectors
     * @returns {Connectors[]} list of connectors
     */
    get connectors() { return this._connectors; }
}

module.exports = ConnectorsManager;
