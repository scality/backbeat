const joi = require('joi');
const async = require('async');
const uuid = require('uuid');
const util = require('util');
const schedule = require('node-schedule');
const { errors } = require('arsenal');

const constants = require('../constants');
const KafkaConnectWrapper = require('../../../lib/wrappers/KafkaConnectWrapper');
const Connector = require('./Connector');
const OplogPopulatorMetrics = require('../OplogPopulatorMetrics');

const paramsJoi = joi.object({
    nbConnectors: joi.number().required(),
    database: joi.string().required(),
    mongoUrl: joi.string().required(),
    oplogTopic: joi.string().required(),
    cronRule: joi.string().required(),
    prefix: joi.string(),
    heartbeatIntervalMs: joi.number().required(),
    kafkaConnectHost: joi.string().required(),
    kafkaConnectPort: joi.number().required(),
    metricsHandler: joi.object()
        .instance(OplogPopulatorMetrics).required(),
    logger: joi.object().required(),
}).required();

// Promisify async functions
const eachLimit = util.promisify(async.eachLimit);
const timesLimit = util.promisify(async.timesLimit);

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
     * @param {string} params.cronRule connector updates cron rule
     * @param {string} params.kafkaConnectHost kafka connect host
     * @param {number} params.kafkaConnectPort kafka connect port
     * @param {Logger} params.logger logger object
     */
    constructor(params) {
        joi.attempt(params, paramsJoi);
        this._nbConnectors = params.nbConnectors;
        this._cronRule = params.cronRule;
        this._heartbeatIntervalMs = params.heartbeatIntervalMs;
        this._logger = params.logger;
        this._kafkaConnectHost = params.kafkaConnectHost;
        this._kafkaConnectPort = params.kafkaConnectPort;
        this._kafkaConnect = new KafkaConnectWrapper({
            kafkaConnectHost: this._kafkaConnectHost,
            kafkaConnectPort: this._kafkaConnectPort,
            logger: this._logger,
        });
        this._metricsHandler = params.metricsHandler;
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
            }),
            // hearbeat prevents having an outdated resume token in the connectors
            // by constantly updating the offset to the last object in the oplog
            'heartbeat.interval.ms': this._heartbeatIntervalMs,
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
            this._logger.debug('Successfully created connector', {
                method: 'ConnectorsManager.addConnector',
                connector: connector.name
            });
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
            const connectors = await Promise.all(connectorNames.map(async connectorName => {
                // get old connector config
                const oldConfig = await this._kafkaConnect.getConnectorConfig(connectorName);
                // extract buckets from old connector config
                const buckets = this._extractBucketsFromConfig(oldConfig);
                // generating a new config as the old config can be outdated (wrong topic for example)
                const config = this._getDefaultConnectorConfiguration(connectorName);
                // initializing connector
                const connector = new Connector({
                    name: connectorName,
                    // update existing connector config while leaving in fields that were
                    // added manually like 'offset.topic.name'
                    config: { ...oldConfig, ...config },
                    buckets,
                    logger: this._logger,
                    kafkaConnectHost: this._kafkaConnectHost,
                    kafkaConnectPort: this._kafkaConnectPort,
                });
                this._logger.debug('Successfully retreived old connector', {
                    method: 'ConnectorsManager._getOldConnectors',
                    connector: connector.name
                });
                return connector;
            }));
            this._logger.info('Successfully retreived old connectors', {
                method: 'ConnectorsManager._getOldConnectors',
                numberOfConnectors: connectors.length
            });
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
                this._metricsHandler.onConnectorsInstantiated(true, oldConnectors.length);
            }
            // Add connectors if required number of connectors not reached
            const nbConnectorsToAdd = this._nbConnectors - this._connectors.length;
            await timesLimit(nbConnectorsToAdd, 10, async () => {
                const newConnector = await this.addConnector();
                this._connectors.push(newConnector);
                this._metricsHandler.onConnectorsInstantiated(false);
            });
            this._logger.info('Successfully initialized connectors', {
                method: 'ConnectorsManager.initializeConnectors',
                numberOfActiveConnectors: this._connectors.length
            });
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
     * Schedules connector updates
     * @returns {undefined}
     */
    scheduleConnectorUpdates() {
        schedule.scheduleJob(this._cronRule, async () => {
            const connectorsStatus = {};
            let connectorUpdateFailed = false;
            await eachLimit(this._connectors, 10, async connector => {
                const startTime = Date.now();
                connectorsStatus[connector.name] = {
                    numberOfBuckets: connector.bucketCount,
                    updated: null,
                };
                try {
                    const updated = await connector.updatePipeline(true);
                    connectorsStatus[connector.name].updated = updated;
                    if (updated) {
                        const delta = (Date.now() - startTime) / 1000;
                        this._metricsHandler.onConnectorReconfiguration(connector, true, delta);
                    }
                } catch (err) {
                    connectorUpdateFailed = true;
                    connectorsStatus[connector.name].updated = false;
                    this._metricsHandler.onConnectorReconfiguration(connector, false);
                    this._logger.error('Failed to updated connector', {
                        method: 'ConnectorsManager.scheduleConnectorUpdates',
                        connector: connector.name,
                        bucketCount: connector.bucketCount,
                        error: err.description || err.message,
                    });
                }
            });
            const logMessage = connectorUpdateFailed ? 'Failed to update some or all the connectors' :
                'Successfully updated all the connectors';
            const logFunction = connectorUpdateFailed ? this._logger.error.bind(this._logger) :
                this._logger.info.bind(this._logger);
            logFunction(logMessage, {
                method: 'ConnectorsManager.scheduleConnectorUpdates',
                connectorsStatus,
            });
        });
    }

    /**
     * Get list of connectors created by this
     * instance of the oplogPopulator
     * @returns {Connectors[]} list of connectors
     */
    get connectors() { return this._connectors; }

    /**
     * Get list of connectors not created by this
     * instance of the oplogPopulator
     * @returns {Connectors[]} list of connectors
     */
    get oldConnectors() { return this._oldConnectors; }
}

module.exports = ConnectorsManager;
