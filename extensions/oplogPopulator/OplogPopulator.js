const joi = require('joi');
const { errors } = require('arsenal');
const { MongoClient } = require('mongodb');
const constants = require('./constants');
const { constructConnectionString } = require('../utils/MongoUtils');
const KafkaConnectWrapper = require('../../lib/wrappers/KafkaConnectWrapper');
const ChangeStream = require('../../lib/wrappers/ChangeStream');

const paramsJoi = joi.object({
    config: joi.object().required(),
    mongoConfig: joi.object().required(),
    activeExtensions: joi.array().required(),
    logger: joi.object().required(),
}).required();

/**
 * @class OplogPopulator
 *
 * @classdesc The OplogPopulator configures kafka connect
 * to read the correct entries from the MongoDB oplog
 */
class OplogPopulator {

    /**
     * @constructor
     * @param {Object} params - constructor params
     * @param {Object} params.config - oplog populator config
     * @param {Object} params.mongoConfig - mongo connection config
     * @param {Object} params.mongoConfig.authCredentials - mongo auth credentials
     * @param {Object} params.mongoConfig.replicaSetHosts - mongo creplication hosts
     * @param {Object} params.mongoConfig.writeConcern - mongo write concern
     * @param {Object} params.mongoConfig.replicaSet - mongo replica set
     * @param {Object} params.mongoConfig.readPreference - mongo read preference
     * @param {Object} params.mongoConfig.database - metadata database
     * @param {string[]} params.activeExtensions - list of all active extension names
     * @param {Object} params.logger - logger
     */
    constructor(params) {
        joi.attempt(params, paramsJoi);
        this._config = params.config;
        this._mongoConfig = params.mongoConfig;
        this._activeExtensions = params.activeExtensions;
        this._logger = params.logger;
        this._connectWrapper = new KafkaConnectWrapper({
            kafkaConnectHost: this._config.kafkaConnectHost,
            kafkaConnectPort: this._config.kafkaConnectPort,
            logger: this._logger,
        });
        this._changeStreamWrapper = null;
        // stores the set of buckets assigned to connector
        this._bucketsForConnector = new Set();
        // MongoDB related
        this._mongoClient = null;
        this._metastore = null;
        // setup mongo connection data
        this._mongoUrl = constructConnectionString(this._mongoConfig);
        this._replicaSet = this._mongoConfig.replicaSet;
        this._database = this._mongoConfig.database;
    }

    /**
     * Connects to MongoDB using the MongoClientInterface
     * and retreives the metastore collection
     * @returns {Promise|undefined} undefined
     * @throws {InternalError}
     */
    async _setupMongoClient() {
        try {
            const client = await MongoClient.connect(this._mongoUrl, {
                 replicaSet: this._replicaSet,
                 useNewUrlParser: true,
            });
            // connect to metadata DB
            this._mongoClient = client.db(this._database, {
                ignoreUndefined: true,
            });
            // get metastore collection
            this._metastore = this._mongoClient.collection(constants.bucketMetastore);
            this._logger.debug('Connected to MongoDB', {
                method: 'OplogPopulator._setupMongoClient',
            });
            return undefined;
        } catch (err) {
            this._logger.error('Could not connect to MongoDB', {
                method: 'OplogPopulator._setupMongoClient',
                error: err.message,
            });
            throw errors.InternalError.customizeDescription(err.message);
        }
    }

    /**
     * get default connector configuration
     * @param {string} connectorName connector name
     * @returns {Object} connector configuration
     */
    _getDefaultConnectorConfiguration(connectorName) {
        // getting default connector config values
        const config = constants.defaultConnectorConfig;
        // connection and source configuration
        config.name = connectorName;
        config.database = this._database;
        config['connection.uri'] = this._mongoUrl;
        // destination topic configuration
        config['topic.namespace.map'] = JSON.stringify({
            '*': this._config.topic,
        });
        return config;
    }

    /**
     * Get buckets that have at least one extension active
     * @returns {string[]} list of buckets to listen to
     * @throws {InternalError}
     */
    async _getBackbeatEnabledBuckets() {
        const filter = {
            $or: [],
        };
        // notification filter
        if (this._activeExtensions.includes('notification')) {
            const field = constants.extensionConfigField.notification;
            const extFilter = {};
            // notification config should be an object
            extFilter[`value.${field}`] = { $type: 3 };
            filter.$or.push(extFilter);
        }
        // replication filter
        if (this._activeExtensions.includes('replication')) {
            const field = constants.extensionConfigField.replication;
            const extFilter = {};
            // processing buckets where at least one replication
            // rule is enabled
            extFilter[`value.${field}.rules`] = {
                $elemMatch: {
                    enabled: true,
                },
            };
            filter.$or.push(extFilter);
        }
        // lifecycle filter
        if (this._activeExtensions.includes('lifecycle')) {
            const field = constants.extensionConfigField.lifecycle;
            const extFilter = {};
            // processing buckets where at least one lifecycle
            // rule is enabled
            extFilter[`value.${field}.rules`] = {
                $elemMatch: {
                    ruleStatus: 'Enabled',
                },
            };
            filter.$or.push(extFilter);
        }
        // ingestion filter
        if (this._activeExtensions.includes('ingestion')) {
            const field = constants.extensionConfigField.ingestion;
            const extFilter = {};
            extFilter[`value.${field}.status`] = 'enabled';
            filter.$or.push(extFilter);
        }
        try {
            const buckets = await this._metastore.find(filter)
                .project({ _id: 1 })
                .map(bucket => bucket._id)
                .toArray();
            return buckets;
        } catch (err) {
            this._logger.error('Error querying buckets from MongoDB', {
                method: 'OplogPopulator._getBackbeatEnabledBuckets',
                error: err.message,
            });
            throw errors.InternalError.customizeDescription(err.message);
        }
    }

    /**
     * Makes new connector pipeline to listen to
     * specified buckets
     * @param {string[]} bucketNames list of buckets to listen to
     * @returns {Object} new connector pipeline
     */
    _generateNewConnectorPipeline(bucketNames) {
        const pipeline = [
            {
                $match: {
                    'ns.coll': {
                        $in: bucketNames,
                    }
                }
            }
        ];
        return JSON.stringify(pipeline);
    }

    /**
     * start listening to bucket
     * @param {string} bucketName bucket name
     * @returns {undefined}
     */
    _listenToBucket(bucketName) {
        this._bucketsForConnector.add(bucketName);
        this.updateConnectorPipeline(constants.defaultConnectorName, [...this._bucketsForConnector]);
    }

    /**
     * stops listening to bucket
     * @param {string} bucketName bucket name
     * @returns {undefined}
     */
    _stopListeningToBucket(bucketName) {
        this._bucketsForConnector.delete(bucketName);
        this.updateConnectorPipeline(constants.defaultConnectorName, [...this._bucketsForConnector]);
    }

    /**
     * Check if buckets has at least one backbeat extension active
     * @param {BucketInfo} bucketMetadata bucket metadata
     * @returns {boolean} is bucket backbeat enabled
     */
    _isBucketBackbeatEnabled(bucketMetadata) {
        const areExtensionsEnabled = [];
        // notification extension
        if (this._activeExtensions.includes('notification')) {
            const field = constants.extensionConfigField.notification;
            areExtensionsEnabled.push(Boolean(bucketMetadata[field] !== null));
        }
        // replication extension
        if (this._activeExtensions.includes('replication')) {
            const field = constants.extensionConfigField.replication;
            const rules = (bucketMetadata[field] && bucketMetadata[field].rules)
                || null;
            if (!rules || rules.length === 0) {
                areExtensionsEnabled.push(false);
            } else {
                const areRulesActive = rules.some(rule =>
                    rule.enabled === true);
                areExtensionsEnabled.push(areRulesActive);
            }
        }
        // lifecycle extension
        if (this._activeExtensions.includes('lifecycle')) {
            const field = constants.extensionConfigField.lifecycle;
            const rules = (bucketMetadata[field] && bucketMetadata[field].rules)
                || null;
            if (!rules || rules.length === 0) {
                areExtensionsEnabled.push(false);
            } else {
                const areRulesActive = rules.some(rule =>
                    rule.ruleStatus === 'Enabled');
                areExtensionsEnabled.push(areRulesActive);
            }
        }
        // ingestion extension
        if (this._activeExtensions.includes('ingestion')) {
            const field = constants.extensionConfigField.ingestion;
            const enabled = (bucketMetadata[field] && bucketMetadata[field].status)
                || null;
            areExtensionsEnabled.push(Boolean(enabled === 'enabled'));
        }
        return areExtensionsEnabled.some(ext => ext);
    }

    /**
     * Handler for the change stream "change" event.
     * Updates connector pipeline when change occurs
     * @param {ChangeStreamDocument} change Change stream change object
     * @returns {undefined}
     */
    _handleChangeStreamChangeEvent(change) {
        const isListeningToBucket = this._bucketsForConnector.has(change.documentKey._id);
        // no fullDocument field in delete events
        const isBackbeatEnabled = change.fullDocument ?
            this._isBucketBackbeatEnabled(change.fullDocument.value) : null;
        switch (change.operationType) {
            case 'delete':
                if (isListeningToBucket) {
                    this._stopListeningToBucket(change.documentKey._id);
                }
                break;
            case 'replace':
            case 'update':
            case 'insert':
                // remove bucket if no longer backbeat enabled
                if (isListeningToBucket && !isBackbeatEnabled) {
                    this._stopListeningToBucket(change.documentKey._id);
                // add bucket if it is backbeat enabled
                } else if (!isListeningToBucket && isBackbeatEnabled) {
                    this._listenToBucket(change.documentKey._id);
                }
                break;
            default:
                this._logger.debug('Skipping unsupported change stream event', {
                    method: 'OplogPopulator._handleChangeStreamChange',
                    type: change.operationType,
                    key: change.documentKey._id,
                });
                break;
        }
        this._logger.debug('Change stream event processed', {
            method: 'OplogPopulator._handleChangeStreamChange',
            type: change.operationType,
            key: change.documentKey._id,
        });
    }

    /**
     * Initializes a change stream on the metastore collection
     * @returns {undefined}
     * @throws {InternalError}
     */
    _setMetastoreChangeStream() {
        const changeStreamPipeline = [
            {
                $project: {
                    '_id': 1,
                    'operationType': 1,
                    'documentKey._id': 1,
                    'fullDocument.value': 1
                },
            },
        ];
        this._changeStreamWrapper = new ChangeStream({
            logger: this._logger,
            collection: this._metastore,
            pipeline: changeStreamPipeline,
            handler: this._handleChangeStreamChangeEvent.bind(this),
            throwOnError: false,
        });
        // start watching metastore
        this._changeStreamWrapper.start();
    }

    /**
     * Updates connector pipeline
     * @param {string} connectorName connector to update
     * @param {array} buckets connector assigned buckets
     * @returns {Promise|Object} connector config
     * @throws {InternalError}
     */
    async updateConnectorPipeline(connectorName, buckets) {
        // TODO: Add support for multiple connectors
        const pipeline = this._generateNewConnectorPipeline(buckets);
        try {
            const config = await this._connectWrapper.updateConnectorPipeline(connectorName, pipeline);
            return config;
        } catch (err) {
            this._logger.error('Error while updating connector', {
                method: 'OplogPopulator.updateConnectorPipeline',
                connector: connectorName,
                buckets,
                error: err.description,
            });
            throw errors.InternalError.customizeDescription(err.description);
        }
    }

    /**
     * Creates and configures kafka connect
     * connectors based on the config
     * @param {Object} connectorConfig Config of connector
     * @returns {Promise|Object} Updated connector configuration
     * @throws {InternalError}
     */
    async _configureConnector(connectorConfig) {
        try {
            const activeConnectors = await this._connectWrapper.getConnectors();
            const connectorName = connectorConfig.name;
            // update the connector config if it already exist
            if (activeConnectors.includes(connectorName)) {
                return this._connectWrapper.updateConnectorConfig(connectorName,
                    connectorConfig.config);
            } else {
                // otherwise create a new connector
                return this._connectWrapper.createConnector({
                    name: connectorName,
                    config: connectorConfig.config,
                });
            }
        } catch (err) {
            this._logger.error('Error while initially configuring connectors', {
                method: 'OplogPopulator._configureConnectors',
                error: err.description,
                config: connectorConfig,
            });
            throw errors.InternalError.customizeDescription(err.description);
        }
    }

    /**
     * Sets up the OplogPopulator
     * @returns {Promise|undefined} undefined
     * @throws {InternalError}
     */
    async setup() {
       try {
           await this._setupMongoClient();
           // getting buckets with at least one extension active
           const backbeatEnabledBuckets = await this._getBackbeatEnabledBuckets();
           this._bucketsForConnector = new Set(backbeatEnabledBuckets);
           // TODO: add support for multiple connectors
            const defaultConnectorConfig = this._getDefaultConnectorConfiguration(constants.defaultConnectorName);
            defaultConnectorConfig.pipeline = this._generateNewConnectorPipeline(backbeatEnabledBuckets);
            const connectorConfigs = {
                name: constants.defaultConnectorName,
                config: defaultConnectorConfig
            };
            await this._configureConnector(connectorConfigs);
           this._setMetastoreChangeStream();
       } catch (err) {
            this._logger.error('An error occured when setting up the OplogPopulator', {
                method: 'OplogPopulator.setup',
                error: err.description,
            });
            throw errors.InternalError.customizeDescription(err.description);
       }
    }
}

module.exports = OplogPopulator;
