const joi = require('joi');
const { errors } = require('arsenal');
const { MongoClient } = require('mongodb');
const constants = require('./constants');
const { constructConnectionString } = require('../utils/MongoUtils');
const ChangeStream = require('../../lib/wrappers/ChangeStream');
const Allocator = require('./modules/Allocator');
const ConnectorsManager = require('./modules/ConnectorsManager');

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
        this._changeStreamWrapper = null;
        this._allocator = null;
        this._connectorsManager  = null;
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
        // return empty list if no extension active
        if (filter.$or.length === 0) {
            return [];
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
     * Checks if buckets has at least one backbeat extension active
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
    async _handleChangeStreamChangeEvent(change) {
        const isListeningToBucket = this._allocator.has(change.documentKey._id);
        // no fullDocument field in delete events
        const isBackbeatEnabled = change.fullDocument ?
            this._isBucketBackbeatEnabled(change.fullDocument.value) : null;
        switch (change.operationType) {
            case 'delete':
                if (isListeningToBucket) {
                    await this._allocator.stopListeningToBucket(change.documentKey._id);
                }
                break;
            case 'replace':
            case 'update':
            case 'insert':
                // remove bucket if no longer backbeat enabled
                if (isListeningToBucket && !isBackbeatEnabled) {
                    await this._allocator.stopListeningToBucket(change.documentKey._id);
                // add bucket if it became backbeat enabled
                } else if (!isListeningToBucket && isBackbeatEnabled) {
                    await this._allocator.listenToBucket(change.documentKey._id);
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
     * Sets the OplogPopulator
     * @returns {Promise|undefined} undefined
     * @throws {InternalError}
     */
    async setup() {
       try {
           this._connectorsManager = new ConnectorsManager({
               nbConnectors: this._config.numberOfConnectors,
               database: this._database,
               mongoUrl: this._mongoUrl,
               oplogTopic: this._config.topic,
               prefix: this._config.prefix,
               kafkaConnectHost: this._config.kafkaConnectHost,
               kafkaConnectPort: this._config.kafkaConnectPort,
               logger: this._logger,
            });
            await this._connectorsManager.initializeConnectors();
            this._allocator = new Allocator({
                connectorsManager: this._connectorsManager,
                logger: this._logger,
            });
            // initialize mongo client
            await this._setupMongoClient();
            // get currently valid buckets from mongo
            const validBuckets = await this._getBackbeatEnabledBuckets();
            // listen to valid buckets
            await Promise.all(validBuckets.map(bucket => this._allocator.listenToBucket(bucket)));
            // establish change stream
            this._setMetastoreChangeStream();
            // remove no longer valid buckets from old connectors
            await this._connectorsManager.removeInvalidBuckets(validBuckets);
            this._logger.debug('OplogPopulator setup complete', {
                method: 'OplogPopulator.setup',
            });
       } catch (err) {
            this._logger.error('An error occured when setting up the OplogPopulator', {
                method: 'OplogPopulator.setup',
                error: err.description || err.message,
            });
            throw errors.InternalError.customizeDescription(err.description);
       }
    }
}

module.exports = OplogPopulator;
