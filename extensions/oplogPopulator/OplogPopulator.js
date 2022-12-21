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
        // contains OplogPopulatorUtils class of each supported extension
        this._extHelpers = {};
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
            this._logger.info('Connected to MongoDB', {
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
     * Load OplogPopulatorUtils Class of each
     * supported active backbeat extension
     * @returns {undefined}
     */
    _loadOplogHelperClasses() {
        this._activeExtensions.forEach(extName => {
            const index = require(`../../extensions/${extName}/index.js`);
            if (index.oplogPopulatorUtils) {
                this._extHelpers[extName] = index.oplogPopulatorUtils;
            }
        });
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
        // getting MongoDB filters of extensions
        Object.values(this._extHelpers).forEach(extHelper => {
            const extFilter = extHelper.getExtensionMongoDBFilter();
            filter.$or.push(extFilter);
        });

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
        return Object.values(this._extHelpers).some(extHelper =>
            extHelper.isBucketExtensionEnabled(bucketMetadata));
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
                this._logger.info('Skipping unsupported change stream event', {
                    method: 'OplogPopulator._handleChangeStreamChange',
                    type: change.operationType,
                    key: change.documentKey._id,
                });
                break;
        }
        this._logger.info('Change stream event processed', {
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
           this._loadOplogHelperClasses();
           this._connectorsManager = new ConnectorsManager({
               nbConnectors: this._config.numberOfConnectors,
               database: this._database,
               mongoUrl: this._mongoUrl,
               oplogTopic: this._config.topic,
               cronRule: this._config.connectorsUpdateCronRule,
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
            const oldConnectorBuckets = this._connectorsManager.oldConnectors
                .map(connector => connector.buckets)
                .flat();
            const invalidBuckets = oldConnectorBuckets.filter(bucket => !validBuckets.includes(bucket));
            await Promise.all(invalidBuckets.map(bucket => this._allocator.stopListeningToBucket(bucket)));
            this._logger.info('Successfully removed invalid buckets from old connectors', {
                method: 'ConnectorsManager.removeInvalidBuckets',
            });
            // start scheduler for updating connectors
            this._connectorsManager.scheduleConnectorUpdates();
            this._logger.info('OplogPopulator setup complete', {
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

    /**
     * Check if OplogPopulator is ready
     * @returns {bool} is oplogPopulator ready
     */
    isReady() {
        const components = {
            mongoClient: this._mongoClient,
            metastore: this._metastore,
            connectorsManager: this._connectorsManager,
            allocator: this._allocator,
            changeStream: this._changeStreamWrapper
        };

        const allReady = Object.values(components).every(v => v);
        if (!allReady) {
            this._logger.error('ready state', components);
        }
        return allReady;
    }
}

module.exports = OplogPopulator;
