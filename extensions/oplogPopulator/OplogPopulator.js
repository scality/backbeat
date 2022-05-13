const joi = require('joi');
const constants = require('./constants');
const KafkaConnectWrapper = require('./KafkaConnectWrapper');

const paramsJoi = joi.object({
    config: joi.object().required(),
    mongoConfig: joi.object().required(),
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
     * @param {Object} params.logger - logger
     */
    constructor(params) {
        joi.attempt(params, paramsJoi);
        this._config = params.config;
        this._mongoConfig = params.mongoConfig;
        this._logger = params.logger;
        this._connectWrapper = new KafkaConnectWrapper({
            kafkaConnectHost: this._config.kafkaConnectHost,
            kafkaConnectPort: this._config.kafkaConnectPort,
            logger: this._logger,
        });
    }

    /**
     * Sets default connector configuration
     * @param {Object} connectorConfig custom connector configuration
     * @param {string} connectorConfig.name connector name
     * @returns {Object} connector configuration
     */
    getDefaultConnectorConfiguration(connectorConfig) {
        const { authCredentials, replicaSetHosts, writeConcern, replicaSet,
            readPreference, database } = this._mongoConfig;
        // getting default connector config values
        const config = constants.defaultConnectorConfig;
        // connection and source configuration
        config.name = connectorConfig.name;
        config.database = database;
        // get credentials
        let cred = '';
        if (authCredentials &&
            authCredentials.username &&
            authCredentials.password) {
            const username = encodeURIComponent(authCredentials.username);
            const password = encodeURIComponent(authCredentials.password);
            cred = `${username}:${password}@`;
        }
        let connectionUrl = `mongodb://${cred}${replicaSetHosts}/` +
            `?w=${writeConcern}&readPreference=${readPreference}`;
        if (replicaSet) {
            connectionUrl += `&replicaSet=${replicaSet}`;
        }
        config['connection.uri'] = connectionUrl;
        // destination topic configuration
        config['topic.namespace.map'] = JSON.stringify({
            '*': this._config.topic,
        });
        return config;
    }

    /**
     * Creates and configures kafka connect
     * connectors based on the config
     * @returns {Promise|Object} Updated connector configuration
     * @throws {InternalError}
     */
    async setup() {
        const activeConnectors = await this._connectWrapper.getConnectors();
        await Promise.all(this._config.connectors.map(async connectorConfig => {
            // getting default config
            const defaultConfig = this.getDefaultConnectorConfiguration(connectorConfig);
            // update the connector config if it already exist
            if (activeConnectors.includes(connectorConfig.name)) {
                return this._connectWrapper.updateConnectorConfig(connectorConfig.name, defaultConfig);
            } else {
                // otherwise create a new connector
                return this._connectWrapper.createConnector({
                    name: connectorConfig.name,
                    config: defaultConfig,
                });
            }
        }));
    }

}

module.exports = OplogPopulator;
