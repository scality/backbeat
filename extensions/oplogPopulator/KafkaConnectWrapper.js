const { errors } = require('arsenal');
const joi = require('joi');
const http = require('http');

const paramsJoi = joi.object({
    kafkaConnectHost: joi.string().required(),
    kafkaConnectPort: joi.number().required(),
    logger: joi.object().required(),
}).required();

/**
 * @class KafkaConnectWrapper
 *
 * @classdesc Wrapper arround the Kafka Connect REST API.
 */
class KafkaConnectWrapper {

    /**
     * @constructor
     * @param {Object} params constructor params
     * @param {string} params.kafkaConnectHost kafka connect host
     * @param {number} params.kafkaConnectPort kafka connect port
     * @param {Logger} params.logger - logger object
     */
    constructor(params) {
        joi.attempt(params, paramsJoi);
        this._kafkaConnectHost = params.kafkaConnectHost;
        this._kafkaConnectPort = params.kafkaConnectPort;
        this._logger = params.logger;
    }

    /**
     * makes http request
     * @param {Object} params request params
     * @param {string} params.method http method
     * @param {string} params.path path to resource
     * @param {string} params.headers request headers (optional)
     * @param {Object} data request data
     * @returns {Promise|Object} http response
     */
    makeRequest(params, data) {
        return new Promise((resolve, reject) => {
            const req = http.request({
                host: this._kafkaConnectHost,
                port: this._kafkaConnectPort,
                ...params,
            }, res => {
                // cumulate data
                let body = [];
                res.on('data', chunk => {
                    body.push(chunk);
                });
                // resolve on end
                res.on('end', () => {
                    try {
                        body = JSON.parse(Buffer.concat(body).toString());
                    } catch (e) {
                        this._logger.error('Error occured while parsing request body', {
                            method: 'KafkaConnectManager.makeRequest',
                            error: e.message,
                            params,
                        });
                        return reject(errors.InternalError.customizeDescription(
                            `Error occured while parsing request body, ${e.message}`));
                    }
                    // reject on bad status
                    if (res.statusCode < 200 || res.statusCode >= 300) {
                        this._logger.error('Got bad status when making request', {
                            method: 'KafkaConnectManager.makeRequest',
                            statusCode: res.statusCode,
                            statusMessage: res.message,
                            bodyErrorCode: body.error_code,
                            bodyMessage: body.message,
                            params,
                        });
                        return reject(errors.InternalError.customizeDescription(
                            `Error occured when making request, ${body.message}`
                        ));
                    }
                    return resolve(body);
                });
            });
            // reject on request error
            req.on('error', err => {
                this._logger.error('Error occured when making request', {
                    method: 'KafkaConnectManager.makeRequest',
                    error: err.message,
                    params,
                });
                reject(errors.InternalError.customizeDescription(
                    `Error occured when making request, ${err.message}`));
            });

            if (data) {
                req.write(JSON.stringify(data));
            }
            req.end();
        });
    }

    /**
     * Get the list of active connectors
     * @returns {Promise|Array} list of connectors
     * @throws {InternalError}
     */
    async getConnectors() {
        try {
            const connectors = await this.makeRequest({
                method: 'GET',
                path: '/connectors',
            });
            return connectors;
        } catch (err) {
            this._logger.error('Error when getting active connectors', {
                method: 'KafkaConnectManager.getConnectors',
                error: err.description,
            });
            throw err;
        }
    }

    /**
     * create a connector with a given configuration
     * @param {Object} config configuration
     * @param {string} config.name connector name
     * @param {Object} config.config connector configuration
     * @returns {Promise|Object} Connector info
     * @throws {InternalError}
     */
    async createConnector(config) {
        try {
            const connector = await this.makeRequest({
                method: 'POST',
                path: '/connectors',
                headers: {
                    'Content-Type': 'application/json',
                }
            }, config);
            return connector;
        } catch (err) {
            this._logger.error('Error when creating connector', {
                method: 'KafkaConnectManager.createConnector',
                config,
                error: err.description,
            });
            throw err;
        }
    }

    /**
     * Get information about a connector
     * @param {string} connectorName connector name
     * @returns {Promise|Object} connector info
     * @throws {InternalError}
     */
    async getConnector(connectorName) {
        try {
            const connector = await this.makeRequest({
                method: 'GET',
                path: `/connectors/${connectorName}`,
            });
            return connector;
        } catch (err) {
            this._logger.error('Error when getting connector', {
                method: 'KafkaConnectManager.getConnector',
                connector: connectorName,
                error: err.description,
            });
            throw err;
        }
    }

    /**
     * Get the configuration of a connector
     * @param {string} connectorName connector name
     * @returns {Promise|Object} connector configuration
     * @throws {InternalError}
     */
    async getConnectorConfig(connectorName) {
        try {
            const connectorConfig = await this.makeRequest({
                method: 'GET',
                path: `/connectors/${connectorName}/config`,
            });
            return connectorConfig;
        } catch (err) {
            this._logger.error('Error when getting connector configuration', {
                method: 'KafkaConnectManager.getConnectorConfig',
                connector: connectorName,
                error: err.description,
            });
            throw err;
        }
    }

    /**
     * Get the status of a connector
     * @param {string} connectorName connector name
     * @returns {Promise|Object} connector status
     * @throws {InternalError}
     */
    async getConnectorStatus(connectorName) {
        try {
            const connectorStatus = await this.makeRequest({
                method: 'GET',
                path: `/connectors/${connectorName}/status`,
            });
            return connectorStatus;
        } catch (err) {
            this._logger.error('Error when getting connector status', {
                method: 'KafkaConnectManager.getConnectorStatus',
                connector: connectorName,
                error: err.description,
            });
            throw err;
        }
    }

    /**
     * Update the configuration of a connector
     * @param {string} connectorName connector name
     * @param {Object} config connector configuration
     * @returns {Promise|Object|undefined} connector
     * configuration (returns undefined if connector
     * not updated)
     * @throws {InternalError}
     */
    async updateConnectorConfig(connectorName, config) {
        const activeConnectors = await this.getConnectors();
        if (activeConnectors.includes(connectorName)) {
            try {
                const connectorConfig = await this.makeRequest({
                    method: 'PUT',
                    path: `/connectors/${connectorName}/config`,
                    headers: {
                        'Content-Type': 'application/json',
                    }
                }, config);
                return connectorConfig;
            } catch (err) {
                this._logger.error('Error when putting configuration', {
                    method: 'KafkaConnectManager.updateConnectorConfig',
                    connector: connectorName,
                    config,
                    error: err.description,
                });
                throw err;
            }
        }
        // do nothing if connector not found
        this._logger.error('Can\'t update connector config, connector not found', {
            method: 'KafkaConnectManager.updateConnectorConfig',
            connector: connectorName,
        });
        return undefined;
    }

    /**
     * Updated the mongo pipeline used by the connector
     * @param {string} connectorName connector name
     * @param {string} pipeline pipeline definition
     * @returns {Promise|Object} new connector configuration
     * @throws {InternalError}
     */
    async updateConnectorPipeline(connectorName, pipeline) {
        try {
            const connectorConfig = await this.getConnectorConfig(connectorName);
            connectorConfig.pipeline = pipeline;
            const newConnectorConfig = await this.updateConnectorConfig(connectorName,
                connectorConfig);
            return newConnectorConfig;
        } catch (err) {
            this._logger.error('Error when updating connector pipeline', {
                method: 'KafkaConnectManager.updateConnectorPipeline',
                connector: connectorName,
                pipeline,
                error: err.description,
            });
            throw err;
        }
    }

    /**
     * Deletes a connector
     * @param {string} connectorName connector name
     * @returns {Promise|undefined} undefined
     * @throws {InternalError}
    */
    async deleteConnector(connectorName) {
        try {
            await this.makeRequest({
                method: 'DELETE',
                path: `/connectors/${connectorName}`,
            });
            return undefined;
        } catch (err) {
            this._logger.error('Error when deleting connector', {
                method: 'KafkaConnectManager.deleteConnector',
                connector: connectorName,
                error: err.description,
            });
            throw err;
        }
    }
 }

module.exports = KafkaConnectWrapper;
