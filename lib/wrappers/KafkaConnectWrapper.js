const { errors } = require('@scality/arsenal');
const joi = require('joi');
const http = require('http');

const safeJsonParse = require('../util/safeJsonParse');
const { getStringSizeInBytes } = require('../util/buffer');

const paramsJoi = joi.object({
    kafkaConnectHost: joi.string().required(),
    kafkaConnectPort: joi.number().required(),
    logger: joi.object().required(),
    metricsHandler: joi.object(),
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
     * @param {OplogPopulatorMetrics} [params.metricsHandler] records the
     * connector creations and configuration updates, if set
     */
    constructor(params) {
        joi.attempt(params, paramsJoi);
        this._kafkaConnectHost = params.kafkaConnectHost;
        this._kafkaConnectPort = params.kafkaConnectPort;
        this._logger = params.logger;
        this._metricsHandler = params.metricsHandler;
    }

    /**
     * makes http request
     * @param {Object} params request params
     * @param {string} params.method http method
     * @param {string} params.path path to resource
     * @param {string} params.headers request headers (optional)
     * @param {string} [body] request body
     * @returns {Promise|Object} http response
     */
    makeRequest(params, body) {
        return new Promise((resolve, reject) => {
            const req = http.request({
                host: this._kafkaConnectHost,
                port: this._kafkaConnectPort,
                ...params,
            }, res => {
                // cumulate data
                const chunks = [];
                res.on('data', chunk => {
                    chunks.push(chunk);
                });
                // resolve on end
                res.on('end', () => {
                    // DELETE, stop and resume answer with an empty body
                    const { error, result } = safeJsonParse(
                        params.method !== 'DELETE' && Buffer.concat(chunks).toString() || '{}');
                    if (res.statusCode < 200 || res.statusCode >= 300) {
                        // callers handle missing resources themselves
                        const logLevel = res.statusCode === 404 ? 'debug' : 'error';
                        this._logger[logLevel]('Got bad status when making request', {
                            method: 'KafkaConnectManager.makeRequest',
                            statusCode: res.statusCode,
                            statusMessage: res.statusMessage,
                            bodyErrorCode: result?.error_code,
                            bodyMessage: result?.message,
                            params,
                        });
                        const err = errors.InternalError.customizeDescription(
                            `Error occured when making request, ${result?.message || `status ${res.statusCode}`}`);
                        // lets callers tell e.g. a missing connector (404) apart
                        err.statusCode = res.statusCode;
                        return reject(err);
                    }
                    if (error) {
                        this._logger.error('Error occured while parsing request body', {
                            method: 'KafkaConnectManager.makeRequest',
                            error: error.message,
                            params,
                        });
                        return reject(errors.InternalError.customizeDescription(
                            `Error occured while parsing request body, ${error.message}`));
                    }
                    return resolve(result);
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

            if (body) {
                req.write(body);
            }
            req.end();
        });
    }

    /**
     * Records a connector creation or configuration update
     * @param {string} connectorName connector name
     * @param {boolean} success whether kafka connect applied it
     * @param {number} startTime time the request started, in milliseconds
     * @param {number} bytes size of the request body
     * @returns {undefined}
     */
    _recordConfigRequest(connectorName, success, startTime, bytes) {
        const durationS = (Date.now() - startTime) / 1000;
        this._metricsHandler?.onConnectorReconfiguration({ name: connectorName }, success, durationS, bytes);
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
        const body = JSON.stringify(config);
        const bytes = getStringSizeInBytes(body);
        const startTime = Date.now();
        try {
            const connector = await this.makeRequest({
                method: 'POST',
                path: '/connectors',
                headers: {
                    'Content-Type': 'application/json',
                }
            }, body);
            this._recordConfigRequest(config.name, true, startTime, bytes);
            return connector;
        } catch (err) {
            this._recordConfigRequest(config.name, false, startTime, bytes);
            this._logger.error('Error when creating connector', {
                method: 'KafkaConnectManager.createConnector',
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
        const body = JSON.stringify(config);
        const bytes = getStringSizeInBytes(body);
        const startTime = Date.now();
        try {
            const connectorConfig = await this._putConnectorConfig(connectorName, body);
            this._recordConfigRequest(connectorName, true, startTime, bytes);
            return connectorConfig;
        } catch (err) {
            this._recordConfigRequest(connectorName, false, startTime, bytes);
            throw err;
        }
    }

    /**
     * Puts the configuration of an existing connector
     * @param {string} connectorName connector name
     * @param {string} body connector configuration, serialized
     * @returns {Promise|Object} connector configuration
     * @throws {InternalError}
     */
    async _putConnectorConfig(connectorName, body) {
        const activeConnectors = await this.getConnectors();
        if (activeConnectors.includes(connectorName)) {
            try {
                const connectorConfig = await this.makeRequest({
                    method: 'PUT',
                    path: `/connectors/${connectorName}/config`,
                    headers: {
                        'Content-Type': 'application/json',
                    }
                }, body);
                return connectorConfig;
            } catch (err) {
                this._logger.error('Error when putting configuration', {
                    method: 'KafkaConnectManager.updateConnectorConfig',
                    connector: connectorName,
                    error: err.description,
                });
                throw err;
            }
        }
        this._logger.error('Can\'t update connector config, connector not found', {
            method: 'KafkaConnectManager.updateConnectorConfig',
            connector: connectorName,
        });
        throw errors.InternalError.customizeDescription('Could not find connector to update');
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
            if (err.statusCode !== 404) {
                this._logger.error('Error when deleting connector', {
                    method: 'KafkaConnectManager.deleteConnector',
                    connector: connectorName,
                    error: err.description,
                });
            }
            throw err;
        }
    }

    /**
     * Get all connectors with their configuration and status, in a
     * single request. Kafka connect leaves out connectors whose status
     * could not be read.
     * @returns {Promise<Object>} map of connector name to
     * `{ info: { config }, status: { connector, tasks } }`
     * @throws {InternalError}
     */
    async getConnectorsWithStatus() {
        try {
            return await this.makeRequest({
                method: 'GET',
                path: '/connectors?expand=info&expand=status',
            });
        } catch (err) {
            this._logger.error('Error when getting connectors with status', {
                method: 'KafkaConnectManager.getConnectorsWithStatus',
                error: err.description,
            });
            throw err;
        }
    }

    /**
     * Stops a connector (Kafka Connect >= 3.5), idempotent: its tasks shut
     * down, committing their final offsets
     * @param {string} connectorName connector name
     * @returns {Promise|undefined} undefined
     * @throws {InternalError}
     */
    async stopConnector(connectorName) {
        try {
            await this.makeRequest({
                method: 'PUT',
                path: `/connectors/${connectorName}/stop`,
            });
            return undefined;
        } catch (err) {
            if (err.statusCode !== 404) {
                this._logger.error('Error when stopping connector', {
                    method: 'KafkaConnectManager.stopConnector',
                    connector: connectorName,
                    error: err.description,
                });
            }
            throw err;
        }
    }

    /**
     * Get the committed offsets of a connector (Kafka Connect >= 3.5)
     * @param {string} connectorName connector name
     * @returns {Promise<Object[]>} list of `{ partition, offset }`
     * @throws {InternalError}
     */
    async getConnectorOffsets(connectorName) {
        try {
            const res = await this.makeRequest({
                method: 'GET',
                path: `/connectors/${connectorName}/offsets`,
            });
            return res.offsets || [];
        } catch (err) {
            // a 404 (missing connector, or kafka connect < 3.5 without the offsets API) is left to the caller
            if (err.statusCode !== 404) {
                this._logger.error('Error when getting connector offsets', {
                    method: 'KafkaConnectManager.getConnectorOffsets',
                    connector: connectorName,
                    error: err.description,
                });
            }
            throw err;
        }
    }

    /**
     * Restarts a connector
     * @param {string} connectorName connector name
     * @param {boolean} includeTasks Specifies whether to restart
     * the connector instance and task instances or just the connector
     * instance
     * @param {boolean} onlyFailed Specifies whether to restart just
     * the instances with a FAILED status
     * @returns {Promise|undefined} undefined
     * @throws {InternalError}
     */
    async restartConnector(connectorName, includeTasks, onlyFailed) {
        try {
            await this.makeRequest({
                method: 'POST',
                path: `/connectors/${connectorName}/restart?includeTasks=${includeTasks}&onlyFailed=${onlyFailed}`,
            });
            return undefined;
        } catch (err) {
            this._logger.error('Error when restarting connector', {
                method: 'KafkaConnectManager.restartConnector',
                connector: connectorName,
                error: err.description,
            });
            throw err;
        }
    }
 }

module.exports = KafkaConnectWrapper;
