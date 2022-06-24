const joi = require('joi');
const { errors } = require('arsenal');
const KafkaConnectWrapper = require('../../../lib/wrappers/KafkaConnectWrapper');

const connectorParams = joi.object({
    name: joi.string().required(),
    config: joi.object().required(),
    buckets: joi.array().required(),
    logger: joi.object().required(),
    kafkaConnectHost: joi.string().required(),
    kafkaConnectPort: joi.number().required(),
});

/**
 * @class Connector
 *
 * @classdesc The connector class manages the state of
 * a Kafka-Connect MongoDB source connector, it can spawn,
 * destroy and update the config of the connector when adding
 * or removing buckets from it
 */
class Connector {

    /**
     * @constructor
     * @param {Object} params connector config
     * @param {string} params.name connector name
     * @param {Object} params.config Kafka-connect MongoDB source
     * connector config
     * @param {string[]} params.buckets buckets assigned to this connector
     * @param {Logger} params.logger logger object
     * @param {string} params.kafkaConnectHost kafka connect host
     * @param {number} params.kafkaConnectPort kafka connect port
     */
    constructor(params) {
        joi.attempt(params, connectorParams);
        this._name = params.name;
        this._config = params.config;
        this._buckets = new Set(params.buckets);
        this._logger = params.logger;
        this._kafkaConnect = new KafkaConnectWrapper({
            kafkaConnectHost: params.kafkaConnectHost,
            kafkaConnectPort: params.kafkaConnectPort,
            logger: this._logger,
        });
    }

    /**
     * Getter for connector name
     * @returns {string} connector name
     */
    get name() { return this._name; }

    /**
     * Getter for connector buckets
     * @returns {string[]} buckets assigned to this connector
     */
    get buckets() { return [...this._buckets]; }

    /**
     * Get number of buckets assigned to this
     * connector
     * @returns {Number} number of buckets
     */
    get bucketCount() { return this._buckets.size; }

    /**
     * Creates the Kafka-connect mongo connector
     * @returns {Promise|undefined} undefined
     * @throws {InternalError}
     */
    async spawn() {
        const connectorConfig = { ...this._config };
        try {
            await this._kafkaConnect.createConnector({
                name: this._name,
                config: this._config,
            });
        } catch (err) {
            this._logger.error('Error while spawning connector', {
                method: 'Connector.spawn',
                connector: this._name,
                config: connectorConfig,
                error: err.description || err.message,
            });
            throw errors.InternalError.customizeDescription(err.description);
        }
    }

    /**
     * Destroys the Kafka-connect mongo connector
     * @returns {Promise|undefined} undefined
     * @throws {InternalError}
     */
    async destroy() {
        try {
            await this._kafkaConnect.deleteConnector(this._name);
        } catch (err) {
            this._logger.error('Error while destroying connector', {
                method: 'Connector.destroy',
                connector: this._name,
                error: err.description || err.message,
            });
            throw errors.InternalError.customizeDescription(err.description);
        }
    }

    /**
     * Add bucket to this connector
     * Connector is updated with the new bucket list
     * @param {string} bucket bucket to add
     * @param {boolean} [doUpdate=true] updates connector if true
     * @returns {Promise|undefined} undefined
     * @throws {InternalError}
     */
    async addBucket(bucket, doUpdate = true) {
        try {
            this._buckets.add(bucket);
            await this.updatePipeline(doUpdate);
        } catch (err) {
            this._logger.error('Error while adding bucket to connector', {
                method: 'Connector.addBucket',
                connector: this._name,
                bucket,
                error: err.description || err.message,
            });
            throw errors.InternalError.customizeDescription(err.description);
        }
    }

    /**
     * Remove bucket from this connector
     * Connector is updated with new bucket list
     * @param {string} bucket bucket to add
     * @param {boolean} [doUpdate=true] updates connector if true
     * @returns {Promise|undefined} undefined
     * @throws {InternalError}
     */
    async removeBucket(bucket, doUpdate = true) {
        try {
            this._buckets.delete(bucket);
            await this.updatePipeline(doUpdate);
        } catch (err) {
            this._logger.error('Error while removing bucket from connector', {
                method: 'Connector.removeBucket',
                connector: this._name,
                bucket,
                error: err.description || err.message,
            });
            throw errors.InternalError.customizeDescription(err.description);
        }
    }

    /**
     * Makes new connector pipeline that includes
     * buckets assigned to this connector
     * @param {string[]} buckets list of bucket names
     * @returns {string} new connector pipeline
     */
    _generateConnectorPipeline(buckets) {
        const pipeline = [
            {
                $match: {
                    'ns.coll': {
                        $in: buckets,
                    }
                }
            }
        ];
        return JSON.stringify(pipeline);
    }

    /**
     * Updates connector pipeline with
     * buckets assigned to this connector
     * @param {boolean} [doUpdate=true] updates connector if true
     * @returns {Promise|undefined} undefined
     * @throws {InternalError}
     */
    async updatePipeline(doUpdate = true) {
        this._config.pipeline = this._generateConnectorPipeline([...this._buckets]);
        try {
            if (doUpdate) {
                await this._kafkaConnect.updateConnectorPipeline(this._name, this._config.pipeline);
            }
        } catch (err) {
            this._logger.error('Error while updating connector pipeline', {
                method: 'Connector.updatePipeline',
                connector: this._name,
                buckets: [...this._buckets],
                pipeline: this._config.pipeline,
                error: err.description || err.message,
            });
            throw errors.InternalError.customizeDescription(err.description);
        }
    }

}

module.exports = Connector;
