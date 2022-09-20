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
        this._state = {
            // Used to check if buckets assigned to this connector
            // got modified from the last connector update
            bucketsGotModified: true,
            // Used to avoid concurrency issues when updating the state.
            // the state value gets updated to false only when the connector
            // update is successful. And because updating the connector is an
            // asynchronous operation, multiple updates could of happened in the
            // mean time, so we only set to false when no other update happened.
            lastUpdated: Date.now(),
            isUpdating: false,
        };
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
     * @param {boolean} [doUpdate=false] updates connector if true
     * @returns {Promise|undefined} undefined
     * @throws {InternalError}
     */
    async addBucket(bucket, doUpdate = false) {
        this._buckets.add(bucket);
        this._updateConnectorState(true);
        try {
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
     * @param {boolean} [doUpdate=false] updates connector if true
     * @returns {Promise|undefined} undefined
     * @throws {InternalError}
     */
    async removeBucket(bucket, doUpdate = false) {
        this._buckets.delete(bucket);
        this._updateConnectorState(true);
        try {
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
     * Handles updating the values of _bucketsGotModified
     * @param {boolean} bucketsGotModified value of _state.bucketsGotModified
     * to set
     * @param {number} timeBeforeUpdate time just before async
     * connector update operation (only needed when updated connector)
     * @returns {undefined}
     */
    _updateConnectorState(bucketsGotModified, timeBeforeUpdate = 0) {
        const currentTime = Date.now();
        // If updating to false (connector got updated), we
        // need to check if any update occured while asynchronously
        // updating the connector, as those operations were not included
        // in the update
        const shouldUpdateState = !bucketsGotModified && !!timeBeforeUpdate &&
            timeBeforeUpdate >= this._state.lastUpdated;
        // If updating to true (a bucket got added/removed or update failed)
        // directly update the value as checking is not required
        if (bucketsGotModified || shouldUpdateState) {
            this._state.bucketsGotModified = bucketsGotModified;
        }
        this._state.lastUpdated = currentTime;
        return undefined;
    }

    /**
     * Updates connector pipeline with
     * buckets assigned to this connector
     *
     * The first time this function is called,
     * on an old connector, it updates it's configuration
     * as it can be outdated; having the wrong topic for example.
     * That is why we use updateConnectorConfig() instead of
     * updateConnectorPipeline()
     * @param {boolean} [doUpdate=false] updates connector if true
     * @returns {Promise|boolean} connector did update
     * @throws {InternalError}
     */
    async updatePipeline(doUpdate = false) {
        // Only update when buckets changed and when not already updating
        if (!this._state.bucketsGotModified || this._state.isUpdating) {
            return false;
        }
        this._config.pipeline = this._generateConnectorPipeline([...this._buckets]);
        try {
            if (doUpdate) {
                const timeBeforeUpdate = Date.now();
                this._state.isUpdating = true;
                await this._kafkaConnect.updateConnectorConfig(this._name, this._config);
                this._updateConnectorState(false, timeBeforeUpdate);
                this._state.isUpdating = false;
                return true;
            }
            return false;
        } catch (err) {
            // make sure to trigger the next update in case of error
            this._state.isUpdating = false;
            this._updateConnectorState(true);
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
