const joi = require('joi');
const uuid = require('uuid');
const { errors } = require('arsenal');
const { EventEmitter } = require('stream');
const KafkaConnectWrapper = require('../../../lib/wrappers/KafkaConnectWrapper');
const constants = require('../constants');

const connectorParams = joi.object({
    name: joi.string().required(),
    config: joi.object().required(),
    buckets: joi.array().required(),
    getPipeline: joi.func().required(),
    isRunning: joi.boolean().required(),
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
class Connector extends EventEmitter {

    /**
     * @constructor
     * @param {Object} params connector config
     * @param {string} params.name connector name
     * @param {Boolean} params.isRunning true if connector is running
     * @param {Object} params.config Kafka-connect MongoDB source
     * connector config
     * @param {string[]} params.buckets buckets assigned to this connector
     * @param {boolean} params.getPipeline callback to get connector pipeline
     * @param {Logger} params.logger logger object
     * @param {string} params.kafkaConnectHost kafka connect host
     * @param {number} params.kafkaConnectPort kafka connect port
     */
    constructor(params) {
        super();
        joi.attempt(params, connectorParams);
        this._name = params.name;
        this._config = params.config;
        this._buckets = new Set(params.buckets);
        this._isRunning = params.isRunning;
        this._getPipeline = params.getPipeline;
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
     * Get connector config
     * @returns {Object} connector config
     */
    get config() { return this._config; }

    /**
     * Getter for connector running state
     * @returns {Boolean} connector running state
     */
    get isRunning() { return this._isRunning; }

    /**
     * Calculate config size in bytes
     * @returns {number} config size
     */
    getConfigSizeInBytes() {
        try {
            const configSize = Buffer.byteLength(JSON.stringify(this._config));
            return configSize;
        } catch (err) {
            this._logger.error('Error while calculating config size', {
                method: 'Connector.getConfigSizeInBytes',
                connector: this._name,
                error: err.description || err.message,
            });
            throw errors.InternalError.customizeDescription(err.description);
        }
    }

    /**
     * Updates partition name in connector config
     * @returns {undefined}
     */
    updatePartitionName() {
        this._config['offset.partition.name'] = `partition-${uuid.v4()}`;
    }

    /**
     * Sets the resume point of the change stream
     * to the first event of the first bucket added
     * to the connector.
     * @param {Date} eventDate oplog event date
     * @returns {undefined}
    */
    setResumePoint(eventDate) {
        if (this._config['startup.mode.timestamp.start.at.operation.time']) {
            return;
        }

        this._config['startup.mode.timestamp.start.at.operation.time'] = eventDate.toISOString();

        this._logger.info('Connector resume point updated', {
            method: 'Connector.updateResumeDate',
            date: eventDate.toISOString(),
            connector: this._name,
        });
    }

    /**
     * Creates the Kafka-connect mongo connector
     * @returns {Promise|undefined} undefined
     * @throws {InternalError}
     */
    async spawn() {
        if (this._isRunning) {
            this._logger.error('tried spawning an already created connector', {
                method: 'Connector.spawn',
                connector: this._name,
            });
            return;
        }
        // reset resume token to avoid getting outdated token
        this.updatePartitionName();
        try {
            await this._kafkaConnect.createConnector({
                name: this._name,
                config: this._config,
            });
            this._isRunning = true;
        } catch (err) {
            this._logger.error('Error while spawning connector', {
                method: 'Connector.spawn',
                connector: this._name,
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
        if (!this._isRunning) {
            this._logger.error('tried destroying an already destroyed connector', {
                method: 'Connector.destroy',
                connector: this._name,
            });
            return;
        }
        try {
            this.emit(constants.connectorUpdatedEvent, this);
            await this._kafkaConnect.deleteConnector(this._name);
            this._isRunning = false;
            // resetting the resume point to set a new one on creation of the connector
            delete this._config['startup.mode.timestamp.start.at.operation.time'];
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
     * Restarts the Kafka-connect mongo connector
     * @returns {Promise|undefined} undefined
     * @throws {InternalError}
     */
    async restart() {
        if (!this._isRunning) {
            this._logger.error('tried restarting a destroyed connector', {
                method: 'Connector.restart',
                connector: this._name,
            });
            return;
        }
        try {
            // only restarting failed instances of tasks and connector
            await this._kafkaConnect.restartConnector(this._name, true, true);
        } catch (err) {
            this._logger.error('Error while restarting connector', {
                method: 'Connector.restart',
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
        this._config.pipeline = this._getPipeline([...this._buckets]);
        try {
            if (doUpdate && this._isRunning) {
                const timeBeforeUpdate = Date.now();
                this._state.isUpdating = true;
                this.emit(constants.connectorUpdatedEvent, this);
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
