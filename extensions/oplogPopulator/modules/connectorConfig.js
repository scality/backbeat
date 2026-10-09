const joi = require('joi');
const { v4: uuid } = require('uuid');
const constants = require('../constants');
const OplogPopulatorMetrics = require('../OplogPopulatorMetrics');

// constructor params shared by the connectors managers of both ingestion modes
const connectorsManagerParamsJoi = joi.object({
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

/**
 * Builds the configuration of an oplog source connector, before any
 * pipeline or resume point is set.
 * @param {Object} params params
 * @param {string} params.name connector name
 * @param {string} params.database MongoDB database to watch
 * @param {string} params.mongoUrl MongoDB connection url
 * @param {string} params.oplogTopic topic to produce to
 * @param {number} params.heartbeatIntervalMs heartbeat interval
 * @returns {Object} connector configuration
 */
function buildConnectorConfig({ name, database, mongoUrl, oplogTopic, heartbeatIntervalMs }) {
    return {
        ...constants.defaultConnectorConfig,
        name,
        database,
        'connection.uri': mongoUrl,
        'topic.namespace.map': JSON.stringify({
            '*': oplogTopic,
        }),
        // hearbeat prevents having an outdated resume token in the connectors
        // by constantly updating the offset to the last object in the oplog
        'heartbeat.interval.ms': heartbeatIntervalMs,
    };
}

/**
 * Generates a new `offset.partition.name`: a connector created with a new
 * partition name ignores any offset committed under a previous one, and
 * starts from its configured startup mode
 * @returns {string} partition name
 */
function newPartitionName() {
    return `partition-${uuid()}`;
}

/**
 * Lists the failed tasks of a connector status
 * @param {Object} status connector status, as returned by kafka connect
 * @returns {Object[]} failed tasks
 */
function getFailedTasks(status) {
    return status?.tasks?.filter(task => task.state === 'FAILED') || [];
}

/**
 * Checks whether a connector or any of its tasks failed
 * @param {Object} status connector status, as returned by kafka connect
 * @returns {boolean} true if the connector needs a restart
 */
function isConnectorFailed(status) {
    return status?.connector?.state === 'FAILED' || getFailedTasks(status).length > 0;
}

module.exports = {
    connectorsManagerParamsJoi,
    buildConnectorConfig,
    newPartitionName,
    getFailedTasks,
    isConnectorFailed,
};
