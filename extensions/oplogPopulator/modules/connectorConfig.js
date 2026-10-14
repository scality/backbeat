const { v4: uuid } = require('uuid');
const constants = require('../constants');

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
        'topic.namespace.map': JSON.stringify({ '*': oplogTopic }),
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

module.exports = {
    buildConnectorConfig,
    newPartitionName,
};
