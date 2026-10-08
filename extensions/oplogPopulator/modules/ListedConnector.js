const constants = require('../constants');
const {
    timestampFromConnectorOffset,
    timestampFromConnectorStartupTime,
    minTimestamp,
} = require('./resumeToken');

/**
 * @class ListedConnector
 *
 * @classdesc A connector as listed by kafka connect, with its configuration
 * and status
 */
class ListedConnector {
    /**
     * @constructor
     * @param {Object} params params
     * @param {string} params.name connector name
     * @param {Object} params.config connector configuration
     * @param {Object} [params.status] connector status, `{ connector, tasks }`
     */
    constructor({ name, config, status }) {
        this.name = name;
        this.config = config;
        this.status = status;
        this.failedTasks = status?.tasks?.filter(task => task.state === 'FAILED') || [];
        // oplog time reached, once stopped
        this.oplogTime = null;
    }

    /**
     * @returns {string} one of `failed`, `paused` (also when stopped), `running`
     */
    get state() {
        const connectorState = this.status?.connector?.state;
        if (connectorState === 'FAILED' || this.failedTasks.length > 0) {
            return 'failed';
        }
        return ['PAUSED', 'STOPPED'].includes(connectorState) ? 'paused' : 'running';
    }

    /**
     * Oplog time reached by the connector: its committed offset, or else the
     * time it was configured to start at, which it never went past without
     * committing, or else shortly before it was stopped
     * @param {Object[]} offsets offsets of the connector, as returned by
     * `KafkaConnectWrapper.getConnectorOffsets()`
     * @param {Object} [stopRequestedAt] cluster time `{ t, i }` read before
     * stopping the connector
     * @returns {Object|null} `{ t, i }`, or null if unknown
     */
    oplogTimeFrom(offsets, stopRequestedAt) {
        const partitionName = this.config['offset.partition.name'];
        // offsets of previous incarnations (other partition names) are stale
        const current = offsets.filter(o => !partitionName || o.partition?.ns === partitionName);
        const committed = minTimestamp(current
            .map(o => timestampFromConnectorOffset(o.offset))
            .filter(ts => ts !== null));
        const configured = timestampFromConnectorStartupTime(
            this.config['startup.mode.timestamp.start.at.operation.time']);
        if (committed || configured) {
            return committed || configured;
        }
        if (!stopRequestedAt) {
            return null;
        }
        // without any offset it never committed, so it started at most a
        // flush interval before it stopped (with some margin on top)
        const marginSeconds = Math.ceil(2 * constants.kafkaConnectOffsetFlushIntervalMs / 1000);
        return { t: stopRequestedAt.t - marginSeconds, i: 0 };
    }
}

module.exports = ListedConnector;
