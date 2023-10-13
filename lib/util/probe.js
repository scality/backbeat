const util = require('util');
const { ProbeServer } = require('arsenal').network.probe.ProbeServer;
const { ZenkoMetrics } = require('arsenal').metrics;
const RdkafkaStats = require('node-rdkafka-prometheus');

/**
 * Configure probe servers
 * @typedef {Object} ProbeServerConfig
 * @property {string} bindAddress - Address to bind probe server to
 * @property {number} port - Port to bind probe server to
 */

/**
 * Callback when Probe server is listening.
 * Note that a disabled probe server does not pass an error to the callback.
 * @callback DoneCallback
 * @param {Object} [err] - Possible error creating a probe server
 * @param {ProbeServer} [probeServer] - Probe server or undefined if disabled
 */

/**
 * Start probe server for Queue Processor
 * @param {ProbeServerConfig} config - Configuration for probe server
 * @param {DoneCallback} callback - Callback when probe server is up
 * @returns {undefined}
 */
function startProbeServer(config, callback) {
    if (!config) {
        const err =  new Error('configuration for probe server is missing');
        callback(err);
        return;
    }

    ZenkoMetrics.collectDefaultMetrics();

    const probeServer = new ProbeServer(config);
    probeServer.onListening(() => callback(null, probeServer));
    probeServer.onError(err => callback(err));
    probeServer.start();
}

/**
 * Start probe server for Queue Processor
 * @param {ProbeServerConfig} config - Configuration for probe server
 * @returns {Promise|ProbeServer} Probe server or undefined if disabled
 */
const startProbeServerPromise = util.promisify(startProbeServer);

/**
 * Global kafka metrics
 */
const kafkaMetrics = new RdkafkaStats();

/**
 * Observe rdkafka stats to convert them to prometheus metrics.
 * @param {*} msg Param from rdkafka `events.stats` callback
 * @returns {undefined}
 */
function observeKafkaStats(msg) {
    kafkaMetrics.observe(JSON.parse(msg.message));
}

module.exports = {
    startProbeServer,
    startProbeServerPromise,
    observeKafkaStats,
};
