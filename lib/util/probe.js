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
        callback();
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

/**
 * Get probe config will pull the configuration for the probe server based on
 * the provided site name and topic name. If siteNames is empty, it returns the global probe server config
 * only if it's a single object.
 *
 * @param {Object} config - replication configuration
 * @param {Array<String>} siteNames - List of site names (should contain at most one element)
 * @param {String|undefined} topicName - Topic name if it's a replay processor
 * @param {Logger} logger - Logger instance
 * @returns {Object|undefined} Config for site or global config, undefined if no match found or invalid config
 */
function getReplicationProbeConfig(config, siteNames, topicName, logger) {
    // In S3C, replay processors have a dedicated probeserver config
    // per site and per replay topic.
    let probeConfig = config.queueProcessor.probeServer;
    if (topicName && config.replayProcessor?.probeServer) {
        probeConfig = config.replayProcessor.probeServer;
    }

    if (Array.isArray(probeConfig)) {
        if (siteNames.length > 1) {
            logger.error('Process configured for more than one site or no site provided', {
                siteNames,
                probeConfig,
            });
            return undefined;
        }
        const siteConfig = probeConfig.find(
            config => config.site === siteNames[0] &&
            config.topicName === topicName
        );
        if (siteConfig === undefined) {
            logger.warn('Probe server configuration for site not found', {
                siteName: siteNames[0],
                probeConfig,
            });
        }
        return siteConfig;
    }

    return probeConfig;
}

module.exports = {
    startProbeServer,
    startProbeServerPromise,
    observeKafkaStats,
    getReplicationProbeConfig,
};
