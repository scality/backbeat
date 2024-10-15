const util = require('util');
const { ProbeServer } = require('arsenal').network.probe.ProbeServer;
const { ZenkoMetrics } = require('arsenal').metrics;
const RdkafkaStats = require('node-rdkafka-prometheus');
const werelogs = require('werelogs');

const Logger = werelogs.Logger;


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

/**
 * Get probe config will pull the configuration for the probe server based on
 * the provided site name. If siteNames is empty, it returns the global probe server config
 * only if it's a single object.
 *
 * @param {Object} queueProcessorConfig - Configuration of the queue processor that
 *      holds the probe server configs for all sites
 * @param {Array<String>} siteNames - List of site names (should contain at most one element)
 * @returns {Object|undefined} Config for site or global config, undefined if no match found or invalid config
 */
function getProbeConfig(queueProcessorConfig, siteNames) {
    if (siteNames.length === 0) {
        if (!Array.isArray(queueProcessorConfig.probeServer)) {
            return queueProcessorConfig.probeServer;
        }

        Logger.error('Configuration set for specific sites, but no site provided to the process', {
            siteNames,
            queueProcessorConfig,
        });
        return undefined;
    }

    if (Array.isArray(queueProcessorConfig.probeServer)) {
        if (siteNames.length !== 1) {
            Logger.error('Process configured for more than one site', {
                siteNames,
                queueProcessorConfig,
            });
            return undefined;
        }

        const siteConfig = queueProcessorConfig.probeServer.find(config => config.site === siteNames[0]);
        return siteConfig || undefined;
    }

    return undefined;
}

module.exports = {
    startProbeServer,
    startProbeServerPromise,
    observeKafkaStats,
    getProbeConfig,
};
