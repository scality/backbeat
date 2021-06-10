const { ProbeServer, DEFAULT_LIVE_ROUTE } =
    require('arsenal').network.probe.ProbeServer;

/**
 * Configure probe servers
 * @typedef {Object} ProbeServerConfig
 * @property {string} bindAddress - Address to bind probe server to
 * @property {number} port - Port to bind probe server to
 */

/**
 * Callback when Queue Processor Probe server is listening.
 * Note that a disabled probe server does not pass an error to the callback.
 * @callback DoneCallback
 * @param {Object} [err] - Possible error creating a probe server
 * @param {ProbeServer} [probeServer] - Probe server or undefined if disabled
 */

/**
 * Start probe server for Queue Processor
 * @param {Map<string, QueueProcessor>} queueProcessors - Queue processor
 * @param {ProbeServerConfig} config - Configuration for probe server
 * @param {DoneCallback} [callback] - Callback when probe server is up
 * @returns {undefined}
 */
function startProbeServer(queueProcessors, config, callback) {
    if (process.env.CRR_METRICS_PROBE === 'false' || config === undefined) {
        callback();
        return;
    }
    const probeServer = new ProbeServer(config);
    probeServer.addHandler(
        // for backwards compatibility we also include readiness
        [DEFAULT_LIVE_ROUTE, '/_/health/readiness'],
        (res, log) => {
            // take all our processors and create one liveness response
            let responses = [];
            Object.keys(queueProcessors).forEach(site => {
                const qp = queueProcessors[site];
                responses = responses.concat(qp.handleLiveness(log));
            });
            if (responses.length > 0) {
                return JSON.stringify(responses);
            }
            res.writeHead(200);
            res.end();
            return undefined;
        }
    );
    if (callback) {
        probeServer._cbOnListening = () => callback(probeServer);
        probeServer.onListening(() => callback(null, probeServer));
        probeServer.onError(err => callback(err));
    }
    probeServer.start();
}


/**
 * Get probe config will pull the configuration for the probe server based on
 * the provided site key.
 *
 * @param {Object} queueProcessorConfig - Configuration of the queue processor that
 *      holds the probe server configs for all sites
 * @param {string} site - Name of the site we are processing
 * @returns {ProbeServerConfig|undefined} Config for site or undefined if not found
 */
function getProbeConfig(queueProcessorConfig, site) {
    return queueProcessorConfig &&
        queueProcessorConfig.probeServer &&
        queueProcessorConfig.probeServer.filter(c => c.site === site)[0];
}

module.exports = { startProbeServer, getProbeConfig };
