const { ProbeServer, DEFAULT_LIVE_ROUTE } =
    require('arsenal').network.probe.ProbeServer;

/**
 * Configure probe servers
 * @typedef {Object} ProbeServerConfig
 * @property {string} bindAddress - Address to bind probe server to
 * @property {number} port - Port to bind probe server to
 */

/**
 * Callback when Queue Processor Probe server is listening
 * @callback DoneCallback
 * @param {ProbeServer} [probeServer] - Probe server or undefined if disabled
 */

/**
 * Start probe server for Queue Processor
 * @param {QueueProcessor} queueProcessor - Queue processor
 * @param {ProbeServerConfig} config - Configuration for probe server
 * @param {DoneCallback} callback - Callback when probe server is up
 */
function startProbeServer(queueProcessor, config, callback) {
    if (process.env.CRR_METRICS_PROBE === 'false' || config === undefined) {
        callback();
        return;
    }
    const probeServer = new ProbeServer(config);
    probeServer.addHandler(
        DEFAULT_LIVE_ROUTE,
        (res, log) => queueProcessor.handleLiveness(res, log)
    );
    probeServer._cbOnListening = () => callback(probeServer);
    probeServer.start();
    return undefined;
}

module.exports = { startProbeServer };
