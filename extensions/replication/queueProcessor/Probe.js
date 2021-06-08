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
 * @param {QueueProcessor} queueProcessor - Queue processor
 * @param {ProbeServerConfig} config - Configuration for probe server
 * @param {DoneCallback} callback - Callback when probe server is up
 * @returns {undefined}
 */
function startProbeServer(queueProcessor, config, callback) {
    if (process.env.CRR_METRICS_PROBE !== 'true' || config === undefined) {
        callback();
        return;
    }
    const probeServer = new ProbeServer(config);
    probeServer.addHandler(
        DEFAULT_LIVE_ROUTE,
        (res, log) => queueProcessor.handleLiveness(res, log)
    );
    probeServer.onListening(() => callback(undefined, probeServer));
    probeServer.onError(err => callback(err));
    probeServer.start();
}

module.exports = { startProbeServer };
