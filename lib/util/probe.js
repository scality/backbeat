const { ProbeServer } = require('arsenal').network.probe.ProbeServer;

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
    if (process.env.S3_REPLICATION_METRICS_PROBE === 'false' || !config) {
        callback();
        return;
    }
    const probeServer = new ProbeServer(config);
    probeServer.onListening(() => callback(null, probeServer));
    probeServer.onError(err => callback(err));
    probeServer.start();
}

/**
 * Send an Error response with multiple errors
 * @param {http.HTTPServerResponse} res - HTTP response for writing
 * @param {Logger} log - Werelogs instance for logging if you choose to
 * @param {Array} errorMessages - error messages
 * @return {undefined}
 */
function sendMultipleErrors(res, log, errorMessages) {
    const messages = JSON.stringify(errorMessages);
    const errorCode = 500;
    log.error('sending back error response', {
        httpCode: errorCode,
        error: errorMessages,
    });
    res.writeHead(errorCode);
    res.end(messages);
}

/**
 * Send an Error response
 * @param {http.HTTPServerResponse} res - HTTP response for writing
 * @param {Logger} log - Werelogs instance for logging if you choose to
 * @param {Error} error - Error to send back to the user
 * @param {String} [optMessage] - Message to use instead of the errors message
 * @return {undefined}
 */
function sendError(res, log, error, optMessage) {
    const message = optMessage || error.description || '';
    log.error('sending back error response', {
        httpCode: error.code,
        errorType: error.message,
        error: message,
    });
    res.writeHead(error.code);
    res.end(
        JSON.stringify({
            errorType: error.message,
            errorMessage: message,
        }),
    );
}

/**
 * Send a successful HTTP response of 200 OK
 * @param {http.HTTPServerResponse} res - HTTP response for writing
 * @param {Logger} log - Werelogs instance for logging if you choose to
 * @param {String} [message] - Message to send as response, defaults to OK
 * @return {undefined}
 */
function sendSuccess(res, log, message = 'OK') {
    log.debug('replying with success');
    res.writeHead(200);
    res.end(message);
}

module.exports = {
    startProbeServer,
    sendSuccess,
    sendError,
    sendMultipleErrors,
};
