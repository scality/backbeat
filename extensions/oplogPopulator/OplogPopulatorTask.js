'use strict'; // eslint-disable-line

const werelogs = require('werelogs');
const { errors } = require('arsenal');

const config = require('../../lib/Config');
const OplogPopulator = require('./OplogPopulator');

const {
    DEFAULT_LIVE_ROUTE,
    DEFAULT_READY_ROUTE,
} = require('arsenal').network.probe.ProbeServer;
const { sendSuccess, sendError } = require('arsenal').network.probe.Utils;
const { startProbeServerPromise } = require('../../lib/util/probe');

const logger = new werelogs.Logger('Backbeat:OplogPopulator');
werelogs.configure({ level: config.log.logLevel,
    dump: config.log.dumpLevel });

const mongoConfig = config.queuePopulator.mongo;
const oplogPopulatorConfig = config.extensions.oplogPopulator;
// Temporary as no extension uses the oplogPopulator for now
const activeExtensions = [];

const oplogPopulator = new OplogPopulator({
    config: oplogPopulatorConfig,
    mongoConfig,
    activeExtensions,
    logger,
});

/**
 * Handle ProbeServer liveness check
 *
 * @param {http.HTTPServerResponse} res - HTTP Response to respond with
 * @param {Logger} log - Logger
 * @returns {undefined}
 */
 function handleLiveness(res, log) {
    if (oplogPopulator.isReady()) {
        sendSuccess(res, log);
    } else {
        log.error('Notification Queue Processor is not ready');
        sendError(res, log, errors.ServiceUnavailable, 'unhealthy');
    }
}

(async () => {
    try {
        await oplogPopulator.setup();
        const probeServer = await startProbeServerPromise(oplogPopulatorConfig.probeServer);
        if (probeServer !== undefined) {
            // following the same pattern as other extensions, where liveness
            // and readiness are handled by the same handler
            probeServer.addHandler([DEFAULT_LIVE_ROUTE, DEFAULT_READY_ROUTE], handleLiveness);
        }
    } catch (error) {
        logger.error('Error when starting up the oplog populator', {
            method: 'OplogPopulatorTask.setup',
            error: error.description || error.message,
        });
        process.exit(0);
    }
})();
