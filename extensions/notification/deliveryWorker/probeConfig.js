// Several delivery workers can run from a single rendered config file, on
// one host or inside one container, and only one process can bind a given
// port. The deployment hands each worker process its own port through this
// environment variable, which wins over the configured one.
const DELIVERY_POOL_PROBE_PORT_ENV = 'DELIVERY_POOL_PROBE_PORT';

const MAX_PORT = 65535;

/**
 * Resolve the probe server configuration of a delivery worker
 *
 * @param {Object} deliveryPoolConfig - delivery pool configuration
 * @param {Object} [env] - environment to read the port override from
 * @param {Logger} [logger] - logger object
 * @return {Object|undefined} probe server configuration, or undefined when
 *   no probe server is configured
 */
function resolveProbeServerConfig(deliveryPoolConfig, env, logger) {
    const probeServer = deliveryPoolConfig && deliveryPoolConfig.probeServer;
    if (!probeServer) {
        return undefined;
    }
    const rawPort = (env || {})[DELIVERY_POOL_PROBE_PORT_ENV];
    if (rawPort === undefined || `${rawPort}`.trim() === '') {
        return probeServer;
    }
    const trimmedPort = `${rawPort}`.trim();
    const port = /^\d+$/.test(trimmedPort) ? Number.parseInt(trimmedPort, 10) : NaN;
    if (!Number.isInteger(port) || port <= 0 || port > MAX_PORT) {
        if (logger) {
            logger.warn('ignoring invalid probe server port from the environment', {
                method: 'resolveProbeServerConfig',
                envVar: DELIVERY_POOL_PROBE_PORT_ENV,
                value: rawPort,
                port: probeServer.port,
            });
        }
        return probeServer;
    }
    return { ...probeServer, port };
}

module.exports = {
    DELIVERY_POOL_PROBE_PORT_ENV,
    resolveProbeServerConfig,
};
