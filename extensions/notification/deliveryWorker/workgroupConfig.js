// One delivery worker process serves exactly one workgroup. Several workers
// can be rendered from a single config file, on one host or inside one
// container, so the deployment hands each process its workgroup id through
// this environment variable, which wins over the configured one.
const DELIVERY_POOL_WORKGROUP_ID_ENV = 'DELIVERY_POOL_WORKGROUP_ID';

const { WORKGROUP_ID_PATTERN } = require('../utils/workgroups');

/**
 * Resolve the workgroup id this delivery worker serves
 *
 * @param {Object} deliveryPoolConfig - delivery pool configuration
 * @param {Object} [env] - environment to read the override from
 * @param {Logger} [logger] - logger object
 * @return {String|undefined} workgroup id, or undefined when workgroups are
 *   not configured or no id could be resolved
 */
function resolveWorkgroupId(deliveryPoolConfig, env, logger) {
    const workgroups = deliveryPoolConfig && deliveryPoolConfig.workgroups;
    const rawId = (env || {})[DELIVERY_POOL_WORKGROUP_ID_ENV];
    if (!workgroups) {
        if (rawId !== undefined && `${rawId}`.trim() !== '' && logger) {
            logger.warn('ignoring a workgroup id from the environment, ' +
                'workgroups are not configured', {
                method: 'resolveWorkgroupId',
                envVar: DELIVERY_POOL_WORKGROUP_ID_ENV,
                value: rawId,
            });
        }
        return undefined;
    }
    if (rawId === undefined || `${rawId}`.trim() === '') {
        return workgroups.id;
    }
    const trimmedId = `${rawId}`.trim();
    if (!WORKGROUP_ID_PATTERN.test(trimmedId)) {
        if (logger) {
            logger.warn('ignoring invalid workgroup id from the environment', {
                method: 'resolveWorkgroupId',
                envVar: DELIVERY_POOL_WORKGROUP_ID_ENV,
                value: rawId,
                workgroupId: workgroups.id,
            });
        }
        return workgroups.id;
    }
    return trimmedId;
}

module.exports = {
    DELIVERY_POOL_WORKGROUP_ID_ENV,
    resolveWorkgroupId,
};
