const configUtil = require('./config');

/**
 * Last segment of a queue ARN, which is the destination resource name
 *
 * @param {String} queueArn - queue ARN from a bucket notification
 *   configuration
 * @return {String} destination resource name
 */
function destinationOfArn(queueArn) {
    return `${queueArn}`.split(':').pop();
}

/**
 * Find the destinations of a bucket notification configuration that an
 * event matches.
 *
 * This is the per-destination decision the queue processor makes for its
 * single destination, made once for every destination the caller serves:
 * the queue configurations naming a destination are validated against the
 * entry, and the first matching configuration id is kept with it.
 *
 * @param {Object} params - params
 * @param {Object} params.bucketConfig - bucket notification configuration,
 *   as returned by NotificationConfigManager.getConfig
 * @param {Object} params.entry - notification entry: bucket, key, eventType
 * @param {Function} params.isServed - isServed(destinationId) returns true
 *   when the caller delivers to that destination
 * @return {Object[]} matches, as { destinationId, configurationId }, in
 *   the order the destinations appear in the configuration
 */
function matchDestinations(params) {
    const { bucketConfig, entry, isServed } = params;
    const notifConf = bucketConfig && bucketConfig.notificationConfiguration;
    const queueConfigs = (notifConf && notifConf.queueConfig) || [];
    if (queueConfigs.length === 0) {
        return [];
    }
    const seen = new Set();
    const matches = [];
    queueConfigs.forEach(queueConfig => {
        const destinationId = destinationOfArn(queueConfig.queueArn);
        if (seen.has(destinationId) || !isServed(destinationId)) {
            return;
        }
        seen.add(destinationId);
        const destConfig = {
            bucket: entry.bucket,
            notificationConfiguration: {
                queueConfig: queueConfigs.filter(c =>
                    destinationOfArn(c.queueArn) === destinationId),
            },
        };
        const { isValid, matchingConfig } =
            configUtil.validateEntry(destConfig, entry);
        if (isValid) {
            matches.push({
                destinationId,
                configurationId: matchingConfig.id,
            });
        }
    });
    return matches;
}

module.exports = {
    destinationOfArn,
    matchDestinations,
};
