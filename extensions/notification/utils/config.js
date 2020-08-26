const constants = require('../constants');

/**
 * Transforms array of configurations to a Map
 * @param  {Object[]} bnConfigs - Array of bucket notification configurations
 * @return {Map} Map of bucket notification configurations
 */
function configArrayToMap(bnConfigs) {
    const configMap = new Map();
    bnConfigs.forEach(config => {
        if (config.bucket && config.notificationConfiguration) {
            const bucket = config.bucket;
            const conf = config.notificationConfiguration.queueConfig;
            if (configMap.has(bucket)) {
                const val = configMap.get(bucket);
                configMap.set(bucket, [...val, ...conf]);
            } else {
                configMap.set(bucket, [...conf]);
            }
        }
    });
    return configMap;
}

/**
 * Validate object bucket notification configuration filters
 * @param  {Object[]} filterRules - Filter rules from the bucket notification
 * configuration.
 * @param  {Object} entry - An entry from the log
 * @return {boolean} true if filter matches the object name
 */
function validateEntryWithFilter(filterRules, entry) {
    const { key } = entry;
    return filterRules.every(rule => {
        if (rule.name === constants.nameFilter.prefix) {
            return key.startsWith(rule.value);
        }
        if (rule.name === constants.nameFilter.suffix) {
            return key.endsWith(rule.value);
        }
        return false;
    });
}

/**
 * Validate event against bucket queue configuration event type
 * @param  {Object[]} bnConfigs - Array of bucket specific queue
 * configurations
 * @param  {string} event - Type of event
 * @return {undefined|Object[]} Bucket queue configurations if found
 */
function filterConfigsByEvent(bnConfigs, event) {
    return bnConfigs.filter(config => config.events.includes(event));
}

/**
 * Validates an entry from log against bucket notification configurations to see
 * if the entry has to be published. Validations include, bucket specific
 * configuration check, event type check, object name specific filter checks
 * @param  {Object} bnConfig - Bucket notification configuration
 * @param  {Object} entry - An entry from the log
 * @return {boolean} true if event qualifies for notification
 */
function validateEntry(bnConfig, entry) {
    const { bucket, type } = entry;
    const notifConf = bnConfig.notificationConfiguration;
    // check if the entry belongs to the bucket in the configuration
    if (bucket !== bnConfig.bucket) {
        return false;
    }
    // check if the event type matches
    const qConfigs = filterConfigsByEvent(notifConf.queueConfig, type);
    if (qConfigs.length > 0) {
        const qConfigWithFilters
            = qConfigs.filter(c => c.filterRules && c.filterRules.length > 0);
        // if there are configs without filters, make the entry valid
        if (qConfigs.length > qConfigWithFilters.length) {
            return true;
        }
        return qConfigWithFilters.some(
            c => validateEntryWithFilter(c.filterRules, entry));
    }
    return false;
}

module.exports = {
    configArrayToMap,
    validateEntry,
};
