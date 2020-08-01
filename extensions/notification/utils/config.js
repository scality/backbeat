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
            const conf = config.notificationConfiguration.queueConfigurations;
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
 * Validate event against bucket notification configuration event type
 * @param  {Object[]} bnConfigs - Array of bucket specific notification
 * configurations
 * @param  {string} event - Type of event
 * @return {undefined|Object} A bucket notification configuration if found
 */
function validateConfigsEvent(bnConfigs, event) {
    const config = bnConfigs.filter(config => config.events.includes(event));
    return config.length > 0 ? config[0] : undefined;
}

/**
 * Validates an entry from log against bucket notification configurations to see
 * if the entry has to be published. Validations include, bucket specific
 * configuration check, event type check, object name specific filter checks
 * @param  {Object[]} bnConfigs - Array of bucket notifications configuration
 * @param  {Object} entry - An entry from the log
 * @return {boolean} true if event qualifies for notification
 */
function validateEntry(bnConfigs, entry) {
    const { bucket, type } = entry;
    // check if the entry belongs to any bucket in the configuration
    if (bnConfigs.has(bucket)) {
        const bnConfig = bnConfigs.get(bucket);
        // check if the event type matches
        const qConfig = validateConfigsEvent(bnConfig, type);
        if (qConfig) {
            // check if there are filters and the object matches it
            const configFilter = qConfig.filter;
            if (configFilter && configFilter.filterRules) {
                return validateEntryWithFilter(configFilter.filterRules, entry);
            }
            return true;
        }
        return false;
    }
    return false;
}

module.exports = {
    configArrayToMap,
    validateEntry,
};
