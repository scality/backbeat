const constants = require('../constants');

/**
 * Transforms array of configurations to a Map
 * @param  {Object[]} configs - Array of bucket notification configurations
 * @return {Map} Map of bucket notification configurations
 */
function transform(configs) {
    const configMap = new Map();
    configs.forEach(config => {
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
 * Validate object name against bucket notification configuration filters
 * @param  {Object[]} filterRules - Filter rules from the bucket notification
 * configuration.
 * @param  {string} objectName - Name of the object
 * @return {boolean} true if filter matches the object name
 */
function validateFilter(filterRules, objectName) {
    return filterRules.some(rule => {
        const name = rule.name;
        const value = rule.value;
        if (name === constants.nameFilter.prefix) {
            return objectName.startsWith(value);
        }
        if (name === constants.nameFilter.suffix) {
            return objectName.endsWith(value);
        }
        return false;
    });
}

/**
 * Validate event against bucket notification configuration event type
 * @param  {Object[]} bnConfig - Array of bucket specific notification
 * configurations
 * @param  {string} event - Type of event
 * @return {undefined|Object} A bucket notification configuration if found
 */
function validateEvent(bnConfig, event) {
    if (bnConfig.length) {
        const config = bnConfig.filter(config => config.events.includes(event));
        return config.length ? config[0] : undefined;
    }
    return undefined;
}

/**
 * Validates an entry from log against bucket notification configurations to see
 * if the entry has to be published. Validations include, bucket specific
 * configuration check, event type check, object name specific filter checks
 * @param  {Object[]} bnConfigs - Array of bucket notifications configuration
 * @param  {Object} entry - Event object from the logs
 * @return {boolean} true if event qualifies for notification
 */
function validateEntry(bnConfigs, entry) {
    if (entry.bucket && entry.key && entry.type) {
        const bucket = entry.bucket;
        const event = entry.type;
        const key = entry.key;
        // check if the entry belongs to any bucket in the configuration!
        if (bnConfigs.has(bucket)) {
            const bnConfig = bnConfigs.get(bucket);
            // check if the event type matches!
            const qConfig = validateEvent(bnConfig, event);
            if (qConfig) {
                // check if there are filters and the object matches it!
                if (qConfig.filter && qConfig.filter.filterRules) {
                    return validateFilter(qConfig.filter.filterRules, key);
                }
                return true;
            }
            return false;
        }
    }
    return false;
}

module.exports = {
    transform,
    validateEntry,
};
