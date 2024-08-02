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
        const { name, value } = rule;
        const { prefix, suffix } = constants.nameFilter;
        if (name.toLowerCase() === prefix.toLowerCase()) {
            return key.startsWith(value);
        }
        if (name.toLowerCase() === suffix.toLowerCase()) {
            return key.endsWith(value);
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
    return bnConfigs.filter(config => config.events.some(evt => {
        // support wildcard events
        if (evt.endsWith('*')) {
            const starts = evt.replace('*', '');
            return event.toLowerCase().startsWith(starts.toLowerCase());
        }
        return event.toLowerCase() === evt.toLowerCase();
    }));
}

/**
 * Validates an entry from log against bucket notification configurations to see
 * if the entry has to be published. Validations include, bucket specific
 * configuration check, event type check, object name specific filter checks.
 * @param {Object} bnConfig - Bucket notification configuration.
 * @param {Object} entry - An entry from the log.
 * @return {Object} Result with validity boolean and matching configuration rule.
 */
function validateEntry(bnConfig, entry) {
    const { bucket, eventType } = entry;

    if (!eventType) {
        return { isValid: false, matchingConfig: null };
    }

    if (bucket !== bnConfig.bucket) {
        return { isValid: false, matchingConfig: null };
    }

    const notifConf = bnConfig.notificationConfiguration;
    const qConfigs = filterConfigsByEvent(notifConf.queueConfig, eventType);

    if (qConfigs.length > 0) {
        const matchingConfig = qConfigs.find(c => {
            if (!c.filterRules || c.filterRules.length === 0) {
                return true;
            }
            return validateEntryWithFilter(c.filterRules, entry);
        });

        return { isValid: !!matchingConfig, matchingConfig };
    }

    return { isValid: false, matchingConfig: null };
}

module.exports = {
    configArrayToMap,
    validateEntry,
};
