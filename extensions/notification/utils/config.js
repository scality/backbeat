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
 * @param  {Object[]} queueConfig - Array of bucket specific queue
 * configurations
 * @param  {string} eventType - Type of event
 * @return {undefined|Object[]} Bucket queue configurations if found
 */
function filterConfigsByEvent(queueConfig, eventType) {
    // only return queue configs valid for the event type
    return queueConfig.filter(config =>
        // a queue config can have multiple valid event types
        // this returns true when at least one event is the same as eventType
        config.events.some(evt => {
            // support wildcard events
            if (evt.endsWith('*')) {
                const starts = evt.replace('*', '');
                return eventType.toLowerCase().startsWith(starts.toLowerCase());
            }
            return eventType.toLowerCase() === evt.toLowerCase();
        })
    );
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
    const eventType = entry.eventType;
    /**
     * if the event type is unavailable, it is an entry that is a
     * placeholder for deletion or cleanup, these entries should be ignored and
     * not be processed.
    */
    if (!eventType) {
        return false;
    }
    // check if the event type matches at least one supported event from
    // one of the queue configurations
    const qConfigs = filterConfigsByEvent(bnConfig.queueConfig, eventType);
    if (qConfigs.length > 0) {
        // get queue configurations that have filters
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
