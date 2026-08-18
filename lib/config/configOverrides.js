'use strict';

/**
 * Generic configuration overrides, from the BACKBEAT_CONFIG_OVERRIDES
 * environment variable.
 *
 * The variable holds a JSON document applied to the configuration as a JSON
 * Merge Patch (RFC 7386): objects are merged recursively, arrays and scalars
 * replace the value they override, and `null` deletes a field, restoring the
 * default the schema defines for it.
 *
 * This is a troubleshooting escape hatch, for the fields no named setting
 * reaches — e.g. the librdkafka parameters of `kafka.producerParams`, whose
 * dotted keys need no escaping here, being plain JSON object keys:
 *
 *     BACKBEAT_CONFIG_OVERRIDES='{"kafka":{"producerParams":{"linger.ms":10}}}'
 *
 * It is applied last, over the configuration file and any named setting, so
 * that nothing silently overrides the hatch someone reached for precisely
 * because the usual path did not work. The result is still validated against
 * the schema, so a typo or a wrong type fails at startup rather than leaving a
 * setting silently ignored.
 */

const { getField } = require('./fields');

const CONFIG_OVERRIDES = 'BACKBEAT_CONFIG_OVERRIDES';

/**
 * @param {*} value - value to test
 * @returns {boolean} true for a JSON object, excluding arrays and null
 */
function isObject(value) {
    return typeof value === 'object' && value !== null && !Array.isArray(value);
}

/**
 * Applies a JSON Merge Patch to a target value, transcribed from the reference
 * pseudocode of RFC 7386, section 2.
 *
 * lodash's `merge()` is deliberately not used: it merges arrays element-wise
 * instead of replacing them, which would leave stale entries behind when
 * overriding a list with a shorter one, and it assigns `null` instead of
 * deleting the field.
 *
 * The target is updated in place when both sides are objects, so that the
 * caller keeps its reference; any other patch replaces it, and the new value is
 * returned.
 *
 * @param {*} target - value to patch
 * @param {*} patch - merge patch to apply
 * @returns {*} patched value
 */
function mergePatch(target, patch) {
    if (!isObject(patch)) {
        return patch;
    }
    if (!isObject(target)) {
        // the patch describes an object: whatever the target held is replaced
        target = {}; // eslint-disable-line no-param-reassign
    }
    Object.entries(patch).forEach(([key, value]) => {
        if (value === null) {
            delete target[key]; // eslint-disable-line no-param-reassign
        } else {
            target[key] = mergePatch(target[key], value); // eslint-disable-line no-param-reassign
        }
    });
    return target;
}

/**
 * Applies the fraction of the BACKBEAT_CONFIG_OVERRIDES merge patch covering
 * the fields of one schema, in place. The patch mirrors the configuration, so
 * that fraction sits at the config path of the schema root.
 *
 * A fraction that is not an object replaces the whole section rather than
 * updating it, and cannot be applied in place: the caller has to use the value
 * returned, and leave the schema to report it if the section is mandatory.
 *
 * @param {Object} config - configuration to update, matching the schema
 * @param {string[]} [prefix] - config path of the schema root
 * @param {Object} [env] - environment to read the overrides from
 * @returns {*} updated configuration
 */
function applyConfigOverrides(config, prefix = [], env = process.env) {
    if (!env[CONFIG_OVERRIDES]) {
        return config;
    }

    let patch;
    try {
        patch = JSON.parse(env[CONFIG_OVERRIDES]);
    } catch (err) {
        throw new Error(`invalid JSON value for ${CONFIG_OVERRIDES}: ${err.message}`);
    }
    if (!isObject(patch)) {
        // any other document would replace the whole configuration instead of
        // updating it in place, which the caller would silently drop
        throw new Error(`${CONFIG_OVERRIDES} must hold a JSON object`);
    }

    // the patch may cover none of the fields of the schema, and then leaves the
    // configuration alone
    const fraction = getField(patch, prefix);
    if (fraction === undefined) {
        return config;
    }

    return mergePatch(config, fraction);
}

module.exports = {
    applyConfigOverrides,
    mergePatch,
};
