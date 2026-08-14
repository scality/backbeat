'use strict';

/**
 * BB-809: generic configuration overrides via BACKBEAT_CONFIG_OVERRIDES.
 *
 * The env var holds a JSON document that is applied to the file config as a
 * JSON Merge Patch (RFC 7386) before joi validation. This is an escape hatch
 * for support/troubleshooting; typed configuration keys remain the supported
 * path for named knobs.
 */

const OVERRIDES_ENV = 'BACKBEAT_CONFIG_OVERRIDES';

function isPlainObject(value) {
    return typeof value === 'object'
        && value !== null
        && !Array.isArray(value);
}

/**
 * Apply a JSON Merge Patch (RFC 7386) to `target` and return the resulting
 * value.
 *
 * Semantics:
 *  - if `patch` is not a plain object, it replaces `target` entirely
 *    (arrays and scalars replace);
 *  - otherwise, for each key in `patch`:
 *      * a `null` value deletes the key from the merged object;
 *      * any other value is applied recursively.
 *
 * The patch is applied in-place on `target` when possible, and always
 * returned so callers can reassign the root value if it was replaced.
 *
 * @param {*} target - value to patch (typically the parsed file config)
 * @param {*} patch  - JSON Merge Patch document
 * @returns {*} merged value
 */
function applyMergePatch(target, patch) {
    if (!isPlainObject(patch)) {
        return patch;
    }

    const base = isPlainObject(target) ? target : {};
    Object.keys(patch).forEach(key => {
        // Guard against prototype pollution: never allow patches to reach
        // Object.prototype through reserved keys.
        if (key === '__proto__' || key === 'constructor' || key === 'prototype') {
            return;
        }
        const patchValue = patch[key];
        if (patchValue === null) {
            delete base[key];
            return;
        }
        base[key] = applyMergePatch(base[key], patchValue);
    });
    return base;
}

/**
 * Parse and apply BACKBEAT_CONFIG_OVERRIDES (if set) on `config` as a JSON
 * Merge Patch. Returns the merged value; the caller is expected to run its
 * joi validation on the result so that unknown keys or wrong types are still
 * rejected at startup.
 *
 * @param {Object} config - the file-based configuration object
 * @param {Object} [env]  - environment map (defaults to process.env), used
 *                          to make the helper trivially testable
 * @returns {Object} merged configuration
 */
function applyConfigOverrides(config, env = process.env) {
    const raw = env[OVERRIDES_ENV];
    if (raw === undefined || raw === '') {
        return config;
    }

    let patch;
    try {
        patch = JSON.parse(raw);
    } catch (err) {
        throw new Error(
            `could not parse ${OVERRIDES_ENV} as JSON: ${err.message}`);
    }

    return applyMergePatch(config, patch);
}

module.exports = {
    OVERRIDES_ENV,
    applyMergePatch,
    applyConfigOverrides,
};
