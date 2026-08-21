'use strict';
/**
 * Configuration overrides from environment variables, with the names of the
 * variables derived from the joi schemas.
 */

const { getField, setField } = require('./fields');

// A container exposes a single probe endpoint, shared by all the probe servers
// of the process it runs: this variable sets the port of every one of them. No
// derived name can stand for it, as a name maps to a single field.
const LIVENESS_PROBE_PORT = 'LIVENESS_PROBE_PORT';

/**
 * Converts a config key to its env var fragment: `batchMaxRead` to
 * `BATCH_MAX_READ`, `minMPUSizeMB` to `MIN_MPU_SIZE_MB`, `aws_s3` to `AWS_S3`.
 *
 * @param {string} key - config key
 * @returns {string} env var fragment
 */
function toSnakeCase(key) {
    return key
        .replace(/([a-z0-9])([A-Z])/g, '$1_$2')
        .replace(/([A-Z]+)([A-Z][a-z])/g, '$1_$2')
        .toUpperCase();
}

/**
 * @param {string[]} path - config path
 * @returns {string} env var name derived from the path
 */
function envVarName(path) {
    return path.map(toSnakeCase).join('_');
}

/**
 * @param {string} value - JSON document
 * @param {string} name - env var it comes from, for error reporting
 * @returns {*} parsed value
 */
function parseJSON(value, name) {
    try {
        return JSON.parse(value);
    } catch (err) {
        throw new Error(`invalid JSON value for ${name}: ${err.message}`);
    }
}

// types joi does not coerce from a string, and which a JSON value can express:
// `null` is an ambiguous type, e.g. a field accepting a string or an array
const JSON_TYPES = ['array', 'object', null];

/**
 * Values are injected as raw strings and coerced by joi, except for the types a
 * string cannot express: booleans accept the usual shell spellings, arrays a
 * comma separated list, and structured values a JSON document.
 *
 * @param {string} value - raw env var value
 * @param {string} [type] - joi type of the field, null when ambiguous
 * @param {string} name - env var name, for error reporting
 * @returns {*} value to inject in the configuration
 */
function coerceValue(value, type, name) {
    if (type === 'boolean') {
        const spelling = value.toLowerCase();
        if (['1', 'y', 'yes', 'on', 'true'].includes(spelling)) {
            return true;
        }
        if (['0', 'n', 'no', 'off', 'false'].includes(spelling)) {
            return false;
        }
        return value;
    }
    if (JSON_TYPES.includes(type) && /^\s*[[{]/.test(value)) {
        return parseJSON(value, name);
    }
    if (type === 'array') {
        return value.split(',').map(item => item.trim());
    }
    return value;
}

/**
 * @param {Object} description - joi description of a node
 * @returns {Object} the annotations of the node, merged
 */
function annotations(description) {
    return Object.assign({}, ...(description.metas || []));
}

/**
 * @param {string[]} names - env var names of the field
 * @param {Object} entry - { path, type, decode } of the field
 * @param {Map} mappings - env var name to entry, updated in place
 * @returns {undefined}
 */
function registerMapping(names, entry, mappings) {
    names.forEach(name => {
        const existing = mappings.get(name);
        if (!existing) {
            mappings.set(name, entry);
        } else if (existing.path.join('.') !== entry.path.join('.')) {
            throw new Error(`env var ${name} maps to both ${existing.path.join('.')} ` +
                            `and ${entry.path.join('.')}`);
        } else if (existing.type !== entry.type) {
            // ambiguous type: inject the raw value and let joi coerce it
            existing.type = null;
        }
    });
}

/**
 * Env var names of a field, from the names of its parent: the `env` annotation
 * renames the segment it contributes, and `envVarAlias` adds a name replacing
 * the path within the schema.
 *
 * @param {Object} description - joi description of the node
 * @param {string[]} parentNames - env var names of the parent node
 * @param {string} root - env var prefix of the schema root
 * @param {string} key - config key of the node
 * @returns {string[]} env var names of the node
 */
function nodeNames(description, parentNames, root, key) {
    const meta = annotations(description);
    const segment = meta.env === undefined ? toSnakeCase(key) : meta.env;
    const names = parentNames.map(prefix => (prefix ? `${prefix}_${segment}` : segment));
    if (meta.envVarAlias) {
        names.push(root ? `${root}_${meta.envVarAlias}` : meta.envVarAlias);
    }
    return names;
}

/**
 * Walks a joi schema description, mapping the env var names of each field to
 * its config path.
 *
 * @param {Object} description - joi description of the node
 * @param {string[]} names - env var names of the node
 * @param {string[]} path - config path of the node, relative to the schema root
 * @param {string} root - env var prefix of the schema root
 * @param {Map} mappings - env var name to { path, type }, updated in place
 * @returns {undefined}
 */
function collectMappings(description, names, path, root, mappings) {
    if (description.flags?.presence === 'forbidden') {
        // a field the schema rejects has no env var
        return;
    }
    switch (description.type) {
        case 'object':
            Object.entries(description.keys || {}).forEach(([key, child]) =>
                collectMappings(child, nodeNames(child, names, root, key),
                                [...path, key], root, mappings));
            break;
        case 'alternatives':
            (description.matches || []).forEach(match =>
                [match.schema, match.then, match.otherwise]
                    .filter(alternative => alternative)
                    .forEach(alternative =>
                        collectMappings(alternative, names, path, root, mappings)));
            break;
        default:
            // arrays are leaves: their items have no name to derive from
            registerMapping(names, {
                path,
                type: description.type,
                decode: annotations(description).envDecodeHook,
            }, mappings);
    }
}

// `describe()` accounts for most of the configuration parsing time, and the
// mappings of a schema never change: they are derived once per process
const mappingsCache = new WeakMap();

/**
 * Maps the env var names a schema supports to the config path they set. The
 * returned map is shared between calls, and must not be modified.
 *
 * @param {joi.Schema} schema - configuration schema
 * @param {string[]} [prefix] - config path of the schema root
 * @returns {Map} env var name to { path, type }
 */
function envVarMappings(schema, prefix = []) {
    const root = envVarName(prefix);
    if (!mappingsCache.has(schema)) {
        mappingsCache.set(schema, new Map());
    }
    const cached = mappingsCache.get(schema);
    if (!cached.has(root)) {
        const mappings = new Map();
        collectMappings(schema.describe(), [root], [], root, mappings);
        cached.set(root, mappings);
    }
    return cached.get(root);
}

/**
 * Sets the port of every probe server of the schema, bound to all interfaces.
 * Sections missing from the configuration are left alone, so that the port is
 * not set on an otherwise unconfigured processor.
 *
 * @param {Object} config - configuration to update
 * @param {Map} mappings - env var name to { path, type }
 * @param {string} port - liveness probe port
 * @returns {undefined}
 */
function applyLivenessProbePort(config, mappings, port) {
    mappings.forEach(({ path }) => {
        if (path[path.length - 1] !== 'port' || path[path.length - 2] !== 'probeServer') {
            return;
        }
        const parent = path.slice(0, -2);
        const section = getField(config, parent);
        // per-site probe servers each have their own port
        if (!section || typeof section !== 'object' || Array.isArray(section.probeServer)) {
            return;
        }
        setField(config, [...parent, 'probeServer'], { bindAddress: '0.0.0.0', port });
    });
}

/**
 * Applies the env vars mapped to the fields of a schema, in place.
 *
 * @param {Object} config - configuration to update, matching the schema
 * @param {joi.Schema} schema - configuration schema
 * @param {string[]} [prefix] - config path of the schema root
 * @param {Object} [env] - environment to read the overrides from
 * @returns {Object} updated configuration. Invalid config returned untouched for joi to report
 */
function applyEnvOverrides(config, schema, prefix = [], env = process.env) {
    if (config === null || typeof config !== 'object') {
        return config;
    }
    const mappings = envVarMappings(schema, prefix);

    if (env[LIVENESS_PROBE_PORT]) {
        applyLivenessProbePort(config, mappings, env[LIVENESS_PROBE_PORT]);
    }

    mappings.forEach(({ path, type, decode }, name) => {
        if (env[name]) {
            // a field with a syntax of its own decodes the value itself: it defers
            // by returning undefined, and throws to reject a value
            const decoded = decode?.(env[name], env);
            setField(config, path, decoded ?? coerceValue(env[name], type, name));
        }
    });

    return config;
}

module.exports = {
    applyEnvOverrides,
    envVarMappings,
    parseJSON,
};
