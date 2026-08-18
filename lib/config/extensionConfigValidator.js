'use strict';

const joi = require('joi');

const { applyEnvOverrides } = require('./envOverrides');

/**
 * Builds an extension config validator applying the env var overrides derived
 * from the extension schema (e.g. EXTENSIONS_GC_TOPIC for the `topic` field of
 * the gc extension) before validating.
 *
 * The global backbeat configuration is passed as the validation context, so
 * that a field can default to a global one, e.g. `joi.ref('$log.logLevel')`. It
 * is already validated when an extension is: its own defaults are set.
 *
 * It does not carry the extension configurations: those are validated one after
 * the other, so referencing one would resolve differently depending on the
 * order of the configuration file. An extension must not depend on another one.
 *
 * @param {string} extName - extension name, as configured in `extensions`
 * @param {joi.Schema} schema - extension configuration schema
 * @returns {function} extension config validator
 */
function extensionConfigValidator(extName, schema) {
    return (backbeatConfig, extConfig) =>
        joi.attempt(applyEnvOverrides(extConfig, schema, ['extensions', extName]), schema,
                    { context: backbeatConfig });
}

module.exports = { extensionConfigValidator };
