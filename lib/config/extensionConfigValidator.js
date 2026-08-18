'use strict';

const joi = require('joi');

const { applyEnvOverrides } = require('./envOverrides');

/**
 * Builds an extension config validator applying the env var overrides derived
 * from the extension schema (e.g. EXTENSIONS_GC_TOPIC for the `topic` field of
 * the gc extension) before validating.
 *
 * @param {string} extName - extension name, as configured in `extensions`
 * @param {joi.Schema} schema - extension configuration schema
 * @returns {function} extension config validator
 */
function extensionConfigValidator(extName, schema) {
    return (backbeatConfig, extConfig) =>
        joi.attempt(applyEnvOverrides(extConfig, schema, ['extensions', extName]), schema);
}

module.exports = { extensionConfigValidator };
