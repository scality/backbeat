'use strict'; // eslint-disable-line

const joi = require('joi');

const hostPortJoi = joi.object().keys({
    host: joi.string().required(),
    port: joi.number().greater(0).required(),
});

const transportJoi = joi.alternatives().try('http', 'https')
    .default('http');

const bootstrapServers = joi.object().keys({
    site: joi.string().required(),
    servers: joi.array().items(joi.string()),
    default: joi.boolean().truthy('yes').falsy('no'),
    echo: joi.boolean().default(false),
});

const bootstrapCloudBackend = joi.object().keys({
    site: joi.string().required(),
    type: joi.string().valid('aws_s3', 'azure'),
    default: joi.boolean().truthy('yes').falsy('no'),
});

const bootstrapListJoi = joi.array()
    .items(bootstrapServers, bootstrapCloudBackend)
    .unique((a, b) => {
        if (a.default === undefined || a.default === false) {
            return false;
        }
        // Cannot have multiple default endpoints truthy
        return a.default === b.default;
    });

const logJoi =
          joi.object({
              logLevel: joi.alternatives()
                  .try('error', 'warn', 'info', 'debug', 'trace'),
              dumpLevel: joi.alternatives()
                  .try('error', 'warn', 'info', 'debug', 'trace'),
          }).default({
              logLevel: 'info',
              dumpLevel: 'error',
          });

const adminCredsJoi = joi.object()
          .min(1)
          .pattern(/^[A-Za-z0-9]{20}$/, joi.string());

const certFilePathsJoi = joi.object({
    key: joi.string().empty(''),
    cert: joi.string().empty(''),
    ca: joi.string().empty(''),
});

module.exports = {
    hostPortJoi,
    transportJoi,
    bootstrapListJoi,
    logJoi,
    adminCredsJoi,
    certFilePathsJoi,
};
