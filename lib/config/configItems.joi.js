'use strict'; // eslint-disable-line

const joi = require('@hapi/joi');
const constants = require('../../extensions/replication/constants');

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
    type: joi.string().valid(...constants.replicationBackends),
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

const zenkoAuthJoi = joi.object({
    type: joi.alternatives().try('account', 'service').required(),
    account: joi.string().required(),
});

const retryParamsJoi = joi.object({
    maxRetries: joi.number().default(5),
    timeoutS: joi.number().default(300),
    backoff: joi.object({
        min: joi.number().default(1000),
        max: joi.number().default(300000),
        jitter: joi.number().default(0.1),
        factor: joi.number().default(1.5),
    }),
});

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
    zenkoAuthJoi,
    retryParamsJoi,
    certFilePathsJoi,
};
