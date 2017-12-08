'use strict'; // eslint-disable-line

const joi = require('joi');

const hostPortJoi = joi.object({
    host: joi.string().required(),
    port: joi.number().greater(0).required(),
});

const bootstrapServers = joi.object({
    site: joi.string().required(),
    servers: joi.array().items(joi.string()),
});

const bootstrapCloudBackend = joi.object({
    site: joi.string().required(),
    type: joi.string().valid('aws_s3', 'azure'),
});

const bootstrapListJoi = joi.array().items(
    bootstrapServers, bootstrapCloudBackend);

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

module.exports = {
    hostPortJoi,
    bootstrapListJoi,
    logJoi,
};
