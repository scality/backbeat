'use strict'; // eslint-disable-line

const joi = require('joi');

const hostPortJoi = joi.object({
    host: joi.string().required(),
    port: joi.number().greater(0).required(),
});

const bootstrapListJoi = joi.array().min(1).items(
    joi.object({
        site: joi.string().required(),
        servers: joi.array().min(1).items(joi.string()),
    })
);

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
