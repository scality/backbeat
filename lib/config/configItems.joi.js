'use strict'; // eslint-disable-line

const joi = require('joi');
const constants = require('../../extensions/replication/constants');
const {
    authTypeAccount,
    authTypeService,
    authTypeAssumeRole,
} = require('../constants');

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

const keyPairJoi = joi.object({
    accessKey: joi.string()
        .when('externalFile', { is: joi.exist(), then: joi.forbidden(), otherwise: joi.required() }),
    secretKey: joi.string()
        .when('externalFile', { is: joi.exist(), then: joi.forbidden(), otherwise: joi.required() }),
    externalFile: joi.string(),
});

const stsConfigJoi = hostPortJoi.concat(keyPairJoi);

const authJoi = joi.object({
    transport: transportJoi,
    type: joi.alternatives().try(
        authTypeService,
        authTypeAccount,
        authTypeAssumeRole
    ).required(),
    account: joi.string()
        .when('type', { is: authTypeAccount, then: joi.required() })
        .when('type', { is: authTypeService, then: joi.required() }),
    roleName: joi.string()
        .when('type', { is: authTypeAssumeRole, then: joi.required() }),
    sts: stsConfigJoi
        .when('type', { is: authTypeAssumeRole, then: joi.required() }),
    vault: hostPortJoi
        .when('type', { is: authTypeAssumeRole, then: joi.required() }),
});

const inheritedAuthJoi = authJoi
    .when('...auth', {
        is: joi.exist(),
        then: joi.optional(),
        otherwise: joi.required(),
    }
);

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

const probeServerJoi = joi.object({
    bindAddress: joi.string().default('localhost'),
    port: joi.number().required(),
});

const mongoJoi = joi.object({
    replicaSetHosts: joi.string().default('localhost:27017'),
    logName: joi.string().default('s3-recordlog'),
    writeConcern: joi.string().default('majority'),
    shardCollections: joi.boolean().default(false),
    replicaSet: joi.when(
        'shardCollections', {
            is: false,
            then: joi.string().default('rs0'),
            otherwise: joi.forbidden(),
        },
    ),
    readPreference: joi.string().default('primary'),
    database: joi.string().default('metadata'),
    authCredentials: joi.object({
        username: joi.string().required(),
        password: joi.string().required(),
    }),
});

module.exports = {
    hostPortJoi,
    transportJoi,
    bootstrapListJoi,
    logJoi,
    adminCredsJoi,
    authJoi,
    inheritedAuthJoi,
    retryParamsJoi,
    certFilePathsJoi,
    probeServerJoi,
    stsConfigJoi,
    mongoJoi,
};
