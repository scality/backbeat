'use strict'; // eslint-disable-line

const joi = require('joi');
const { hostPortJoi, logJoi } = require('../lib/config/configItems.joi.js');

const transportJoi = joi.alternatives().try('http', 'https')
    .default('http');

const logSourcesJoi = joi.string().valid('bucketd', 'mongo', 'ingestion',
    'dmd');

const joiSchema = {
    zookeeper: {
        connectionString: joi.string().required(),
        autoCreateNamespace: joi.boolean().default(false),
    },
    kafka: {
        hosts: joi.string().required(),
    },
    transport: transportJoi,
    s3: hostPortJoi.required(),
    queuePopulator: {
        cronRule: joi.string().required(),
        batchMaxRead: joi.number().default(10000),
        zookeeperPath: joi.string().required(),

        logSource: joi.alternatives().try(logSourcesJoi).required(),
        subscribeToLogSourceDispatcher: joi.boolean().default(false),
        bucketd: hostPortJoi
            .when('logSource', { is: 'bucketd', then: joi.required() }),
        dmd: hostPortJoi.keys({
            logName: joi.string().default('s3-recordlog'),
        }).when('logSource', { is: 'dmd', then: joi.required() }),
        mongo: joi.object({
            replicaSetHosts: joi.string().default('localhost:27017'),
            logName: joi.string().default('s3-recordlog'),
            writeConcern: joi.string().default('majority'),
            replicaSet: joi.string().default('rs0'),
            readPreference: joi.string().default('primary'),
            database: joi.string().default('metadata'),
        }),
    },
    log: logJoi,
    extensions: joi.object(),
    metrics: {
        topic: joi.string().required(),
    },
    server: {
        healthChecks: joi.object({
            allowFrom: joi.array().items(joi.string()).default([]),
        }).required(),
        host: joi.string().required(),
        port: joi.number().default(8900),
    },
    redis: {
        host: joi.string().default('localhost'),
        port: joi.number().default(6379),
        name: joi.string().default('backbeat'),
        password: joi.string().default('').allow(''),
        sentinels: joi.array().items(
            joi.object({
                host: joi.string().required(),
                port: joi.number().required(),
            })
        ),
    },
};

module.exports = joiSchema;
