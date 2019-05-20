'use strict'; // eslint-disable-line

const joi = require('joi');
const { hostPortJoi, transportJoi, logJoi, certFilePathsJoi } =
      require('../lib/config/configItems.joi.js');

const joiSchema = {
    zookeeper: {
        connectionString: joi.string().required(),
        autoCreateNamespace: joi.boolean().default(false),
    },
    kafka: {
        hosts: joi.string().required(),
    },
    queuePopulator: {
        cronRule: joi.string().required(),
        batchMaxRead: joi.number().default(10000),
        zookeeperPath: joi.string().required(),
        logSource: joi.alternatives().try('bucketd', 'dmd').required(),
        bucketd: hostPortJoi
            .keys({ transport: transportJoi })
            .when('logSource', { is: 'bucketd', then: joi.required() }),
        dmd: hostPortJoi.keys({
            logName: joi.string().default('s3-recordlog'),
        }).when('logSource', { is: 'dmd', then: joi.required() }),
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
        name: joi.string().default('backbeat'),
        password: joi.string().default('').allow(''),
        sentinels: joi.array().items(
            joi.object({
                host: joi.string().required(),
                port: joi.number().required(),
            }),
        ),
        sentinelPassword: joi.string().default('').allow(''),
    },
    certFilePaths: certFilePathsJoi,
    internalCertFilePaths: certFilePathsJoi,
};

module.exports = joiSchema;
