'use strict'; // eslint-disable-line

const joi = require('joi');
const { hostPortJoi, logJoi } = require('../lib/config/configItems.joi.js');

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

        logSource: joi.alternatives().try('bucketd', 'dmd', 'mongo').required(),
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
};

module.exports = joiSchema;
