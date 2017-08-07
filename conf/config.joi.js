'use strict'; // eslint-disable-line

const joi = require('joi');
const { hostPortJoi, bootstrapListJoi, logJoi, zookeeperJoi } =
          require('../lib/config/configItems.joi.js');

const authJoi = joi.object({
    type: joi.alternatives().try('account', 'role').required(),
    account: joi.string(),
    vault: hostPortJoi,
});

const transportJoi = joi.alternatives().try('http', 'https')
    .default('http');

const joiSchema = {
    zookeeper: zookeeperJoi,
    kafka: {
        hosts: joi.string().required(),
    },
    log: logJoi,
    extensions: {
        replication: {
            source: {
                transport: transportJoi,
                s3: hostPortJoi.required(),
                auth: authJoi.required(),
                logSource: joi.alternatives().try('bucketd', 'dmd').required(),
                bucketd: hostPortJoi,
                dmd: hostPortJoi.keys({
                    logName: joi.string().default('s3-recordlog'),
                }),
            },
            destination: {
                transport: transportJoi,
                auth: authJoi.required(),
                bootstrapList: bootstrapListJoi,
                certFilePaths: joi.object({
                    key: joi.string().required(),
                    cert: joi.string().required(),
                    ca: joi.string().empty(''),
                }).required(),
            },
            topic: joi.string().required(),
            queuePopulator: {
                cronRule: joi.string().required(),
                batchMaxRead: joi.number().default(10000),
                zookeeperPath: joi.string().required(),
            },
            queueProcessor: {
                groupId: joi.string().required(),
            },
        },
    },
};

module.exports = joiSchema;
