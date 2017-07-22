'use strict'; // eslint-disable-line

const joi = require('joi');
const { hostPortJoi, logJoi, zookeeperJoi } =
          require('../lib/config/configItems.joi.js');

const authJoi = joi.object({
    type: joi.alternatives().try('account', 'role').required(),
    account: joi.string(),
    vault: hostPortJoi,
});

const joiSchema = {
    zookeeper: zookeeperJoi,
    kafka: hostPortJoi.default({
        host: '127.0.0.1',
        port: 9092,
    }),
    log: logJoi,
    extensions: {
        replication: {
            source: {
                s3: hostPortJoi.keys({
                    transport: joi.alternatives().try('http', 'https')
                        .default('https'),
                }).required(),
                auth: authJoi.required(),
                logSource: joi.alternatives()
                    .try('bucketd', 'dmd').required(),
                bucketd: hostPortJoi,
                dmd: hostPortJoi.keys({
                    logName: joi.string().default('s3-recordlog'),
                }),
            },
            destination: {
                s3: hostPortJoi.keys({
                    transport: joi.alternatives().try('http', 'https')
                        .default('https'),
                }).required(),
                auth: authJoi.required(),
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
