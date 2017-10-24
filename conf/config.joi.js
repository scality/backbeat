'use strict'; // eslint-disable-line

const joi = require('joi');
const { hostPortJoi, bootstrapListJoi, logJoi } =
    require('../lib/config/configItems.joi.js');

const authJoi = joi.object({
    type: joi.alternatives().try('account', 'role').required(),
    account: joi.string(),
    vault: hostPortJoi.keys({
        adminPort: joi.number().greater(0).optional(),
    }),
});

const transportJoi = joi.alternatives().try('http', 'https')
    .default('http');

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
        bucketd: hostPortJoi,
        dmd: hostPortJoi.keys({
            logName: joi.string().default('s3-recordlog'),
        }),
    },
    log: logJoi,
    extensions: {
        replication: {
            source: {
                transport: transportJoi,
                s3: hostPortJoi.required(),
                auth: authJoi.required(),
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
            queueProcessor: {
                groupId: joi.string().required(),
                retryTimeoutS: joi.number().default(300),
                // versioning can support out of order updates
                concurrency: joi.number().greater(0).default(10),
            },
        },
        clueso: {
            // TODO: Handle auth in unified way
            s3: hostPortJoi.keys({
                transport: joi.alternatives().try('http', 'https')
                    .default('http'),
                accessKeyId: joi.string().required(),
                secretKeyId: joi.string().required(),
            }).required(),
            livy: hostPortJoi.keys({
                transport: joi.alternatives().try('http', 'https')
                    .default('http'),
            }).required(),
        },
    },
};

module.exports = joiSchema;
