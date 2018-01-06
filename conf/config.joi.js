'use strict'; // eslint-disable-line

const joi = require('joi');
const { hostPortJoi, bootstrapListJoi, logJoi } =
    require('../lib/config/configItems.joi.js');

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
        bucketd: hostPortJoi
            .when('logSource', { is: 'bucketd', then: joi.required() }),
        dmd: hostPortJoi.keys({
            logName: joi.string().default('s3-recordlog'),
        }).when('logSource', { is: 'dmd', then: joi.required() }),
    },
    log: logJoi,
    extensions: {
        replication: {
            source: {
                transport: transportJoi,
                s3: hostPortJoi.required(),
                auth: joi.object({
                    type: joi.alternatives().try('account', 'role').required(),
                    account: joi.string()
                        .when('type', { is: 'account', then: joi.required() }),
                    vault: joi.object({
                        host: joi.string().required(),
                        port: joi.number().greater(0).required(),
                        adminPort: joi.number().greater(0)
                            .when('adminCredentialsFile', {
                                is: joi.exist(),
                                then: joi.required(),
                            }),
                        adminCredentialsFile: joi.string().optional(),
                    }).when('type', { is: 'role', then: joi.required() }),
                }).required(),
            },
            destination: {
                transport: transportJoi,
                auth: joi.object({
                    type: joi.alternatives().try('account', 'role').required(),
                    account: joi.string()
                        .when('type', { is: 'account', then: joi.required() }),
                    vault: joi.object({
                        host: joi.string().optional(),
                        port: joi.number().greater(0).optional(),
                        adminPort: joi.number().greater(0).optional(),
                        adminCredentialsFile: joi.string().optional(),
                    }),
                }).required(),
                bootstrapList: bootstrapListJoi,
                certFilePaths: joi.object({
                    key: joi.string().required(),
                    cert: joi.string().required(),
                    ca: joi.string().empty(''),
                }).required(),
            },
            topic: joi.string().required(),
            replicationStatusTopic: joi.string().required(),
            queueProcessor: {
                groupId: joi.string().required(),
                retryTimeoutS: joi.number().default(300),
                // versioning can support out of order updates
                concurrency: joi.number().greater(0).default(10),
            },
            replicationStatusProcessor: {
                groupId: joi.string().required(),
                retryTimeoutS: joi.number().default(300),
                // versioning can support out of order updates
                concurrency: joi.number().greater(0).default(10),
            },
        },
    },
};

module.exports = joiSchema;
