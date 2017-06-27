'use strict'; // eslint-disable-line

const joi = require('joi');

const hostPortJoi = joi.object({
    host: joi.string().required(),
    port: joi.number().greater(0).required(),
});

const joiSchema = {
    zookeeper: hostPortJoi.default({
        host: '127.0.0.1',
        port: 2181,
    }),
    kafka: hostPortJoi.default({
        host: '127.0.0.1',
        port: 9092,
    }),
    log: joi.object({
        logLevel: joi.alternatives()
            .try('error', 'warn', 'info', 'debug', 'trace'),
        dumpLevel: joi.alternatives()
            .try('error', 'warn', 'info', 'debug', 'trace'),
    })
        .default({
            logLevel: 'info',
            dumpLevel: 'error',
        }),
    extensions: {
        replication: {
            source: {
                s3: hostPortJoi.required(),
                vault: hostPortJoi.required(),
                logSource: joi.alternatives()
                    .try('bucketd', 'dmd').required(),
                bucketd: hostPortJoi.keys({
                    raftSession: joi.number().required(),
                }),
                dmd: hostPortJoi,
            },
            destination: {
                s3: hostPortJoi.required(),
                vault: hostPortJoi.required(),
                certFilePaths: joi.object({
                    key: joi.string().required(),
                    cert: joi.string().required(),
                    ca: joi.string().empty(''),
                }).required(),
            },
            topic: joi.string().required(),
            groupId: joi.string().required(),
            queuePopulator: {
                cronRule: joi.string().required(),
                batchMaxRead: joi.number().default(10000),
                zookeeperNamespace: joi.string().required(),
            },
            queueProcessor: {
            },
        },
    },
};

module.exports = joiSchema;
