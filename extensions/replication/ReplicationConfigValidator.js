const fs = require('fs');
const joi = require('joi');
const { hostPortJoi, transportJoi, bootstrapListJoi, adminCredsJoi,
        retryParamsJoi, probeServerJoi } =
    require('../../lib/config/configItems.joi');

const qpRetryJoi = joi.object({
    aws_s3: retryParamsJoi, // eslint-disable-line camelcase
    azure: retryParamsJoi,
    gcp: retryParamsJoi,
    scality: retryParamsJoi,
});

const CRR_FAILURE_EXPIRY = 24 * 60 * 60; // Expire Redis keys after 24 hours.
const OBJECT_SIZE_METRICS = [66560, 8388608, 68157440];

const joiSchema = joi.object({
    source: {
        transport: transportJoi,
        s3: hostPortJoi.required(),
        auth: joi.object({
            type: joi.alternatives().try('account', 'role', 'service').
                required(),
            account: joi.string()
                .when('type', { is: 'account', then: joi.required() })
                .when('type', { is: 'service', then: joi.required() }),
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
            type: joi.alternatives().try('account', 'role', 'service')
                .required(),
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
    },
    topic: joi.string().required(),
    dataMoverTopic: joi.string().required(),
    replicationStatusTopic: joi.string().required(),
    monitorReplicationFailures: joi.boolean().default(true),
    replicationFailedTopic: joi.string().required(),
    monitorReplicationFailureExpiryTimeS:
        joi.number().default(CRR_FAILURE_EXPIRY),
    replayTopics: joi.array().items(
        joi.object({
            topicName: joi.string().required(),
            retries: joi.number().required(),
        })
    ),
    queueProcessor: joi.object({
        groupId: joi.string().required(),
        retry: qpRetryJoi,
        concurrency: joi.number().greater(0).default(10),
        mpuPartsConcurrency: joi.number().greater(0).default(10),
        minMPUSizeMB: joi.number().greater(0).default(20),
        probeServer: probeServerJoi.default(),
    }).required(),
    replicationStatusProcessor: {
        groupId: joi.string().required(),
        retry: retryParamsJoi,
        concurrency: joi.number().greater(0).default(10),
        probeServer: probeServerJoi.default(),
    },
    objectSizeMetrics: joi.array().items(joi.number()).default(OBJECT_SIZE_METRICS),
});

function _loadAdminCredentialsFromFile(filePath) {
    const adminCredsJSON = fs.readFileSync(filePath);
    const adminCredsObj = JSON.parse(adminCredsJSON);
    joi.attempt(adminCredsObj, adminCredsJoi,
                'invalid admin credentials');
    const accessKey = Object.keys(adminCredsObj)[0];
    const secretKey = adminCredsObj[accessKey];
    return { accessKey, secretKey };
}

function configValidator(backbeatConfig, extConfig) {
    const validatedConfig = joi.attempt(extConfig, joiSchema);
    const { source, destination } = validatedConfig;

    if (source.auth.vault) {
        const { adminCredentialsFile } = source.auth.vault;
        if (adminCredentialsFile) {
            source.auth.vault.adminCredentials =
                _loadAdminCredentialsFromFile(adminCredentialsFile);
        }
    }
    if (destination.auth.vault) {
        const { adminCredentialsFile } = destination.auth.vault;
        if (adminCredentialsFile) {
            destination.auth.vault.adminCredentials =
                _loadAdminCredentialsFromFile(adminCredentialsFile);
        }
    }
    return validatedConfig;
}

module.exports = configValidator;
