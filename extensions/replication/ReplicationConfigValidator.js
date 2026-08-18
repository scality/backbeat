const fs = require('fs');
const joi = require('joi');
const { hostPortJoi, transportJoi, bootstrapListJoi, adminCredsJoi,
        retryParamsJoi, probeServerJoi, probeServerPerSite, 
        stsConfigJoi } =
    require('../../lib/config/configItems.joi');
const { extensionConfigValidator } = require('../../lib/config/extensionConfigValidator');
const {
    authTypeAccount,
    authTypeAssumeRole,
    authTypeRole,
    authTypeService,
} = require('../../lib/constants');

const { MAX_QUEUED_DEFAULT }  = require('../../lib/constants').backbeatConsumer;

// the historic env var names put the backend before `RETRY`, e.g.
// EXTENSIONS_REPLICATION_QUEUE_PROCESSOR_AWS_S3_RETRY_BACKOFF_MIN
const qpRetryJoi = joi.object({
    // eslint-disable-next-line camelcase
    aws_s3: retryParamsJoi.meta({ envVarAlias: 'QUEUE_PROCESSOR_AWS_S3_RETRY' }),
    azure: retryParamsJoi.meta({ envVarAlias: 'QUEUE_PROCESSOR_AZURE_RETRY' }),
    gcp: retryParamsJoi.meta({ envVarAlias: 'QUEUE_PROCESSOR_GCP_RETRY' }),
    scality: retryParamsJoi,
});

const CRR_FAILURE_EXPIRY = 24 * 60 * 60; // Expire Redis keys after 24 hours.
const OBJECT_SIZE_METRICS = [66560, 8388608, 68157440];
// Floor matches librdkafka's default session.timeout.ms (45s): kafka refuses to
// start a consumer whose max.poll.interval.ms is below the session timeout.
const MAX_POLL_INTERVAL_MS_MIN = 45000;
// Ceiling of 30 minutes: this knob lets a single slow large-MPU transfer
// finish, not silence a wedged consumer. A poll gap past 30 min means the task
// is stuck - let kafka evict and redeliver rather than hide the failure behind
// an ever-growing timeout.
const MAX_POLL_INTERVAL_MS_MAX = 1800000;

const destinationAuthJoi = joi.object({
    type: joi.alternatives().try(
        authTypeAccount,
        authTypeRole,
        authTypeService,
        authTypeAssumeRole
    ).required(),
    account: joi.string()
        .when('type', { is: authTypeAccount, then: joi.required() }),
    vault: joi.object({
        host: joi.string().optional(),
        port: joi.number().greater(0).optional(),
        adminPort: joi.number().greater(0).optional(),
        adminCredentialsFile: joi.string().optional(),
    }),
    sts: stsConfigJoi
        .keys({
            // override port to make it optional, with default based on transport
            port: joi.alternatives().try(
                stsConfigJoi.extract('port'),
                joi.string().optional().valid('', null).empty(['', null])
            ).default(joi.ref('....transport', { adjust: transport => transport === 'http' ? 80 : 443 })),
        })
        .when('type', { is: authTypeAssumeRole, then: joi.required() }),
});

const joiSchema = joi.object({
    source: {
        transport: transportJoi,
        s3: hostPortJoi.required(),
        auth: joi.object({
            type: joi.alternatives().try(
                authTypeAccount,
                authTypeRole,
                authTypeService,
            ).required(),
            account: joi.string()
                .when('type', { is: authTypeAccount, then: joi.required() })
                .when('type', { is: authTypeService, then: joi.required() }),
            vault: joi.object({
                host: joi.string().required(),
                port: joi.number().greater(0).required(),
                adminPort: joi.number().greater(0)
                    .when('adminCredentialsFile', {
                        is: joi.exist(),
                        then: joi.required(),
                    }),
                adminCredentialsFile: joi.string().optional(),
            }).when('type', { is: authTypeRole, then: joi.required() }),
        }).required(),
    },
    destination: joi.object({
        transport: transportJoi,
        auth: destinationAuthJoi.optional(),
        sites: joi.object().pattern(
            joi.string(), // site name
            joi.object({
                transport: transportJoi,
                auth: destinationAuthJoi,
            }).required()
        ).when('auth', {
            is: joi.exist(),
            then: joi.optional(),
            otherwise: joi.required(),
        }),
        bootstrapList: bootstrapListJoi,
    }).required().custom(_validatePerSiteDestinationConfig).meta({ env: 'DEST' }),
    topic: joi.string().required(),
    dataMoverTopic: joi.string().optional(),
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
        maxPollIntervalMs: joi.number()
            .min(MAX_POLL_INTERVAL_MS_MIN)
            .max(MAX_POLL_INTERVAL_MS_MAX),
        mpuPartsConcurrency: joi.number().greater(0).default(10),
        maxQueued: joi.number().greater(0).default(MAX_QUEUED_DEFAULT),
        minMPUSizeMB: joi.number().greater(0).default(20),
        probeServer: joi.alternatives().try(
            probeServerJoi,
            probeServerPerSite,
        ).default({ bindAddress: 'localhost', port: 4042 }),
        circuitBreaker: joi.object().optional(),
        sourceCheckIfSizeGreaterThanMB: joi.number().positive().default(100),
    }).required(),
    replicationStatusProcessor: joi.object({
        groupId: joi.string().required(),
        retry: retryParamsJoi,
        concurrency: joi.number().greater(0).default(10),
        maxQueued: joi.number().greater(0).default(MAX_QUEUED_DEFAULT),
        probeServer: probeServerJoi.default(),
    }).meta({ env: 'STATUS_PROCESSOR' }),
    replayProcessor: joi.object({
        probeServer: probeServerPerSite,
    }).optional(),
    objectSizeMetrics: joi.array().items(joi.number()).default(OBJECT_SIZE_METRICS),
});

/**
 * When using per site configuration, validate that there is an auth
 * and transport configuration for each site in the bootstrap list.
 * @param {Object} authConfig - auth config
 * @param {joi.CustomHelpers} helpers  - joi helpers
 * @returns {Object|joi.ErrorReport} - auth config when validated or a joi error
 */
function _validatePerSiteDestinationConfig(destination, helpers) {
    // destination.auth and destination.transport is used as a default for all sites
    // no need to check destination.transport as it has a default value and is always set.
    if (destination.auth) {
        return destination;
    }
    const missingConfigs = [];
    destination.bootstrapList.forEach(b => {
        if (!destination.sites?.[b.site]?.auth) {
            missingConfigs.push(b.site);
        }
    });
    if (missingConfigs.length > 0) {
        return helpers.message({
            custom: `missing destination configuration for sites: ${missingConfigs.join(',')}`
        });
    }
    return destination;
}

function _loadAdminCredentialsFromFile(filePath) {
    const adminCredsJSON = fs.readFileSync(filePath);
    const adminCredsObj = JSON.parse(adminCredsJSON);
    joi.attempt(adminCredsObj, adminCredsJoi,
                'invalid admin credentials');
    const accessKey = Object.keys(adminCredsObj)[0];
    const secretKey = adminCredsObj[accessKey];
    return { accessKey, secretKey };
}

const validateConfig = extensionConfigValidator('replication', joiSchema);

function configValidator(backbeatConfig, extConfig) {
    const validatedConfig = validateConfig(backbeatConfig, extConfig);
    const { source, destination } = validatedConfig;
    if (source.auth.vault) {
        const { adminCredentialsFile } = source.auth.vault;
        if (adminCredentialsFile) {
            source.auth.vault.adminCredentials =
                _loadAdminCredentialsFromFile(adminCredentialsFile);
        }
    }
    if (destination.auth?.vault) {
        const { adminCredentialsFile } = destination.auth.vault;
        if (adminCredentialsFile) {
            destination.auth.vault.adminCredentials =
                _loadAdminCredentialsFromFile(adminCredentialsFile);
        }
    }
    if (destination.sites) {
        Object.values(destination.sites).forEach(site => {
            if (site.auth?.vault) {
                const { adminCredentialsFile } = site.auth.vault;
                if (adminCredentialsFile) {
                    // eslint-disable-next-line no-param-reassign
                    site.auth.vault.adminCredentials =
                        _loadAdminCredentialsFromFile(adminCredentialsFile);
                }
            }
        });
    }
    return validatedConfig;
}

module.exports = configValidator;
