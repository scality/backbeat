'use strict'; // eslint-disable-line

const { EventEmitter } = require('events');

const fs = require('fs');
const path = require('path');
const joi = require('@hapi/joi');
const crypto = require('crypto');

const extensions = require('../extensions');
const backbeatConfigJoi = require('./config.joi.js');

// NOTE: currently only s3 connector (s3c 7.4.3 and above) are supported
const ingestionTypeMatch = {
    'location-scality-ring-s3-v1': 'scality_s3',
};

// from cloudserver lib/Config.js:
const defaultExternalBackendsConfig = {
    // eslint-disable-next-line camelcase
    aws_s3: {
        httpAgent: {
            keepAlive: false,
            keepAliveMsecs: 1000,
            maxFreeSockets: 256,
            maxSockets: null,
        },
    },
    gcp: {
        httpAgent: {
            keepAlive: true,
            keepAliveMsecs: 1000,
            maxFreeSockets: 256,
            maxSockets: null,
        },
    },
};

class Config extends EventEmitter {
    constructor() {
        super();
        /*
         * By default, the config file is "config.json" at the root.
         * It can be overridden using the BACKBEAT_CONFIG_FILE environment var.
         */
        this._basePath = `${__dirname}/../conf`;
        if (process.env.BACKBEAT_CONFIG_FILE !== undefined) {
            this._configPath = process.env.BACKBEAT_CONFIG_FILE;
        } else {
            this._configPath = path.join(this._basePath, 'config.json');
        }

        let config;
        try {
            const data = fs.readFileSync(this._configPath,
                                         { encoding: 'utf-8' });
            config = JSON.parse(data);
        } catch (err) {
            throw new Error(`could not parse config file: ${err.message}`);
        }

        const parsedConfig = joi.attempt(config, backbeatConfigJoi,
                                         'invalid backbeat config');

        if (parsedConfig.extensions) {
            Object.keys(parsedConfig.extensions).forEach(extName => {
                const index = extensions[extName];
                if (!index) {
                    throw new Error(`configured extension ${extName}: ` +
                                    'not found in extensions directory');
                }
                if (index.configValidator) {
                    const extConfig = parsedConfig.extensions[extName];
                    const validatedConfig =
                              index.configValidator(this, extConfig);
                    parsedConfig.extensions[extName] = validatedConfig;
                }
            });
        }

        if (parsedConfig.extensions && parsedConfig.extensions.replication
            && parsedConfig.extensions.replication.destination
            && parsedConfig.extensions.replication.destination.bootstrapList) {
            this.bootstrapList = parsedConfig.extensions.replication
            .destination.bootstrapList;
        } else {
            this.bootstrapList = [];
        }

        // NOTE: used to store ingestion bucket information
        this.ingestionBuckets = [];

        // whitelist IP, CIDR for health checks
        const defaultHealthChecks = ['127.0.0.1/8', '::1'];
        const healthChecks = parsedConfig.server.healthChecks;
        healthChecks.allowFrom =
            healthChecks.allowFrom.concat(defaultHealthChecks);

        if (parsedConfig.redis &&
          typeof parsedConfig.redis.sentinels === 'string') {
            const redisConf = { sentinels: [], name: parsedConfig.redis.name };
            parsedConfig.redis.sentinels.split(',').forEach(item => {
                const [host, port] = item.split(':');
                redisConf.sentinels.push({ host,
                    port: Number.parseInt(port, 10) });
            });
            parsedConfig.redis = redisConf;
        }

        // default to standalone configuration if sentinel not setup
        if (!parsedConfig.redis || !parsedConfig.redis.sentinels) {
            this.redis = Object.assign({}, parsedConfig.redis,
                { host: '127.0.0.1', port: 6379 });
        }

        if (parsedConfig.localCache) {
            this.localCache = {
                host: config.localCache.host,
                port: config.localCache.port,
                password: config.localCache.password,
            };
        }

        // additional certs checks
        if (parsedConfig.certFilePaths) {
            parsedConfig.https = this._parseCertFilePaths(
                parsedConfig.certFilePaths);
        }
        if (parsedConfig.internalCertFilePaths) {
            parsedConfig.internalHttps = this._parseCertFilePaths(
                parsedConfig.internalCertFilePaths);
        }

        if (process.env.MONGODB_HOSTS) {
            parsedConfig.queuePopulator.mongo.replicaSetHosts =
                process.env.MONGODB_HOSTS;
        }
        if (process.env.MONGODB_RS) {
            parsedConfig.queuePopulator.mongo.replicatSet =
                process.env.MONGODB_RS;
        }
        if (process.env.MONGODB_DATABASE) {
            parsedConfig.queuePopulator.mongo.database =
                process.env.MONGODB_DATABASE;
        }
        if (process.env.MONGODB_AUTH_USERNAME &&
            process.env.MONGODB_AUTH_PASSWORD) {
            parsedConfig.queuePopulator.mongo.authCredentials = {
                username: process.env.MONGODB_AUTH_USERNAME,
                password: process.env.MONGODB_AUTH_PASSWORD,
            };
        }
        // config is validated, safe to assign directly to the config object
        Object.assign(this, parsedConfig);

        this.locationConstraints = {};
        this.transientLocations = {};

        this.externalBackends = defaultExternalBackendsConfig;
        this.outboundProxy = {};
    }

    _parseCertFilePaths(certFilePaths) {
        const { key, cert, ca } = certFilePaths;

        const makePath = value =>
              (value.startsWith('/') ?
               value : `${this._basePath}/${value}`);
        const https = {};
        if (key && cert) {
            const keypath = makePath(key);
            const certpath = makePath(cert);
            fs.accessSync(keypath, fs.F_OK | fs.R_OK);
            fs.accessSync(certpath, fs.F_OK | fs.R_OK);
            https.cert = fs.readFileSync(certpath, 'ascii');
            https.key = fs.readFileSync(keypath, 'ascii');
        }
        if (ca) {
            const capath = makePath(ca);
            fs.accessSync(capath, fs.F_OK | fs.R_OK);
            https.ca = fs.readFileSync(capath, 'ascii');
        }
        return https;
    }

    getBasePath() {
        return this._basePath;
    }

    getConfigPath() {
        return this._configPath;
    }

    setLocationConstraints(locationConstraints) {
        this.locationConstraints = locationConstraints;
        this.bootstrapList = Object.keys(locationConstraints).map(key => ({
            site: key,
            type: locationConstraints[key].type,
        }));
        this.emit('location-constraints-update');
    }

    getBootstrapList() {
        return this.bootstrapList;
    }

    setIngestionBuckets(locationConstraints, buckets, log) {
        const ingestionBuckets = [];
        buckets.forEach(bucket => {
            const { name, ingestion, locationConstraint } = bucket;
            const locationInfo = locationConstraints[locationConstraint];
            if (!locationInfo ||
                !locationInfo.details ||
                !locationInfo.locationType) {
                log.debug('ingestion bucket missing information', {
                    bucket,
                    locationInfo,
                });
                return;
            }
            // if location is not an enabled backbeat ingestion location, skip
            const locationType = ingestionTypeMatch[locationInfo.locationType];
            if (!locationType) {
                return;
            }
            const ingestionBucketDetails = Object.assign(
                {},
                locationInfo.details,
                {
                    locationType,
                    zenkoBucket: name,
                    ingestion,
                    locationConstraint,
                }
            );
            ingestionBuckets.push(ingestionBucketDetails);
        });
        this.ingestionBuckets = ingestionBuckets;
    }

    getIngestionBuckets() {
        return this.ingestionBuckets;
    }

    setIsTransientLocation(locationName, isTransient) {
        this.transientLocations[locationName] = isTransient;
    }

    getIsTransientLocation(locationName) {
        return this.transientLocations[locationName] || false;
    }

    getPublicInstanceId() {
        return this.publicInstanceId;
    }

    setPublicInstanceId(instanceId) {
        this.publicInstanceId = crypto.createHash('sha256')
                                .update(instanceId)
                                .digest('hex');
    }

    // FIXME: those azure functions have been copied from Cloudserver,
    // they are only there to avoid a crash of backbeat containers
    // when azure locations are configured in Orbit, but replication
    // to azure is not yet supported. (ZENKO-2429).
    getAzureEndpoint(locationConstraint) {
        let azureStorageEndpoint =
        process.env[`${locationConstraint}_AZURE_STORAGE_ENDPOINT`] ||
        this.locationConstraints[locationConstraint]
            .details.azureStorageEndpoint;
        if (!azureStorageEndpoint.endsWith('/')) {
            // append the trailing slash
            azureStorageEndpoint = `${azureStorageEndpoint}/`;
        }
        return azureStorageEndpoint;
    }

    getAzureStorageAccountName(locationConstraint) {
        const { azureStorageAccountName } =
            this.locationConstraints[locationConstraint].details;
        const storageAccountNameFromEnv =
            process.env[`${locationConstraint}_AZURE_STORAGE_ACCOUNT_NAME`];
        return storageAccountNameFromEnv || azureStorageAccountName;
    }

    getAzureStorageCredentials(locationConstraint) {
        const { azureStorageAccessKey } =
            this.locationConstraints[locationConstraint].details;
        const storageAccessKeyFromEnv =
            process.env[`${locationConstraint}_AZURE_STORAGE_ACCESS_KEY`];
        return {
            storageAccountName:
                this.getAzureStorageAccountName(locationConstraint),
            storageAccessKey: storageAccessKeyFromEnv || azureStorageAccessKey,
        };
    }
}

module.exports = new Config();
