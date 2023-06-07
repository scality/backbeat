'use strict'; // eslint-disable-line

const assert = require('assert');
const { EventEmitter } = require('events');

const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const extensions = require('../extensions');
const backbeatConfigJoi = require('./config.joi');

const locationTypeMatch = {
    'location-mem-v1': 'mem',
    'location-file-v1': 'file',
    'location-azure-v1': 'azure',
    'location-do-spaces-v1': 'aws_s3',
    'location-aws-s3-v1': 'aws_s3',
    'location-wasabi-v1': 'aws_s3',
    'location-gcp-v1': 'gcp',
    'location-scality-ring-s3-v1': 'aws_s3',
    'location-scality-artesca-s3-v1': 'aws_s3',
    'location-ceph-radosgw-s3-v1': 'aws_s3',
    'location-dmf-v1': 'dmf',
    'location-azure-archive-v1': 'azure_archive'
};
// NOTE: currently only s3 connector (s3c 7.4.3 and above) are supported
const ingestionTypeMatch = {
    'location-scality-ring-s3-v1': 'scality_s3',
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

        const { value: parsedConfig, err } = backbeatConfigJoi.validate(config);
        if (err) {
            throw new Error(`could not validate config file: ${err.message}`);
        }

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

        // Overwrite logSource if configured
        const configuredLogSource = process.env.BACKBEAT_QUEUEPOPULATOR_LOGSOURCE;
        if (configuredLogSource) {
            parsedConfig.queuePopulator.logSource = configuredLogSource;
        }
        // Overwrite extension configs if configured
        // We can specify the list of extensions that should be handled by this
        // instance of the queuePopulator
        const configuredExtensions = process.env.BACKBEAT_QUEUEPOPULATOR_EXTENSIONS;
        if (configuredExtensions) {
            const allowedExtensions = configuredExtensions.split(',');
            const filteredExtensions = Object.entries(parsedConfig.extensions)
                .filter(entry => allowedExtensions.includes(entry[0]));
            parsedConfig.extensions = Object.fromEntries(filteredExtensions);
        }

        // config is validated, safe to assign directly to the config object
        Object.assign(this, parsedConfig);

        this.transientLocations = {};

        this._setTimeOptions();
    }

    _setTimeOptions() {
        // NOTE: EXPIRE_ONE_DAY_EARLIER and TRANSITION_ONE_DAY_EARLIER are deprecated in favor of
        // TIME_PROGRESSION_FACTOR which decreases the weight attributed to a day in order to, amongst other things,
        // expedite the lifecycle of objects.

        // moves lifecycle expiration deadlines 1 day earlier, mostly for testing
        const expireOneDayEarlier = process.env.EXPIRE_ONE_DAY_EARLIER === 'true';
        // moves lifecycle transition deadlines 1 day earlier, mostly for testing
        const transitionOneDayEarlier = process.env.TRANSITION_ONE_DAY_EARLIER === 'true';
        // decreases the weight attributed to a day in order to expedite the lifecycle of objects.
        const timeProgressionFactor = Number.parseInt(process.env.TIME_PROGRESSION_FACTOR, 10) || 1;

        const isIncompatible = (expireOneDayEarlier || transitionOneDayEarlier) && (timeProgressionFactor > 1);
        assert(!isIncompatible, 'The environment variables "EXPIRE_ONE_DAY_EARLIER" or ' +
        '"TRANSITION_ONE_DAY_EARLIER" are not compatible with the "TIME_PROGRESSION_FACTOR" variable.');

        this.timeOptions = {
            expireOneDayEarlier,
            transitionOneDayEarlier,
            timeProgressionFactor,
        };
    }

    getTimeOptions() {
        return this.timeOptions;
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

    setBootstrapList(locationConstraints) {
        this.bootstrapList = Object.keys(locationConstraints).map(key => ({
            site: key,
            type: locationTypeMatch[locationConstraints[key].locationType],
        }));
        this.emit('bootstrap-list-update');
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
}

module.exports = new Config();
