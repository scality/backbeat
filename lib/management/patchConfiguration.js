'use strict'; // eslint-disable-line strict

const async = require('async');
const config = require('../Config');
const { decryptSecret } = require('arsenal').pensieve.credentialUtils;
const managementDatabaseName = 'PENSIEVE';
const tokenConfigurationKey = 'auth/zenko/remote-management-token';

function updateIngestionBuckets(locations, metadata, logger, cb) {
    metadata.getIngestionBuckets(logger, (err, buckets) => {
        if (err) {
            logger.error('error get ingestion buckets from mongo', {
                method: 'patchConfiguration::updateIngestionBuckets',
                error: err.message,
            });
            return cb(err);
        }
        config.setIngestionBuckets(locations, buckets, logger);
        return cb();
    });
}

function patchLocations(overlayLocations, creds, log) {
    if (!overlayLocations) {
        return {};
    }

    const locations = {};
    Object.keys(overlayLocations || {}).forEach(k => {
        const l = overlayLocations[k];
        const location = {
            name: k,
            objectId: l.objectId,
            details: l.details || {},
            locationType: l.locationType,
        };
        let supportsVersioning = false;
        let pathStyle = process.env.CI_CEPH !== undefined;

        switch (l.locationType) {
        case 'location-mem-v1':
            location.type = 'mem';
            location.details = { supportsVersioning: true };
            break;
        case 'location-file-v1':
            location.type = 'file';
            location.details = { supportsVersioning: true };
            break;
        case 'location-azure-v1':
            location.type = 'azure';
            if (l.details.secretKey && l.details.secretKey.length > 0) {
                location.details = {
                    bucketMatch: l.details.bucketMatch,
                    azureStorageEndpoint: l.details.endpoint,
                    azureStorageAccountName: l.details.accessKey,
                    azureStorageAccessKey: decryptSecret(creds,
                        l.details.secretKey),
                    azureContainerName: l.details.bucketName,
                };
            }
            break;
        case 'location-ceph-radosgw-s3-v1':
        case 'location-scality-ring-s3-v1':
            pathStyle = true; // fallthrough
        case 'location-aws-s3-v1':
        case 'location-wasabi-v1':
            supportsVersioning = true; // fallthrough
        case 'location-do-spaces-v1':
            location.type = 'aws_s3';
            if (l.details.secretKey && l.details.secretKey.length > 0) {
                let https = true;
                let awsEndpoint = l.details.endpoint ||
                    's3.amazonaws.com';
                if (awsEndpoint.includes('://')) {
                    const url = new URL(awsEndpoint);
                    awsEndpoint = url.host;
                    https = url.protocol.includes('https');
                }

                location.details = {
                    credentials: {
                        accessKey: l.details.accessKey,
                        secretKey: decryptSecret(creds,
                            l.details.secretKey),
                    },
                    bucketName: l.details.bucketName,
                    bucketMatch: l.details.bucketMatch,
                    serverSideEncryption:
                        Boolean(l.details.serverSideEncryption),
                    region: l.details.region,
                    awsEndpoint,
                    supportsVersioning,
                    pathStyle,
                    https,
                };
            }
            break;
        case 'location-gcp-v1':
            location.type = 'gcp';
            if (l.details.secretKey && l.details.secretKey.length > 0) {
                location.details = {
                    credentials: {
                        accessKey: l.details.accessKey,
                        secretKey: decryptSecret(creds,
                            l.details.secretKey),
                    },
                    bucketName: l.details.bucketName,
                    mpuBucketName: l.details.mpuBucketName,
                    bucketMatch: l.details.bucketMatch,
                    gcpEndpoint: l.details.endpoint ||
                        'storage.googleapis.com',
                    https: true,
                };
            }
            break;
        case 'location-scality-sproxyd-v1':
            location.type = 'scality';
            if (l.details && l.details.bootstrapList &&
                l.details.proxyPath) {
                location.details = {
                    supportsVersioning: true,
                    connector: {
                        sproxyd: {
                            chordCos: l.details.chordCos || null,
                            bootstrap: l.details.bootstrapList,
                            path: l.details.proxyPath,
                        },
                    },
                };
            }
            break;
        case 'location-nfs-mount-v1':
            location.type = 'pfs';
            if (l.details) {
                location.details = {
                    supportsVersioning: true,
                    bucketMatch: true,
                    pfsDaemonEndpoint: {
                        host: `${l.name}-cosmos-pfsd`,
                        port: 80,
                    },
                };
            }
            break;
        case 'location-scality-hdclient-v2':
            location.type = 'scality';
            if (l.details && l.details.bootstrapList) {
                location.details = {
                    supportsVersioning: true,
                    connector: {
                        hdclient: {
                            bootstrap: l.details.bootstrapList,
                        },
                    },
                };
            }
            break;
        default:
            log.info('unknown location type', { locationType:
                l.locationType });
            return;
        }
        location.sizeLimitGB = l.sizeLimitGB || null;
        location.isTransient = Boolean(l.isTransient);
        location.legacyAwsBehavior = Boolean(l.legacyAwsBehavior);
        locations[location.name] = location;
    });
    return locations;
}

function updateLocations(conf, logger, cb) {
    try {
        const locations = conf.locations;
        const locationsWithReplicationBackend = Object.keys(locations)
        // NOTE: In Orbit, we don't need to have Scality location in our
        // destination bootstrapList config, since we do not replicate to
        // any Scality Instance yet.
        .filter(key => locations[key].locationType !== 'location-file-v1')
        .reduce((obj, key) => {
            /* eslint no-param-reassign:0 */
            obj[key] = locations[key];
            return obj;
        }, {});
        config.setBootstrapList(locationsWithReplicationBackend);

        Object.keys(locations).forEach(locName => {
            config.setIsTransientLocation(
                locName, locations[locName].isTransient);
        });
    } catch (error) {
        logger.error('error updating locations', {
            error: error.message,
            method: 'patchConfiguration::updateLocations',
        });
        return cb(error);
    }
    return cb(null, conf.version);
}

function patchConfiguration(overlayVersion, patchConfig, metadata, ingestionEnabled,
logger, cb) {
    if (patchConfig.version === undefined) {
        return process.nextTick(cb, null);
    }

    const conf = Object.assign({}, patchConfig);
    if (conf && conf.locations) {
        return async.waterfall([
            next => metadata.getObjectMD(
                managementDatabaseName,
                tokenConfigurationKey,
                {},
                logger,
                next,
            ),
            (creds, next) => {
                conf.locations = patchLocations(conf.locations, creds, logger);
                // only update ingestion buckets list when ingestionEnabled
                if (!ingestionEnabled) {
                    return process.nextTick(next);
                }
                return updateIngestionBuckets(conf.locations, metadata, logger, next);
            },
            next => {
                // only update bootstraplist on new overlay version
                if (overlayVersion !== undefined &&
                    overlayVersion >= conf.version) {
                    return process.nextTick(next);
                }

                // save instance id
                config.setPublicInstanceId(conf.instanceId);
                return updateLocations(conf, logger, cb);
            }
        ], cb);
    }
    return process.nextTick(cb, null);
}

module.exports = {
    patchConfiguration,
    updateLocations,
    updateIngestionBuckets,
};
