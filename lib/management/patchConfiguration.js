'use strict'; // eslint-disable-line strict

const async = require('async');
const config = require('../Config');
const { patchLocations } = require('arsenal').patches.locationConstraints;
const managementDatabaseName = 'PENSIEVE';
const tokenConfigurationKey = 'auth/zenko/remote-management-token';
const refreshInterval = 5000;
let patchedLocations = {};

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

function updateLocations(locations) {
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
}

function patchConfiguration(
    overlayVersion, patchConfig, metadata, ingestionEnabled, logger, cb
) {
    if (patchConfig === undefined ||
        patchConfig.version === undefined ||
        patchConfig.locations === undefined
    ) {
        return process.nextTick(cb, null);
    }

    const conf = Object.assign({}, patchConfig);
    return async.waterfall([
        next => metadata.getObjectMD(
            managementDatabaseName,
            tokenConfigurationKey,
            {},
            logger,
            next,
        ),
        (creds, next) => {
            // only update locations on new overlay version
            if (overlayVersion === undefined || conf.version > overlayVersion) {
                patchedLocations = patchLocations(conf.locations, creds, logger);
                config.setPublicInstanceId(conf.instanceId);
                updateLocations(patchedLocations);
            }

            // only update ingestion buckets list when ingestionEnabled
            if (ingestionEnabled) {
                return updateIngestionBuckets(patchedLocations, metadata, logger, next);
            }
            return process.nextTick(next);
        },
    ], cb);
}

function buildMetadataParams(c) {
    const groupId = c.extensions.replication.replicationStatusProcessor.groupId;
    const mongo = c.queuePopulator.mongo;
    const dmd = c.queuePopulator.dmd;
    const params = {
        bucketdBootstrap: ['localhost'],
        bucketdLog: null,
        https: null,
        replicationGroupId: groupId,
        noDbOpen: null,
        constants: {
            usersBucket: 'users..bucket',
            splitter: '..|..',
        },
        mongodb: {
            replicaSetHosts: mongo.replicaSetHosts,
            writeConcern: mongo.writeConcern,
            replicaSet: mongo.replicaSet,
            readPreference: mongo.readPreference,
            database: mongo.database,
            replicationGroupId: groupId,
            path: '',
            authCredentials: mongo.authCredentials,
        },
    };

    if (dmd) {
        params.metadataClient = {
            host: dmd.host,
            port: dmd.port,
        };
    }
    return params;
}

module.exports = {
    patchConfiguration,
    updateLocations,
    updateIngestionBuckets,
    buildMetadataParams,
    refreshInterval,
};
