'use strict';

const config = require('../Config');
const bucketclient = require('@scality/bucketclient');
const Metadata = require('@scality/arsenal').storage.metadata.MetadataWrapper;
const refreshInterval = 5000;

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
            shardCollections: mongo.shardCollections,
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

function periodicallyUpdateIngestionBuckets(locations, logger, cb) {
    const mdParams = buildMetadataParams(config);
    const metadata = new Metadata('mongodb', mdParams, bucketclient, logger);
    return metadata.setup(err => {
        if (err) {
            logger.fatal('error setting up metadata mongodb client', {
                method: 'management::initManagement',
                error: err,
            });
            process.exit(1);
        }
        return updateIngestionBuckets(locations, metadata, logger, err => {
            if (err) {
                logger.error('error updating ingestion buckets', {
                    method: 'management::initManagement',
                    error: err,
                });
                return cb(err);
            }
            setInterval(updateIngestionBuckets, refreshInterval, locations, metadata, logger, err => {
                if (err) {
                    logger.error('error updating ingestion buckets periodically', {
                        method: 'management::initManagement',
                        error: err,
                    });
                }
            });
            return cb();
        });
    });
}

module.exports = {
    updateIngestionBuckets,
    buildMetadataParams,
    periodicallyUpdateIngestionBuckets,
    refreshInterval,
};
