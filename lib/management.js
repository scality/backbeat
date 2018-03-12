const async = require('async');
const forge = require('node-forge');
const arsenal = require('arsenal');
const config = require('../conf/Config');
const AWS = require('aws-sdk');
const werelogs = require('werelogs');
const bucketclient = require('bucketclient');

werelogs.configure({
    level: config.log.logLevel,
    dump: config.log.dumpLevel,
});
const logger = new werelogs.Logger('mdManagement');

const serviceBucket = 'PENSIEVE';

const Metadata = arsenal.storage.metadata.MetadataWrapper;
let metadata;

const params = {
    bucketdBootstrap: ['localhost'],
    bucketdLog: null,
    https: null,
    metadataClient: {
        host: config.queuePopulator.dmd.host,
        port: config.queuePopulator.dmd.port,
    },
    replicationGroupId:
        config.extensions.replication.replicationStatusProcessor.groupId,
    noDbOpen: null,
    constants: {
        usersBucket: 'users..bucket',
        splitter: '..|..',
    },
    mongodb: {
        replicaSetHosts: config.queuePopulator.mongo.replicaSetHosts,
        writeConcern: config.queuePopulator.mongo.writeConcern,
        replicaSet: config.queuePopulator.mongo.replicaSet,
        readPreference: config.queuePopulator.mongo.readPreference,
        database: config.queuePopulator.mongo.database,
        replicationGroupId:
            config.extensions.replication.replicationStatusProcessor.groupId,
        path: '',
    },
};

let overlayVersion;

// let initialized = false;
let serviceCredentials = {
    accounts: [],
};

const tokenKey = 'auth/zenko/remote-management-token';
const replicationStateKey = 'configuration/state/replication';

// XXX copy-pasted from S3
function decryptSecret(instanceCredentials, secret) {
    // XXX don't forget to use u.encryptionKeyVersion if present
    const privateKey = forge.pki.privateKeyFromPem(
        instanceCredentials.privateKey);
    const encryptedSecretKey = forge.util.decode64(secret);
    return privateKey.decrypt(encryptedSecretKey, 'RSA-OAEP', {
        md: forge.md.sha256.create(),
    });
}

function saveCurrentReplicationState(db, currentState, cb) {
    logger.debug('saving replication state', { currentState });
    metadata.putObjectMD(serviceBucket, replicationStateKey,
        JSON.stringify(currentState), {}, logger, cb);
}

function getS3Client(endpoint) {
    const keys = serviceCredentials.accounts[0].keys;
    const credentials = new AWS.Credentials(keys.access, keys.secret);
    const s3Client = new AWS.S3({
        endpoint,
        sslEnabled: false,
        credentials,
        s3ForcePathStyle: true,
        signatureVersion: 'v4',
        httpOptions: { timeout: 0 },
        maxRetries: 3,
    });
    return s3Client;
}

function ensureBucketExists(stream, end, endpoint, cb) {
    logger.debug('Checking that bucket exists', { end, endpoint });
    if (!stream.enabled) {
        logger.debug('stream is not enabled');
        return process.nextTick(cb);
    }
    const params = {
        Bucket: end.bucketName,
    };
    return getS3Client(endpoint).createBucket(params, err => {
        if (err && err.code === 'BucketAlreadyOwnedByYou') {
            return cb();
        }
        return cb(err);
    });
}

function putVersioning(stream, bucketName, endpoint, cb) {
    if (!stream.enabled) {
        return process.nextTick(cb);
    }
    const params = {
        Bucket: bucketName,
        VersioningConfiguration: {
            Status: 'Enabled',
        },
    };
    return getS3Client(endpoint).putBucketVersioning(params, err => cb(err));
}

function installReplicationPolicy(stream, endpoint, cb) {
    const destinationLocations = stream.destination.locations
        .map(location => location.name)
        .join(',');
    const roleName = 'arn:aws:iam::root:role/s3-replication-role';
    const params = {
        Bucket: stream.source.bucketName,
        ReplicationConfiguration: {
            Role: `${roleName}`,
            Rules: [{
                Destination: {
                    Bucket: `arn:aws:s3:::${stream.source.bucketName}`,
                    StorageClass: destinationLocations,
                },
                Prefix: stream.source.prefix || '',
                Status: stream.enabled ? 'Enabled' : 'Disabled',
            }],
        },
    };

    getS3Client(endpoint).putBucketReplication(params, err => {
        logger.debug('replication applied', {
            sourceBucket: stream.source.bucketName,
            destinationLocations, error: err });
        if (err && err.code === 'NoSuchBucket' && !stream.enabled) {
            return cb();
        }
        return cb(err);
    });
}

function putReplication(stream, cb) {
    logger.debug('Changing replication for stream to stream.enabled',
        { streamId: stream.streamId });
    const cfg = config.extensions.replication.source.s3;
    const endpoint = `${cfg.host}:${cfg.port}`;

    async.waterfall([
        done => ensureBucketExists(stream, stream.source, endpoint, done),
        done => putVersioning(stream, stream.source.bucketName, endpoint, done),
        // TODO add service account in source & target bucket ACLs
        done => installReplicationPolicy(stream, endpoint, done),
    ], cb);
}

/* eslint-disable no-unused-vars */
function disableReplication(stream, cb) {
    logger.debug('disabling replication for stream',
        { streamId: stream.streamId });
    return cb(null, stream);
}
/* eslint-enable no-unused-vars */

function applyReplicationState(conf, currentStateSerialized, db, cb) {
    if (!conf.replicationStreams) {
        return process.nextTick(cb);
    }

    const currentState = currentStateSerialized ?
        JSON.parse(currentStateSerialized) : {
            streams: {},
            overlayVersion: 0,
        };

    if (currentState.overlayVersion >= conf.version) {
        return process.nextTick(cb);
    }

    function streamConfigured(stream, cb) {
        currentState.streams[stream.streamId] = stream;
        saveCurrentReplicationState(db, currentState, cb);
    }

    function deleteStream(streamId, cb) {
        logger.debug('deleting obsolete stream`', { streamId });
        const stream = currentState.streams[streamId];
        stream.enabled = false;
        putReplication(stream, err => {
            if (err) {
                return cb(err);
            }
            delete currentState.streams[streamId];
            return saveCurrentReplicationState(db, currentState, cb);
        });
    }

    function arraysEqual(arr1, arr2) {
        if (arr1.length !== arr2.length) {
            return false;
        }
        for (let i = arr1.length; i--;) {
            if (arr1[i] !== arr2[i]) {
                return false;
            }
        }
        return true;
    }

    function putReplicationAndStreamConfigured(stream, cb) {
        putReplication(stream, err => {
            if (err) {
                return cb(err);
            }
            return streamConfigured(stream, cb);
        });
    }

    function configureStream(stream, cb) {
        if (currentState.streams[stream.streamId]) {
            if (!currentState.streams[stream.streamId].enabled
                && stream.enabled) {
                logger.debug('enabling existing stream', {
                    streamId: stream.streamId });
                putReplicationAndStreamConfigured(stream, cb);
            } else if (currentState.streams[stream.streamId].enabled
                && !stream.enabled) {
                logger.debug('disabling existing stream',
                    { streamId: stream.streamId });
                putReplicationAndStreamConfigured(stream, cb);
            } else if (currentState.streams[stream.streamId].source.prefix
                !== stream.source.prefix) {
                logger.debug('changing stream source prefix',
                    { streamId: stream.streamId });
                putReplicationAndStreamConfigured(stream, cb);
            } else if (!arraysEqual(currentState.streams[stream.streamId]
                .destination.locations, stream.destination.locations)) {
                logger.debug('changing stream destination locations',
                    { streamId: stream.streamId });
                putReplicationAndStreamConfigured(stream, cb);
            } else {
                logger.debug('stream has expected state, doing nothing',
                { streamId: stream.streamId, streamEnabled: stream.enabled });
                return cb();
            }
        } else {
            if (stream.enabled) {
                logger.debug('enabling new stream',
                    { streamId: stream.streamId });
                putReplicationAndStreamConfigured(stream, cb);
            } else {
                logger.debug('new disabled stream, doing nothing',
                    { streamId: stream.streamId });
                return cb();
            }
        }
        return undefined;
    }

    const existingStreamIds = Object.keys(currentState.streams);
    const incomingConfigStreamIds =
        conf.replicationStreams.map(s => s.streamId);
    const deletedStreamIds =
        existingStreamIds.filter(s => !incomingConfigStreamIds.includes(s));

    // TODO group streams per source bucket
    async.eachSeries(conf.replicationStreams, configureStream, err => {
        if (err) {
            return cb(err);
        }
        currentState.overlayVersion = conf.version;
        async.eachSeries(deletedStreamIds, deleteStream, err => {
            if (err) {
                return cb(err);
            }
            return saveCurrentReplicationState(db, currentState, cb);
        });
        return undefined;
    });
    return undefined;
}

function saveServiceCredentials(conf, auth) {
    // const instanceAuth = JSON.parse(auth);
    const instanceAuth = auth;
    const serviceAccount = (conf.users || []).find(
        u => u.accountType === 'service-replication');
    if (!serviceAccount) {
        return;
    }

    // TODO generate a proper canonicalID during the service account /
    // instance provisioning
    // TODO use a proper account id in arn

    const secretKey = decryptSecret(instanceAuth, serviceAccount.secretKey);
    serviceCredentials = {
        accounts: [{
            name: 'service-replication',
            arn: 'aws::iam:234456789012:root',
            // canonicalID:
// '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be',
            canonicalID: '12349df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d52',
            displayName: serviceAccount.userName,
            keys: {
                access: serviceAccount.accessKey,
                secret: secretKey,
            },
        }],
    };
    // initialized = true;
}

function loadOverlayVersion(metadata, version, cb) {
    metadata.getObjectMD(serviceBucket, `configuration/overlay/${version}`, {},
    logger, (err, val) => {
        if (err) {
            return cb(err);
        }
        return cb(null, val);
    });
}

function patchConfiguration(conf, cb) {
    if (conf.version === undefined) {
        return process.nextTick(cb, null, conf);
    }
    if (overlayVersion !== undefined &&
        overlayVersion >= conf.version) {
        return process.nextTick(cb, null, conf);
    }
    if (conf && conf.locations) {
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
            config.setReplicationEndpoints(locationsWithReplicationBackend);
        } catch (error) {
            return cb(error);
        }
        overlayVersion = conf.version;
    }
    return process.nextTick(cb, null, conf);
}

function initManagement(done) {
    let setup = false;
    function iterate(done) {
        return async.waterfall([
            cb => {
                if (!setup) {
                    setup = true;
                    metadata = new Metadata('mongodb', params, bucketclient,
                        logger);
                    return metadata.setup(() => cb());
                }
                return process.nextTick(cb);
            },
            cb => {
                metadata.getObjectMD(serviceBucket,
                'configuration/overlay-version', {}, logger, (err, res) =>
                    cb(err, res));
            },
            (version, cb) => loadOverlayVersion(metadata, version, cb),
            (conf, cb) => patchConfiguration(conf, cb),
            (conf, cb) => metadata.getObjectMD(serviceBucket, tokenKey, {},
            logger, (err, instanceAuth) => {
                if (err) {
                    return cb(err);
                }
                saveServiceCredentials(conf, instanceAuth);
                return cb(null, conf);
            }),
            (conf, cb) => metadata.getObjectMD(serviceBucket,
            replicationStateKey, {}, logger, (err, currentReplicationState) => {
                if (err && err.NoSuchKey) {
                    /* eslint-disable no-param-reassign */
                    currentReplicationState = null;
                } else if (err) {
                    return cb(err);
                }
                return applyReplicationState(conf, currentReplicationState,
                    metadata, cb);
            }),
        ], done);
    }

    iterate(err => {
        if (err) {
            return done(err);
        }
        setInterval(iterate, 5000, err => {
            if (err) {
                logger.error('error refreshing mgdb', { err });
            }
        });
        return done();
    });
    return undefined;
}

function getLatestServiceAccountCredentials() {
    return serviceCredentials;
}

module.exports = {
    initManagement,
    getLatestServiceAccountCredentials,
};
