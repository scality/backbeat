const async = require('async');
const forge = require('node-forge');
const arsenal = require('arsenal');
const config = require('../conf/Config');
const AWS = require('aws-sdk');

const MetadataFileClient = arsenal.storage.metadata.MetadataFileClient;
const mdClient = new MetadataFileClient({
    host: config.queuePopulator.dmd.host,
    port: config.queuePopulator.dmd.port,
});

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
    console.log('saving replication state', currentState);
    db.put(replicationStateKey, JSON.stringify(currentState), {}, cb);
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
    if (!stream.enabled) {
        return process.nextTick(cb);
    }
    const params = {
        Bucket: end.bucketName,
        CreateBucketConfiguration: {
            LocationConstraint: end.location,
        },
    };
    getS3Client(endpoint).createBucket(params, err => {
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
    getS3Client(endpoint).putBucketVersioning(params, err => {
        return cb(err);
    });
}

function installReplicationPolicy(stream, endpoint, cb) {
    const roleName = 'arn:aws:iam::root:role/s3-replication-role';
    const params = {
        Bucket: stream.source.bucketName,
        ReplicationConfiguration: {
            Role: `${roleName},${roleName}`,
            Rules: [{
                Destination: {
                    Bucket: `arn:aws:s3:::${stream.destination.bucketName}`,
                },
                Prefix: stream.source.prefix || '',
                Status: stream.enabled ? 'Enabled' : 'Disabled',
            }],
        },
    };
    getS3Client(endpoint).putBucketReplication(params, err => {
        console.log('replication applied for', stream.source.bucketName, 'to',
            stream.destination.bucketName, err);
        if (err && err.code === 'NoSuchBucket' && !stream.enabled) {
            return cb();
        }
        return cb(err);
    });
}

function putReplication(stream, cb) {
    console.log('Changing replication for stream', stream.streamId, 'to', stream.enabled);
    const cfg = config.extensions.replication.source.s3;
    const endpoint = `${cfg.host}:${cfg.port}`;

    async.waterfall([
        done => ensureBucketExists(stream, stream.source, endpoint, done),
        done => ensureBucketExists(stream, stream.destination, endpoint, done),
        done => putVersioning(stream, stream.source.bucketName, endpoint, done),
        done => putVersioning(stream, stream.destination.bucketName, endpoint, done),
        // TODO add service account in source & target bucket ACLs
        done => installReplicationPolicy(stream, endpoint, done),
    ], cb);
}

function disableReplication(stream, cb) {
    console.log('disabling replication for stream', stream.streamId);
    return cb(null, stream);
}

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
        console.log('deleting obsolete stream', streamId);
        const stream = currentState.streams[streamId];
        stream.enabled = false;
        putReplication(stream, err => {
            if (err) {
                return cb(err);
            }
            delete currentState.streams[streamId];
            saveCurrentReplicationState(db, currentState, cb);
        });
    }

    function configureStream(stream, cb) {
        if (currentState.streams[stream.streamId]) {
            if (!currentState.streams[stream.streamId].enabled && stream.enabled) {
                console.log('enabling existing stream', stream.streamId);
                putReplication(stream, err => {
                    if (err) {
                        return cb(err);
                    }
                    return streamConfigured(stream, cb);
                });
            } else if (currentState.streams[stream.streamId].enabled && !stream.enabled) {
                console.log('disabling existing stream', stream.streamId);
                putReplication(stream, err => {
                    if (err) {
                        return cb(err);
                    }
                    return streamConfigured(stream, cb);
                });
            } else {
                console.log('stream has expected state, doing nothing', stream.streamId, stream.enabled);
                return cb();
            }
        } else {
            if (stream.enabled) {
                console.log('enabling new stream', stream.streamId);
                putReplication(stream, err => {
                    if (err) {
                        return cb(err);
                    }
                    return streamConfigured(stream, cb);
                });
            } else {
                console.log('new disabled stream, doing nothing', stream.streamId);
                return cb();
            }
        }
    }

    const existingStreamIds = Object.keys(currentState.streams);
    const incomingConfigStreamIds = conf.replicationStreams.map(s => s.streamId);
    const deletedStreamIds = existingStreamIds.filter(s => !incomingConfigStreamIds.includes(s));

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
            saveCurrentReplicationState(db, currentState, cb);
        });
    });
}

function saveServiceCredentials(conf, auth) {
    const instanceAuth = JSON.parse(auth);
    const serviceAccount = (conf.users || []).find(
        u => u.accountType === 'service-replication');
    if (!serviceAccount) {
        return;
    }

    // TODO generate a proper canonicalID during the service account / instance provisioning
    // TODO use a proper account id in arn

    const secretKey = decryptSecret(instanceAuth, serviceAccount.secretKey);
    serviceCredentials = {
        accounts: [{
            name: 'service-replication',
            arn: 'aws::iam:234456789012:root',
            // canonicalID: '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be',
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

function loadOverlayVersion(db, version, cb) {
    db.get(`configuration/overlay/${version}`, {}, (err, val) => {
        if (err) {
            return cb(err);
        }
        return cb(null, JSON.parse(val));
    });
}

function initManagement(done) {
    // if (initialized) {
    //     return process.nextTick(done);
    // }

    const mdDb = mdClient.openDB(error => {
        if (error) {
            return process.nextTick(done, error);
        }

        const db = mdDb.openSub('PENSIEVE');

        function iterate(done) {
            return async.waterfall([
                cb => db.get('configuration/overlay-version', {}, cb),
                (version, cb) => loadOverlayVersion(db, version, cb),
                (conf, cb) => db.get(tokenKey, {}, (err, instanceAuth) => {
                    if (err) {
                        return cb(err);
                    }
                    saveServiceCredentials(conf, instanceAuth);
                    return cb(null, conf);
                }),
                (conf, cb) => db.get(replicationStateKey, {}, (err, currentReplicationState) => {
                    if (err && err.ObjNotFound) {
                        currentReplicationState = null;
                    } else if (err) {
                        return cb(err);
                    }
                    // console.log('loaded replication state', currentReplicationState);
                    applyReplicationState(conf, currentReplicationState, db, cb);
                    // return cb(null, conf);
                }),
            ], done);
        }

        iterate(err => {
            if (err) {
                return done(err);
            }
            setInterval(iterate, 5000, err => {
                if (err) {
                    console.log('error refreshing mgdb', err);
                }
            });
            done();
        });
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
