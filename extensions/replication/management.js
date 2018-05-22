const async = require('async');
const AWS = require('aws-sdk');
const werelogs = require('werelogs');

const config = require('../../conf/Config');
const management = require('../../lib/management');

const logger = new werelogs.Logger('mdManagement:replication');

function getS3Client(endpoint) {
    const serviceCredentials =
          management.getLatestServiceAccountCredentials();
    // FIXME
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

function applyReplicationState(conf, currentStateSerialized, cb) {
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
        management.saveExtensionState(currentState, cb);
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
            return management.saveExtensionState(currentState, cb);
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
            return management.saveExtensionState(currentState, cb);
        });
        return undefined;
    });
    return undefined;
}

module.exports = {
    applyReplicationState,
};
