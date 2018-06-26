const async = require('async');
const AWS = require('aws-sdk');
const werelogs = require('werelogs');

const config = require('../../conf/Config');
const management = require('../../lib/management/index');

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

function putVersioning(bucketName, endpoint, cb) {
    const params = {
        Bucket: bucketName,
        VersioningConfiguration: {
            Status: 'Enabled',
        },
    };
    return getS3Client(endpoint).putBucketVersioning(params, err => {
        if (err && err.code === 'NoSuchBucket') {
            logger.info('cannot apply replication configuration: bucket ' +
                        'does not exist',
                        { sourceBucket: bucketName });
            return cb();
        }
        return cb(err);
    });
}

function installReplicationConfiguration(bucketName, endpoint, workflows, cb) {
    const params = {
        Bucket: bucketName,
        ReplicationConfiguration: {
            Role: 'arn:aws:iam::root:role/s3-replication-role',
            Rules: workflows.map(wf => ({
                Destination: {
                    Bucket: `arn:aws:s3:::${wf.source.bucketName}`,
                    StorageClass: wf.destination.locations
                        .map(location => {
                            if (wf.destination.preferredReadLocation
                                === location.name) {
                                return `${location.name}:preferred_read`;
                            }
                            return location.name;
                        })
                        .join(','),
                },
                Prefix: wf.source.prefix || '',
                Status: wf.enabled ? 'Enabled' : 'Disabled',
            })),
        },
    };

    getS3Client(endpoint).putBucketReplication(params, err => {
        logger.debug('replication configuration apply done', {
            sourceBucket: bucketName, error: err });
        return cb(err);
    });
}

function putReplication(bucketName, workflows, cb) {
    logger.debug('updating replication configuration');
    const cfg = config.extensions.replication.source.s3;
    const endpoint = `${cfg.host}:${cfg.port}`;

    async.series([
        done => putVersioning(bucketName, endpoint, done),
        // TODO add service account in source & target bucket ACLs
        done => installReplicationConfiguration(bucketName, endpoint,
                                                workflows, done),
    ], cb);
}

function deleteReplication(bucketName, cb) {
    logger.debug('deleting replication configuration');
    const cfg = config.extensions.replication.source.s3;
    const endpoint = `${cfg.host}:${cfg.port}`;

    const params = {
        Bucket: bucketName,
    };
    getS3Client(endpoint).deleteBucketReplication(params, err => {
        logger.debug('replication configuration deleted', {
            sourceBucket: bucketName, error: err });
        if (err && err.code === 'NoSuchBucket') {
            logger.info('cannot delete replication configuration: bucket ' +
                        'does not exist',
                        { sourceBucket: bucketName });
            return cb();
        }
        return cb(err);
    });
}

function applyBucketReplicationWorkflows(bucketName, bucketWorkflows,
                                         workflowUpdates, cb) {
    if (bucketWorkflows.length > 0) {
        putReplication(bucketName, bucketWorkflows, cb);
    } else {
        deleteReplication(bucketName, cb);
    }
}

module.exports = {
    applyBucketReplicationWorkflows,
};
