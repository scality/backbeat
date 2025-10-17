const async = require('async');
const {
    S3Client,
    PutBucketVersioningCommand,
    PutBucketReplicationCommand,
    DeleteBucketReplicationCommand,
} = require('@aws-sdk/client-s3');
const werelogs = require('werelogs');

const config = require('../../lib/Config');
const management = require('../../lib/management/index');

const logger = new werelogs.Logger('mdManagement:replication');

function getS3Client(endpoint) {
    const serviceCredentials =
          management.getLatestServiceAccountCredentials();
    // FIXME
    const keys = serviceCredentials.accounts[0].keys;
    const credentials = {
        accessKeyId: keys.access,
        secretAccessKey: keys.secret,
    };
    const s3Client = new S3Client({
        endpoint,
        credentials,
        region: 'us-east-1',
        forcePathStyle: true,
        tls: false,
        maxAttempts: 4,
        requestHandler: {
            connectionTimeout: 0,
            socketTimeout: 0,
        },
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
    
    const command = new PutBucketVersioningCommand(params);
    getS3Client(endpoint).send(command)
        .then(() => cb())
        .catch(err => {
            if (err.name === 'NoSuchBucket') {
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

    const command = new PutBucketReplicationCommand(params);
    getS3Client(endpoint).send(command)
        .then(() => {
            logger.debug('replication configuration apply done', {
                sourceBucket: bucketName });
            return cb();
        })
        .catch(err => {
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
    
    const command = new DeleteBucketReplicationCommand(params);
    getS3Client(endpoint).send(command)
        .then(() => {
            logger.debug('replication configuration deleted', {
                sourceBucket: bucketName });
            return cb();
        })
        .catch(err => {
            logger.debug('replication configuration deleted', {
                sourceBucket: bucketName, error: err });
            if (err.name === 'NoSuchBucket') {
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
