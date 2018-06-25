const AWS = require('aws-sdk');
const werelogs = require('werelogs');

const config = require('../../conf/Config');
const management = require('../../lib/management/index');

const logger = new werelogs.Logger('mdManagement:lifecycle');

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

function putLifecycleConfiguration(bucketName, workflows, cb) {
    logger.debug('updating lifecycle configuration');
    const cfg = config.s3;
    const endpoint = `${cfg.host}:${cfg.port}`;
    console.log('bucketName!!!', bucketName);
    console.log('workflows!!!', workflows);
    const params = {
        Bucket: bucketName,
        LifecycleConfiguration: {
            Rules: workflows.map(wf => ({
                Expiration: {
                    Days: wf.currentVersionTriggerDelayDays,
                },
                ID: wf.workflowId,
                Filter: {
                    Prefix: wf.filter ? wf.filter.objectKeyPrefix : undefined,
                },
                Status: wf.enabled ? 'Enabled' : 'Disabled',
                NoncurrentVersionExpiration: {
                    NoncurrentDays: wf.previousVersionTriggerDelayDays,
                },
            })),
        },
    };
    getS3Client(endpoint).putBucketLifecycleConfiguration(params, err => {
        logger.debug('lifecycle configuration apply done', {
            bucket: bucketName, error: err });
        if (err && err.code === 'NoSuchBucket') {
            return cb();
        }
        return cb(err);
    });
}

function deleteLifecycleConfiguration(bucketName, cb) {
    logger.debug('deleting lifecycle configuration');
    const cfg = config.extensions.lifecycle.source.s3;
    const endpoint = `${cfg.host}:${cfg.port}`;

    const params = {
        Bucket: bucketName,
    };
    getS3Client(endpoint).deleteBucketLifecycle(params, err => {
        logger.debug('lifecycle configuration deleted', {
            bucket: bucketName, error: err });
        if (err && err.code === 'NoSuchBucket') {
            return cb();
        }
        return cb(err);
    });
}

function applyBucketLifecycleWorkflows(bucketName, bucketWorkflows,
                                       workflowUpdates, cb) {
    if (bucketWorkflows.length > 0) {
        putLifecycleConfiguration(bucketName, bucketWorkflows, cb);
    } else {
        deleteLifecycleConfiguration(bucketName, cb);
    }
}

module.exports = {
    applyBucketLifecycleWorkflows,
};
