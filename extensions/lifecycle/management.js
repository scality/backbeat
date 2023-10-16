const AWS = require('aws-sdk');
const werelogs = require('werelogs');

const config = require('../../lib/Config');
const management = require('../../lib/management/index');
const { TIMEOUT_MS } = require('../../lib/clients/utils');

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
        httpOptions: { timeout: TIMEOUT_MS },
        maxRetries: 3,
    });
    return s3Client;
}

function putLifecycleConfiguration(bucketName, workflows, cb) {
    logger.debug('updating lifecycle configuration');
    const cfg = config.s3;
    const endpoint = `${cfg.host}:${cfg.port}`;
    const params = {
        Bucket: bucketName,
        LifecycleConfiguration: {
            Rules: workflows.map(wf => {
                const workflow = {
                    ID: wf.workflowId,
                    Status: wf.enabled ? 'Enabled' : 'Disabled',
                };
                if (wf.currentVersionTriggerDelayDays) {
                    if (wf.type.includes('expiration')) {
                        workflow.Expiration = {
                            Days: wf.currentVersionTriggerDelayDays,
                        };
                    }
                    if (wf.type.includes('transition')) {
                        workflow.Transitions = [{
                            Days: wf.currentVersionTriggerDelayDays,
                            StorageClass: wf.currentVersionLocations[0].name,
                        }];
                    }
                }
                if (wf.filter && wf.filter.objectKeyPrefix) {
                    workflow.Filter = {
                        Prefix: wf.filter.objectKeyPrefix,
                    };
                } else {
                    workflow.Filter = {};
                }
                if (wf.previousVersionTriggerDelayDays) {
                    if (wf.type.includes('expiration')) {
                        workflow.NoncurrentVersionExpiration = {
                            NoncurrentDays: wf.previousVersionTriggerDelayDays,
                        };
                    }
                }
                return workflow;
            }),
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
    const cfg = config.s3;
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
