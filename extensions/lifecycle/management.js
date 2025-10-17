const {
    S3Client,
    PutBucketLifecycleConfigurationCommand,
    DeleteBucketLifecycleCommand,
} = require('@aws-sdk/client-s3');
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
            connectionTimeout: TIMEOUT_MS,
            socketTimeout: TIMEOUT_MS,
        },
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
    
    (async () => {
        try {
            const command = new PutBucketLifecycleConfigurationCommand(params);
            await getS3Client(endpoint).send(command);
            logger.debug('lifecycle configuration apply done', {
                bucket: bucketName });
            cb();
        } catch (err) {
            logger.debug('lifecycle configuration apply done', {
                bucket: bucketName, error: err });
            if (err.name === 'NoSuchBucket') {
                cb();
            } else {
                cb(err);
            }
        }
    })();
}

function deleteLifecycleConfiguration(bucketName, cb) {
    logger.debug('deleting lifecycle configuration');
    const cfg = config.s3;
    const endpoint = `${cfg.host}:${cfg.port}`;

    const params = {
        Bucket: bucketName,
    };
    
    (async () => {
        try {
            const command = new DeleteBucketLifecycleCommand(params);
            await getS3Client(endpoint).send(command);
            logger.debug('lifecycle configuration deleted', {
                bucket: bucketName });
            cb();
        } catch (err) {
            logger.debug('lifecycle configuration deleted', {
                bucket: bucketName, error: err });
            if (err.name === 'NoSuchBucket') {
                cb();
            } else {
                cb(err);
            }
        }
    })();
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
