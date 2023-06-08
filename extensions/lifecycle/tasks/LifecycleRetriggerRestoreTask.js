'use strict'; // eslint-disable-line

const async = require('async');

const { errors } = require('arsenal');
const ObjectMD = require('arsenal').models.ObjectMD;

const { LifecycleMetrics } = require('../LifecycleMetrics');
const { LifecycleRequeueTask } = require('./LifecycleRequeueTask');

class LifecycleRetriggerRestoreTask extends LifecycleRequeueTask {
    /**
     * Process a lifecycle object entry
     *
     * @constructor
     * @param {LifecycleObjectProcessor} proc - object processor instance
     */
     constructor(proc) {
        const procState = proc.getStateVars();
        super();
        Object.assign(this, procState);
    }

    requeueObjectVersion(accountId, bucketName, objectKey, objectVersion, etag, try_, bucketLogger, cb) {
        const client = this.getBackbeatMetadataProxy(accountId);
        if (!client) {
            return cb(errors.InternalError.customizeDescription(
                `Unable to obtain client for account ${accountId}`,
            ));
        }

        const params = {
            bucket: bucketName,
            objectKey,
        };
        if (objectVersion) {
            params.versionId = objectVersion;
        }

        const log = this.logger.newRequestLogger(bucketLogger.getUids());
        log.addDefaultFields({
            accountId,
            bucketName,
            objectKey,
            objectVersion,
            etag,
            try: try_,
        });

        return client.getMetadata(params, log, (err, blob) => {
            LifecycleMetrics.onS3Request(log, 'getMetadata', 'requeueRestore', err);
            if (err) {
                return cb(err);
            }

            const { result: md, error } = ObjectMD.createFromBlob(blob.Body);
            if (error) {
                return cb(error);
            }

            if (this.shouldSkipObject(md, log)) {
                return cb(null, 0);
            }

            md.setUserMetadata({
                'x-amz-meta-scal-s3-restore-attempt': try_,
            });
            md.setOriginOp('s3:ObjectRestore:Retry')

            return client.putMetadata({ ...params, mdBlob: md.getSerialized() }, log,
                err => {
                    LifecycleMetrics.onS3Request(log, 'putMetadata', 'requeueRestore', err);
                    if (err) {
                        return cb(err);
                    }

                    return cb(null, 1);
                }
            );
        });
    }

    shouldSkipObject(md, log) {
        if (md.getArchive()?.archiveInfo?.archiveId === undefined) {
            log.error('object is not archived, skipping');
            return true;
        }
        
        return false;
    }
}

module.exports = {
    LifecycleRetriggerRestoreTask
};
