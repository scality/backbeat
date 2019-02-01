const async = require('async');
const AWS = require('aws-sdk');

const BackbeatClient = require('../../../lib/clients/BackbeatClient');

function _getClients(sourceInfo) {
    const { port } = sourceInfo;

    const s3sourceCredentials = new AWS.Credentials({
        accessKeyId: 'accessKey1',
        secretAccessKey: 'verySecretKey1',
    });

    const backbeatClient = new BackbeatClient({
        endpoint: `http://localhost:${port}`,
        credentials: s3sourceCredentials,
        sslEnabled: false,
        maxRetries: 0,
        httpOptions: { timeout: 0 },
    });
    const awsClient = new AWS.S3({
        endpoint: 'http://localhost:8000',
        credentials: s3sourceCredentials,
        sslEnabled: false,
        maxRetries: 0,
        httpOptions: { timeout: 0 },
        s3ForcePathStyle: true,
        signatureVersion: 'v4',
    });

    return { backbeatClient, awsClient };
}

/**
 * Create list of versioned buckets and objects in each bucket specified by
 * the metadata mock
 * @param {Object} sourceInfo - ingestion source info
 * @param {Function} cb - callback(error)
 * @return {undefined}
 */
function setupS3Mock(sourceInfo, cb) {
    const { bucket } = sourceInfo;
    const { backbeatClient, awsClient } = _getClients(sourceInfo);

    async.series([
        next => awsClient.createBucket({ Bucket: bucket }, next),
        next => awsClient.putBucketVersioning({
            Bucket: bucket,
            VersioningConfiguration: { Status: 'Enabled' },
        }, next),
        next => backbeatClient.getObjectList({ Bucket: bucket },
            (err, res) => {
                if (err) {
                    return next(err);
                }

                return async.each(res.Contents, (entry, done) => {
                    awsClient.putObject({
                        Bucket: bucket,
                        Key: entry.key,
                    }, done);
                }, next);
            }),
    ], cb);
}

/**
 * Remove all versions, delete markers, and the given bucket of a ingestion
 * source
 * @param {Object} sourceInfo - ingestion source info
 * @param {Function} cb - callback(error)
 * @return {undefined}
 */
function emptyAndDeleteVersionedBucket(sourceInfo, cb) {
    const { bucket } = sourceInfo;
    const { awsClient } = _getClients(sourceInfo);

    // won't need to worry about 1k+ objects pagination
    async.series([
        next => awsClient.listObjectVersions({ Bucket: bucket },
            (err, data) => {
                if (err) {
                    return next(err);
                }

                const list = [
                    ...data.Versions.map(v => ({
                        Key: v.Key,
                        VersionId: v.VersionId,
                    })),
                    ...data.DeleteMarkers.map(dm => ({
                        Key: dm.Key,
                        VersionId: dm.VersionId,
                    })),
                ];

                if (list.length === 0) {
                    return next();
                }

                return awsClient.deleteObjects({
                    Bucket: bucket,
                    Delete: { Objects: list },
                }, next);
            }),
        next => awsClient.deleteBucket({ Bucket: bucket }, next),
    ], cb);
}


module.exports = {
    setupS3Mock,
    emptyAndDeleteVersionedBucket
};
