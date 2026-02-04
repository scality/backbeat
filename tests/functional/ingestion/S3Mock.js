const {
    S3Client,
    CreateBucketCommand,
    PutBucketVersioningCommand,
    PutObjectCommand,
    ListObjectVersionsCommand,
    DeleteObjectsCommand,
    DeleteBucketCommand,
} = require('@aws-sdk/client-s3');

const { BackbeatRoutesClient, GetObjectListCommand } = require('@scality/cloudserverclient');

function getClients(sourceInfo) {
    const { port } = sourceInfo;

    const backbeatClient = new BackbeatRoutesClient({
        endpoint: `http://localhost:${port}`,
        credentials: {
            accessKeyId: 'accessKey1',
            secretAccessKey: 'verySecretKey1',
        },
        region: 'us-east-1',
    });
    
    const awsClient = new S3Client({
        endpoint: 'http://localhost:8000',
        credentials: {
            accessKeyId: 'accessKey1',
            secretAccessKey: 'verySecretKey1',
        },
        region: 'us-east-1',
        forcePathStyle: true,
        tls: false,
        maxAttempts: 1,
        requestHandler: {
            connectionTimeout: 0,
            socketTimeout: 0,
        },
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
    const { backbeatClient, awsClient } = getClients(sourceInfo);

    const createCommand = new CreateBucketCommand({ Bucket: bucket });
    awsClient.send(createCommand)
        .then(() => {
            const versionCommand = new PutBucketVersioningCommand({
                Bucket: bucket,
                VersioningConfiguration: { Status: 'Enabled' },
            });
            return awsClient.send(versionCommand);
        })
        .then(() => backbeatClient.send(new GetObjectListCommand({ Bucket: bucket })))
        .then(res => {
            const promises = res.Contents.map(entry => {
                const putCommand = new PutObjectCommand({
                    Bucket: bucket,
                    Key: entry.key,
                });
                return awsClient.send(putCommand);
            });
            return Promise.all(promises);
        })
        .then(() => cb())
        .catch(err => cb(err));
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
    const { awsClient } = getClients(sourceInfo);

    const listCommand = new ListObjectVersionsCommand({ Bucket: bucket });
    awsClient.send(listCommand)
        .then(data => {
            const list = [
                ...(data.Versions || []).map(v => ({
                    Key: v.Key,
                    VersionId: v.VersionId,
                })),
                ...(data.DeleteMarkers || []).map(dm => ({
                    Key: dm.Key,
                    VersionId: dm.VersionId,
                })),
            ];

            if (list.length > 0) {
                const deleteCommand = new DeleteObjectsCommand({
                    Bucket: bucket,
                    Delete: { Objects: list },
                });
                return awsClient.send(deleteCommand);
            }
            return Promise.resolve();
        })
        .then(() => {
            const deleteBucketCommand = new DeleteBucketCommand({ Bucket: bucket });
            return awsClient.send(deleteBucketCommand);
        })
        .then(() => cb())
        .catch(err => cb(err));
}


module.exports = {
    setupS3Mock,
    emptyAndDeleteVersionedBucket
};
