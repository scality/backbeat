const {
    S3Client,
    CreateBucketCommand,
    PutBucketVersioningCommand,
    PutObjectCommand,
    ListObjectVersionsCommand,
    DeleteObjectsCommand,
    DeleteBucketCommand,
} = require('@aws-sdk/client-s3');
const AWS = require('aws-sdk');

const BackbeatClient = require('../../../lib/clients/BackbeatClient');

function getClients(sourceInfo) {
    const { port } = sourceInfo;

    // BackbeatClient still uses AWS SDK v2
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
    
    // AWS S3 client uses AWS SDK v3
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

    (async () => {
        try {
            // Create bucket
            const createCommand = new CreateBucketCommand({ Bucket: bucket });
            await awsClient.send(createCommand);
            
            // Enable versioning
            const versionCommand = new PutBucketVersioningCommand({
                Bucket: bucket,
                VersioningConfiguration: { Status: 'Enabled' },
            });
            await awsClient.send(versionCommand);
            
            // Get and create objects
            backbeatClient.getObjectList({ Bucket: bucket }, async (err, res) => {
                if (err) {
                    cb(err);
                    return;
                }
                
                try {
                    for (const entry of res.Contents) {
                        const putCommand = new PutObjectCommand({
                            Bucket: bucket,
                            Key: entry.key,
                        });
                        await awsClient.send(putCommand);
                    }
                    cb();
                } catch (error) {
                    cb(error);
                }
            });
        } catch (err) {
            cb(err);
        }
    })();
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

    (async () => {
        try {
            // List all versions
            const listCommand = new ListObjectVersionsCommand({ Bucket: bucket });
            const data = await awsClient.send(listCommand);
            
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
                await awsClient.send(deleteCommand);
            }
            
            // Delete bucket
            const deleteBucketCommand = new DeleteBucketCommand({ Bucket: bucket });
            await awsClient.send(deleteBucketCommand);
            
            cb();
        } catch (err) {
            cb(err);
        }
    })();
}


module.exports = {
    getClients,
    setupS3Mock,
    emptyAndDeleteVersionedBucket
};
