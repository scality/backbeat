import { S3Client, PutObjectCommand, CreateBucketCommand, PutBucketVersioningCommand } from '@aws-sdk/client-s3';

const client = new S3Client({
    endpoint: 'http://localhost:8000',
    region: 'us-east-1',
    credentials: {
        accessKeyId: 'accessKey1',
        secretAccessKey: 'verySecretKey1',
    },
    forcePathStyle: true,
});

console.log('Testing standard S3 client with versioned bucket...');

const bucket = 'aaaa3'
const key = 'oooo3'
try {
    const createBucketCommand = new CreateBucketCommand({
        Bucket: bucket
    });
    await client.send(createBucketCommand);
    
    const versioningCommand = new PutBucketVersioningCommand({
        Bucket: bucket,
        VersioningConfiguration: {
            Status: 'Enabled'
        }
    });
    await client.send(versioningCommand);
    
    const putObjectCommand = new PutObjectCommand({
        Bucket: bucket,
        Key: key,
        Body: 'Hey!!',
    });
    const result = await client.send(putObjectCommand);
} catch (error) {
    console.log('S3 operation failed:', error);
}
