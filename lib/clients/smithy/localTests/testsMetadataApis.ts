import { 
    BackbeatClientConfig,
    BackbeatClient,
    GetMetadataInput,
    GetMetadataCommand,
    PutMetadataInput,
    PutMetadataCommand,
    GetBucketMetadataInput,
    GetBucketMetadataCommand,
} from '@backbeat-service/client';


const config: BackbeatClientConfig = {
    endpoint: 'http://localhost:8000',
    region: 'us-east-1',
    credentials: {
        accessKeyId: 'accessKey1',
        secretAccessKey: 'verySecretKey1',
    },
    maxAttempts: 1
};
const client = new BackbeatClient(config);
const bucketName = 'aaaa3';
const objectKey = 'oooo3';

console.log("\n=== Testing GetMetadata ===");
try {
    const getMetadataInput: GetMetadataInput = {
        Bucket: bucketName,
        Key: objectKey,
    };
    const getMetadataCommand = new GetMetadataCommand(getMetadataInput);
    const metadataData = await client.send(getMetadataCommand);
    console.log('GetMetadata succeeded - Metadata:', metadataData.Body?.toString());
} catch (err) {
    console.log('GetMetadata failed:', err);
}

console.log('Testing PutMetadata...');
try {
    const metadataObj = {
        "content-length": 100,
        "content-type": "text/plain",
        "x-amz-meta-custom": "test-value",
        "last-modified": new Date().toISOString(),
        "etag": "\"d41d8cd98f00b204e9800998ecf8427e\"",
        "x-amz-version-id": "null"
    };
    
    const metadataString = JSON.stringify(metadataObj);
    const metadataBuffer = new TextEncoder().encode(metadataString);
    
    const crypto = require('crypto');
    const contentMD5 = crypto.createHash('md5').update(metadataBuffer).digest('hex');
    const putInput: PutMetadataInput = {
        Bucket: bucketName,
        Key: objectKey,
        Body: metadataBuffer,
        // ContentMD5: contentMD5,
        // ReplicationContent: 'METADATA',
        // VersioningRequired: false,
        // AccountId: '123456789012'
    };
    
    const command = new PutMetadataCommand(putInput);
    const result = await client.send(command);
    console.log('PutMetadata succeeded!', result);
} catch (err: any) {
    console.log('PutMetadata failed:', err);
}


console.log("\n=== Testing GetBucketMetadata ===");
try {
    const getBucketMetadataInput: GetBucketMetadataInput = {
        Bucket: bucketName,
    };
    const getBucketMetadataCommand = new GetBucketMetadataCommand(getBucketMetadataInput);
    const bucketMetadata = await client.send(getBucketMetadataCommand);
    console.log('GetBucketMetadata succeeded:', bucketMetadata);
} catch (err: any) {
    console.log('GetBucketMetadata err:', err);
}

