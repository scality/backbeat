import { 
    BackbeatClientConfig,
    BackbeatClient,
    GetBucketIndexesInput,
    GetBucketIndexesCommand,
    PutBucketIndexesInput,
    PutBucketIndexesCommand,
    DeleteBucketIndexesInput,
    DeleteBucketIndexesCommand,
} from '@backbeat-service/client';

const bucketName = 'aaaa3';
const objectKey = 'oooo3';

const config: BackbeatClientConfig = {
    endpoint: 'http://localhost:8000',
    region: 'us-east-1',
    credentials: {
        accessKeyId: 'accessKey1',
        secretAccessKey: 'verySecretKey1',
    },
};
const client = new BackbeatClient(config);

console.log("\n=== Testing PutBucketIndexes ===");
try {
    const indexData = JSON.stringify([
        {
            name: "testIndex1",
            keys: [
                { order: 1, key: "metadata.userId" }
            ]
        }
    ]);
    
    const putBucketIndexesInput: PutBucketIndexesInput = {
        Bucket: bucketName,
        Body: new TextEncoder().encode(indexData),
    };
    const putBucketIndexesCommand = new PutBucketIndexesCommand(putBucketIndexesInput);
    const result = await client.send(putBucketIndexesCommand);
    console.log('PutBucketIndexes succeeded:', result);
} catch (err) {
    console.log('PutBucketIndexes failed:', err);
}

console.log("\n=== Testing GetBucketIndexes ===");
try {
    const getBucketIndexesInput: GetBucketIndexesInput = {
        Bucket: bucketName,
    };
    const getBucketIndexesCommand = new GetBucketIndexesCommand(getBucketIndexesInput);
    const indexesData = await client.send(getBucketIndexesCommand);
    console.log('GetBucketIndexes succeeded:', indexesData);
    console.log('Indexes count:', indexesData.Indexes?.length);
} catch (err) {
    console.log('GetBucketIndexes failed:', err);
}

console.log("\n=== Testing DeleteBucketIndexes ===");
try {
    // Try matching the same format as PutBucketIndexes
    const indexesToDelete = JSON.stringify([
        {
            name: "testIndex1",
            keys: [
                { order: 1, key: "metadata.userId" }
            ]
        }
    ]);
    
    const deleteBucketIndexesInput: DeleteBucketIndexesInput = {
        Bucket: bucketName,
        Body: new TextEncoder().encode(indexesToDelete),
    };
    const deleteBucketIndexesCommand = new DeleteBucketIndexesCommand(deleteBucketIndexesInput);
    const result = await client.send(deleteBucketIndexesCommand);
    console.log('DeleteBucketIndexes succeeded:', result);
} catch (err) {
    console.log('DeleteBucketIndexes failed:', err);
}

console.log("\n=== Testing GetBucketIndexes Again ===");
try {
    const getBucketIndexesInput: GetBucketIndexesInput = {
        Bucket: bucketName,
    };
    const getBucketIndexesCommand = new GetBucketIndexesCommand(getBucketIndexesInput);
    const indexesData = await client.send(getBucketIndexesCommand);
    console.log('GetBucketIndexes succeeded:', indexesData);
} catch (err) {
    console.log('GetBucketIndexes failed:', err);
}
