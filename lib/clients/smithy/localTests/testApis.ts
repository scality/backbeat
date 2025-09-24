import { 
    PutDataInput,
    PutDataOutput,
    BackbeatClientConfig,
    BackbeatClient,
    PutDataCommand,
    GetObjectInput,
    GetObjectCommand,
    GetObjectListInput,
    GetObjectListCommand,
    BatchDeleteLocation,
    BatchDeleteInput,
    BatchDeleteCommand
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
    maxAttempts: 1,
};

const client = new BackbeatClient(config);

console.log("\n=== Testing PutData ===");
try {
    const bodyText = 'newData!';
    const body = new TextEncoder().encode(bodyText);
    const crypto = require('crypto');
    const contentMD5 = crypto.createHash('md5').update(body).digest('hex');

    const putInput: PutDataInput = {
        Bucket: bucketName,
        Key: objectKey,
        CanonicalID: '39383234313039353433383937313939393939395247303031202036353034352e30',
        ContentMD5: contentMD5,
        Body: body,
        VersioningRequired: true
    };
    const command = new PutDataCommand(putInput);
    const data = await client.send(command);
    console.log('PutData succeeded 1:', data);
    console.log('PutData succeeded 2:', data.Location[0].key);
} catch (err: any) {
    console.log('PutData failed:', err);
}


console.log("=== Testing GetObject ===");
try {
    const getInput: GetObjectInput = {
        Bucket: bucketName,
        Key: objectKey,
        // CanonicalID: '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be',
    };
    const getCommand = new GetObjectCommand(getInput);
    const getData = await client.send(getCommand);
    console.log('GetObject succeeded:', getData);
    if (getData.Body) {
        const bodyBytes = new Uint8Array(getData.Body);
        const bodyString = new TextDecoder().decode(bodyBytes);
        console.log("Retrieved content:", bodyString);
    }
} catch (err) {
    console.log('GetObject failed 1:', err);
    console.log('GetObject failed 2:', err.$metadata.httpStatusCode === 404);
}

console.log("\n=== Testing GetObjectList ===");
// try {
//     const getInput: GetObjectListInput = {
//         Bucket: bucketName,
//     };
//     const getCommand = new GetObjectListCommand(getInput);
//     const getData = await client.send(getCommand);
//     console.log('GetObjectList succeeded:', getData);
//     console.log('Contents count:', getData.Contents?.length);
//     console.log('IsTruncated:', getData.IsTruncated);
// } catch (err) {
//     console.log('GetObjectList failed:', err);
// }

// console.log("\n=== Testing BatchDelete ===");
// try {
//     const locations: BatchDeleteLocation[] = [
//         {
//             dataStoreName: "mem", 
//             key: "aaaa1",
//             size: 8,
//             dataStoreVersionId: "v1"
//         }
//     ];
//     const batchDeleteInput: BatchDeleteInput = {
//         Bucket: bucketName,
//         Key: objectKey,
//         IfUnmodifiedSince: new Date().toISOString(),
//         StorageClass: "STANDARD",
//         Tags: JSON.stringify({ purpose: "test" }),
//         ContentType: "application/octet-stream",
//         Locations: locations
//     };
    
//     const batchDeleteCommand = new BatchDeleteCommand(batchDeleteInput);
//     const result = await client.send(batchDeleteCommand);
//     console.log('BatchDelete succeeded:', result);
// } catch (err: any) {
//     console.log('BatchDelete failed, err:', err);
// }
