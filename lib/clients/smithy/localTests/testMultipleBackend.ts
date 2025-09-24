import { 
    BackbeatClientConfig,
    BackbeatClient,
    MultipleBackendPutObjectInput,
    MultipleBackendPutObjectCommand,
    MultipleBackendDeleteObjectInput,
    MultipleBackendDeleteObjectCommand,
    MultipleBackendHeadObjectInput,
    MultipleBackendHeadObjectCommand,
    MultipleBackendInitiateMPUInput,
    MultipleBackendInitiateMPUCommand,
    MultipleBackendPutMPUPartInput,
    MultipleBackendPutMPUPartCommand,
    MultipleBackendCompleteMPUInput,
    MultipleBackendCompleteMPUCommand,
    MultipleBackendAbortMPUInput,
    MultipleBackendAbortMPUCommand,
    MultipleBackendPutObjectTaggingInput,
    MultipleBackendPutObjectTaggingCommand,
    MultipleBackendDeleteObjectTaggingInput,
    MultipleBackendDeleteObjectTaggingCommand,
} from '@backbeat-service/client';

const bucketName = 'aaaa1';
const objectKey = 'test-key-multiple-backend';

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

console.log('1. Testing MultipleBackendPutObject...');
let locationKey = '';
try {
    const testData = new TextEncoder().encode('Test data for multiple backend object');
    const crypto = require('crypto');
    const contentMD5 = crypto.createHash('md5').update(testData).digest('hex');
    const putInput: MultipleBackendPutObjectInput = {
        Bucket: bucketName,
        Key: objectKey,
        Body: testData,
        ContentMD5: contentMD5,
        ContentType: 'text/plain',
        UserMetaData: JSON.stringify({
            'custom-meta': 'test-value',
            'another-meta': 'another-value'
        }),
        Tags: JSON.stringify({
            'tag1': 'value1',
            'tag2': 'value2'
        }),
        StorageClass: 'us-east-1',
        StorageType: 'file',
        CanonicalID: '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be'
    };
    
    const command = new MultipleBackendPutObjectCommand(putInput);
    const result = await client.send(command);
    console.log('MultipleBackendPutObject succeeded!', result);
    locationKey = result.location?.[0]?.key || '';
} catch (err: any) {
    console.log('MultipleBackendPutObject failed:', err);
}

console.log('\n2. Testing MultipleBackendHeadObject...');
try {
    const headInput: MultipleBackendHeadObjectInput = {
        Bucket: bucketName,
        Key: objectKey,
        Locations: JSON.stringify([{
            key: locationKey,
            dataStoreName: 'us-east-1'
        }])
    };
    const headCommand = new MultipleBackendHeadObjectCommand(headInput);
    const headResult = await client.send(headCommand);
    console.log('MultipleBackendHeadObject succeeded!', headResult);
} catch (err: any) {
    console.log('MultipleBackendHeadObject failed:', err);
}

console.log('\n3. Testing MultipleBackendPutObjectTagging...');
try {
    const tagData = new TextEncoder().encode(JSON.stringify({
        TagSet: [
            { Key: 'Environment', Value: 'Test' },
            { Key: 'Project', Value: 'Backbeat' }
        ]
    }));
    const putTaggingInput: MultipleBackendPutObjectTaggingInput = {
        Bucket: bucketName,
        Key: objectKey,
        StorageClass: 'us-east-1',
        StorageType: 'file',
        Tags: JSON.stringify({
            'Environment': 'Test',
            'Project': 'Backbeat'
        }),
        Body: tagData,
        DataStoreVersionId: 'v1',
        SourceBucket: "aBucket",
        ReplicationEndpointSite: "aVal"
    };
    const putTaggingCommand = new MultipleBackendPutObjectTaggingCommand(putTaggingInput);
    const putTaggingResult = await client.send(putTaggingCommand);
    console.log('MultipleBackendPutObjectTagging succeeded!', putTaggingResult);
} catch (err: any) {
    console.log('MultipleBackendPutObjectTagging failed:', err);
}

console.log('\n4. Testing MultipleBackendDeleteObjectTagging...');
try {
    const deleteTaggingInput: MultipleBackendDeleteObjectTaggingInput = {
        Bucket: bucketName,
        Key: objectKey,
        StorageClass: 'us-east-1',
        StorageType: 'file',
        Body: new Uint8Array(0)
    };
    
    const deleteTaggingCommand = new MultipleBackendDeleteObjectTaggingCommand(deleteTaggingInput);
    const deleteTaggingResult = await client.send(deleteTaggingCommand);
    console.log('MultipleBackendDeleteObjectTagging succeeded!', deleteTaggingResult);
} catch (err: any) {
    console.log('MultipleBackendDeleteObjectTagging failed:', err);
}

console.log('\n5. Testing MultipleBackendDeleteObject...');
try {
    const deleteInput: MultipleBackendDeleteObjectInput = {
        Bucket: bucketName,
        Key: objectKey,
        StorageClass: 'us-east-1',
        StorageType: 'file'
    };
    const command = new MultipleBackendDeleteObjectCommand(deleteInput);
    const result = await client.send(command);
    console.log('MultipleBackendDeleteObject succeeded!', result);
} catch (err: any) {
    console.log('MultipleBackendDeleteObject failed:', err);
}

let uploadId: string | undefined;
console.log('6. Testing MultipleBackendInitiateMPU...');
try {
    const initiateMPUInput: MultipleBackendInitiateMPUInput = {
        Bucket: bucketName,
        Key: `${objectKey}-mpu`,
        StorageClass: 'us-east-1',
        StorageType: 'file',
        ContentType: 'text/plain',
        Tags: JSON.stringify({
            'mpu-tag': 'test-mpu'
        }),
        Body: new Uint8Array(0)
    };
    
    const initiateMPUCommand = new MultipleBackendInitiateMPUCommand(initiateMPUInput);
    const initiateMPUResult = await client.send(initiateMPUCommand);
    uploadId = initiateMPUResult.uploadId;
    console.log('MultipleBackendInitiateMPU succeeded!', initiateMPUResult);
    console.log('UploadId:', uploadId);
} catch (err: any) {
    console.log('MultipleBackendInitiateMPU failed:', err);
}

if (uploadId) {
    console.log('7. Testing MultipleBackendPutMPUPart...');
    try {
        const partData = new TextEncoder().encode('This is part 1 of the multipart upload');
        const putPartInput: MultipleBackendPutMPUPartInput = {
            Bucket: bucketName,
            Key: `${objectKey}-mpu`,
            StorageClass: 'us-east-1',
            StorageType: 'file',
            PartNumber: 1,
            UploadId: uploadId,
            Body: partData
        };
        
        const putPartCommand = new MultipleBackendPutMPUPartCommand(putPartInput);
        const putPartResult = await client.send(putPartCommand);
        console.log('MultipleBackendPutMPUPart succeeded!', putPartResult);
    } catch (err: any) {
        console.log('MultipleBackendPutMPUPart failed:', err);
    }

    console.log('8. Testing MultipleBackendCompleteMPU...');
    try {
        const completeMPUInput: MultipleBackendCompleteMPUInput = {
            Bucket: bucketName,
            Key: `${objectKey}-mpu`,
            StorageClass: 'us-east-1',
            StorageType: 'file',
            UploadId: uploadId,
            ContentType: 'text/plain',
            Body: new TextEncoder().encode(JSON.stringify({
                parts: [{
                    partNumber: 1,
                    etag: 'dummy-etag'
                }]
            }))
        };
        
        const completeMPUCommand = new MultipleBackendCompleteMPUCommand(completeMPUInput);
        const completeMPUResult = await client.send(completeMPUCommand);
        console.log('MultipleBackendCompleteMPU succeeded!', completeMPUResult);
    } catch (err: any) {
        console.log('MultipleBackendCompleteMPU failed:', err);
        
        // If complete fails, try to abort
        console.log('9. Testing MultipleBackendAbortMPU (cleanup)...');
        try {
            const abortMPUInput: MultipleBackendAbortMPUInput = {
                Bucket: bucketName,
                Key: `${objectKey}-mpu`,
                StorageClass: 'us-east-1',
                StorageType: 'file',
                UploadId: uploadId
            };

            const abortMPUCommand = new MultipleBackendAbortMPUCommand(abortMPUInput);
            await client.send(abortMPUCommand);
            console.log('MultipleBackendAbortMPU succeeded (cleanup)!');
        } catch (abortErr: any) {
            console.log('MultipleBackendAbortMPU failed:', abortErr);
        }
    }
}
