import { 
    BackbeatClientConfig,
    BackbeatClient,
    ListLifecycleCurrentsInput,
    ListLifecycleCurrentsCommand,
    ListLifecycleNonCurrentsInput,
    ListLifecycleNonCurrentsCommand,
    ListLifecycleOrphansInput,
    ListLifecycleOrphansCommand,
    DeleteObjectFromExpirationInput,
    DeleteObjectFromExpirationCommand,
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

console.log('1. Testing ListLifecycleCurrents...');
try {
    const listInput: ListLifecycleCurrentsInput = {
        Bucket: bucketName,
        MaxKeys: 5,
    };
    const command = new ListLifecycleCurrentsCommand(listInput);
    const result = await client.send(command);
    console.log('ListLifecycleCurrents succeeded!', result);
} catch (err: any) {
    console.log('ListLifecycleCurrents failed:', err);
}

console.log('2. Testing ListLifecycleNonCurrents...');
try {
    const listInput: ListLifecycleNonCurrentsInput = {
        Bucket: bucketName,
        MaxKeys: 5,
    };
    const command = new ListLifecycleNonCurrentsCommand(listInput);
    const result = await client.send(command);
    console.log('ListLifecycleNonCurrents succeeded!', result);
} catch (err: any) {
    console.log('ListLifecycleNonCurrents failed:', err);
}

console.log('3. Testing ListLifecycleOrphans...');
try {
    const listInput: ListLifecycleOrphansInput = {
        Bucket: bucketName,
        MaxKeys: 5,
    };
    const command = new ListLifecycleOrphansCommand(listInput);
    const result = await client.send(command);
    console.log('ListLifecycleOrphans succeeded!', result);
} catch (err: any) {
    console.log('ListLifecycleOrphans failed:', err);
}

console.log('4. Testing DeleteObjectFromExpiration...');
try {
    const deleteInput: DeleteObjectFromExpirationInput = {
        Bucket: bucketName,
        Key: objectKey,
    };
    const command = new DeleteObjectFromExpirationCommand(deleteInput);
    const result = await client.send(command);
    console.log('DeleteObjectFromExpiration succeeded!', result);
} catch (err: any) {
    console.log('DeleteObjectFromExpiration failed:', err);
}
