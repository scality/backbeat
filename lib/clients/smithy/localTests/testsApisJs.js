const { 
    BackbeatClient,
    GetObjectCommand
} = require('@backbeat-service/client');

const bucketName = 'aaaa3';
const objectKey = 'oooo3';

const config = {
    endpoint: 'http://localhost:8000',
    region: 'us-east-1',
    credentials: {
        accessKeyId: 'accessKey1',
        secretAccessKey: 'verySecretKey1',
    },
    maxAttempts: 1,
};
const client = new BackbeatClient(config);

try {
    const getInput = {
        Bucket: bucketName,
        Key: objectKey,
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
    console.log('GetObject failed:', err.message);
}

