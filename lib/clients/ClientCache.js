const S3 = require('aws-sdk/clients/s3');

const BackbeatClient = require('./BackbeatClient');

function createS3Client(params, log) {
    const { transport, host, port, credentials, agent } = params;
    return new S3({
        endpoint: `${transport}://${host}:${port}`,
        credentials,
        sslEnabled: transport === 'https',
        s3ForcePathStyle: true,
        signatureVersion: 'v4',
        httpOptions: { agent, timeout: 0 },
        maxRetries: 0,
    });
}

function createBackbeatClient(params, log) {
    const { transport, host, port, credentials, agent } = params;
    const endpoint = `${transport}://${host}:${port}`;
    return new BackbeatClient({
        endpoint,
        credentials,
        sslEnabled: transport === 'https',
        httpOptions: { agent, timeout: 0 },
        maxRetries: 0,
    });
}

module.exports = {
    createS3Client,
    createBackbeatClient,
};
