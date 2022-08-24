const S3 = require('aws-sdk/clients/s3');
const BackbeatClient = require('./BackbeatClient');

const TIMEOUT_MS = 1000 * 60 * 2; // 2 minutes in ms

function attachReqUids(s3req, log) {
    s3req.on('build', () => {
        // eslint-disable-next-line no-param-reassign
        s3req.httpRequest.headers['X-Scal-Request-Uids'] =
            log.getSerializedUids();
    });
}

function createS3Client(params) {
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

function createBackbeatClient(params) {
    const { transport, host, port, credentials, agent } = params;
    const endpoint = `${transport}://${host}:${port}`;
    return new BackbeatClient({
        endpoint,
        credentials,
        sslEnabled: transport === 'https',
        httpOptions: { agent, timeout: TIMEOUT_MS },
        maxRetries: 0,
    });
}

module.exports = {
    attachReqUids,
    createS3Client,
    createBackbeatClient,
    TIMEOUT_MS,
};
