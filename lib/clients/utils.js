const { S3Client } = require('@aws-sdk/client-s3');
const { NodeHttpHandler } = require('@smithy/node-http-handler');
const BackbeatClient = require('./BackbeatClient');

const TIMEOUT_MS = 1000 * 60 * 2; // 2 minutes in ms

function attachReqUids(s3req, log) {
    // Support both AWS SDK v2 and v3 request patterns
    // v2: Uses .on('build') event pattern
    // v3: Uses middleware stack
    
    if (s3req.middlewareStack) {
        // AWS SDK v3 - use middleware
        s3req.middlewareStack.add(
            next => async args => {
                if (args.request && args.request.headers) {
                    // eslint-disable-next-line no-param-reassign
                    args.request.headers['X-Scal-Request-Uids'] = log.getSerializedUids();
                }
                return next(args);
            },
            {
                step: 'build',
                name: 'addRequestUids',
            }
        );
    } else if (s3req.on) {
        // AWS SDK v2 - use event listener
        s3req.on('build', () => {
            // eslint-disable-next-line no-param-reassign
            s3req.httpRequest.headers['X-Scal-Request-Uids'] =
                log.getSerializedUids();
        });
    }
}

function createS3Client(params) {
    const { transport, host, port, credentials, agent } = params;
    
    const config = {
        endpoint: `${transport}://${host}:${port}`,
        credentials,
        region: 'us-east-1',
        forcePathStyle: true,
        tls: transport === 'https',
        maxAttempts: 1,
    };

    // Add custom request handler if agent is provided
    if (agent) {
        config.requestHandler = new NodeHttpHandler({
            httpAgent: agent,
            httpsAgent: agent,
            connectionTimeout: TIMEOUT_MS,
            socketTimeout: TIMEOUT_MS,
        });
    }

    return new S3Client(config);
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
