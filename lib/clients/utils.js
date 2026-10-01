const { S3Client } = require('@aws-sdk/client-s3');
const { NodeHttpHandler } = require('@smithy/node-http-handler');
const { isThrottlingError, isTransientError } = require('@smithy/service-error-classification');

const TIMEOUT_MS = 1000 * 60 * 2; // 2 minutes in ms

function isRetryableMiddleware() {
    return next => async args => {
        try {
            return await next(args);
        } catch (error) {
            // The client runs with maxAttempts: 1, retries are handled by our
            // own retry loops: flag errors the SDK would have retried, see
            // https://docs.aws.amazon.com/sdkref/latest/guide/feature-retry-behavior.html
            const retryable = isTransientError(error) || isThrottlingError(error);

            error.$retryable = retryable;
            error.retryable = retryable;

            throw error;
        }
    };
}

function createS3Client(params) {
    const { transport, host, port, credentials, agent } = params;
    let s3Credentials;
    // With the v3 of the SDK, credentials can be passed as a
    // provider function that returns a promise, or as static credentials
    if (typeof credentials?.getCredentialsProvider === 'function') {
        s3Credentials = credentials.getCredentialsProvider();
    } else {
        s3Credentials = credentials;
    }
    
    const config = {
        endpoint: `${transport}://${host}:${port}`,
        credentials: s3Credentials,
        region: 'us-east-1',
        forcePathStyle: true,
        tls: transport === 'https',
        maxAttempts: 1,
    };

    if (agent) {
        config.requestHandler = new NodeHttpHandler({
            httpAgent: agent,
            httpsAgent: agent,
            connectionTimeout: TIMEOUT_MS,
            socketTimeout: TIMEOUT_MS,
        });
    }

    const client = new S3Client(config);
    client.middlewareStack.add(isRetryableMiddleware(), {
        step: 'deserialize',
        priority: 'high',
    });

    return client;
}

module.exports = {
    createS3Client,
    isRetryableMiddleware,
    TIMEOUT_MS,
};
