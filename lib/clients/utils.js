const { S3Client } = require('@aws-sdk/client-s3');
const { NodeHttpHandler } = require('@smithy/node-http-handler');
const {
    TRANSIENT_ERROR_STATUS_CODES,
    NODEJS_TIMEOUT_ERROR_CODES,
    TRANSIENT_ERROR_CODES,
    THROTTLING_ERROR_CODES,
} = require('@smithy/service-error-classification/dist-es/constants');

const TIMEOUT_MS = 1000 * 60 * 2; // 2 minutes in ms

function attachReqUids(s3req, log) {
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
            name: 'attachReqUids',
        }
    );
}

function isRetryableMiddleware() {
    return next => async args => {
        try {
            return await next(args);
        } catch (error) {
            // Set retryable flag using error classification logic from:
            // eslint-disable-next-line max-len
            // https://github.com/smithy-lang/smithy-typescript/blob/main/packages/service-error-classification/src/index.ts
            // https://docs.aws.amazon.com/sdkref/latest/guide/feature-retry-behavior.html
            const code = error.code || error.Code;
            const statusCode = error.$metadata?.httpStatusCode;
            const retryable = TRANSIENT_ERROR_STATUS_CODES.includes(statusCode) ||
                NODEJS_TIMEOUT_ERROR_CODES.includes(code || '') ||
                TRANSIENT_ERROR_CODES.includes(code || '') ||
                THROTTLING_ERROR_CODES.includes(code || '') ||
                statusCode === 429;
            
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
    attachReqUids,
    createS3Client,
    isRetryableMiddleware,
    TIMEOUT_MS,
};
