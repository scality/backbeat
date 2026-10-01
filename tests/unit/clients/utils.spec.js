const assert = require('assert');
const { S3ServiceException } = require('@aws-sdk/client-s3');

const { isRetryableMiddleware } = require('../../../lib/clients/utils');

function s3Error(name, httpStatusCode) {
    return new S3ServiceException({
        name,
        $fault: httpStatusCode >= 500 ? 'server' : 'client',
        $metadata: { httpStatusCode },
        message: name,
    });
}

function networkError(code) {
    return Object.assign(new Error(code), { code });
}

describe('isRetryableMiddleware', () => {
    const middleware = isRetryableMiddleware();

    async function classify(error) {
        const handler = middleware(async () => { throw error; });
        const thrown = await handler({}).then(() => null, err => err);
        assert.strictEqual(thrown, error);
        assert.strictEqual(error.$retryable, error.retryable);
        return error.retryable;
    }

    it('should pass responses through', async () => {
        const output = { $metadata: { httpStatusCode: 200 } };
        const handler = middleware(async () => output);
        assert.strictEqual(await handler({}), output);
    });

    [
        s3Error('InternalError', 500),
        s3Error('ServiceUnavailable', 503),
        s3Error('SlowDown', 503),
        s3Error('RequestTimeout', 400),
        s3Error('TooManyRequests', 429),
        s3Error('ThrottlingException', 400),
        networkError('ECONNRESET'),
        networkError('ECONNREFUSED'),
        networkError('EPIPE'),
        networkError('ETIMEDOUT'),
        Object.assign(new Error('timed out'), { name: 'TimeoutError' }),
    ].forEach(error => it(`should flag ${error.code || error.name} as retryable`, async () => {
        assert.strictEqual(await classify(error), true);
    }));

    [
        s3Error('NoSuchKey', 404),
        s3Error('AccessDenied', 403),
        s3Error('InvalidArgument', 400),
        Object.assign(new Error('aborted'), { name: 'AbortError' }),
        new Error('boom'),
    ].forEach(error => it(`should flag ${error.name} as not retryable`, async () => {
        assert.strictEqual(await classify(error), false);
    }));
});
