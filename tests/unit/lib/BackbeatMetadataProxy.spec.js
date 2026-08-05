const assert = require('assert');
const sinon = require('sinon');
const BackOff = require('backo');

const BackbeatMetadataProxy = require('../../../lib/BackbeatMetadataProxy');
const {
    MAX_STALE_CREDENTIAL_RETRIES,
    isStaleCredentialError,
    retryOnStaleCredentials,
} = require('../../../lib/util/staleCredentialError');

// max.poll.interval.ms defaults to 300s: a correlated skew window must not let
// a single entry hold a concurrency slot anywhere near that long
const MAX_POLL_INTERVAL_MS = 300000;

const log = {
    trace: () => {},
    debug: () => {},
    info: () => {},
    warn: () => {},
    error: () => {},
    getSerializedUids: () => 'test-uid',
    end: () => log,
};

const authConfig = { type: 'account', account: 'bart' };

const mdParams = {
    bucket: 'source-bucket',
    objectKey: 'key',
    versionId: '0123456789',
};

function makeError(name, retryable = false) {
    const err = new Error(name);
    err.name = name;
    err.retryable = retryable;
    return err;
}

function newProxy() {
    return new BackbeatMetadataProxy('http://localhost:8000', authConfig);
}

describe('BackbeatMetadataProxy', () => {
    let proxy;

    beforeEach(() => {
        proxy = newProxy();
        // keep the retry delays out of the wall clock; the real backoff is
        // asserted separately below
        proxy.retryParams.backoff = { min: 1, max: 5, jitter: 0, factor: 1 };
    });

    afterEach(() => sinon.restore());

    describe('stale service-credential retries', () => {
        it('should retry a stale-credential 403 and succeed once a fresh endpoint answers', done => {
            const send = sinon.stub().resolves({ Body: '{}' });
            send.onCall(0).rejects(makeError('InvalidAccessKeyId'));
            send.onCall(1).rejects(makeError('InvalidAccessKeyId'));
            proxy.setBackbeatClient({ send });

            proxy.getMetadata(mdParams, log, (err, data) => {
                assert.ifError(err);
                assert.strictEqual(send.callCount, 3);
                assert.deepStrictEqual(data, { Body: '{}' });
                return done();
            });
        });

        it('should retry a SignatureDoesNotMatch failure', done => {
            const send = sinon.stub().resolves({ Body: '{}' });
            send.onCall(0).rejects(makeError('SignatureDoesNotMatch'));
            proxy.setBackbeatClient({ send });

            proxy.getMetadata(mdParams, log, err => {
                assert.ifError(err);
                assert.strictEqual(send.callCount, 2);
                return done();
            });
        });

        it('should give up once the stale-credential budget is exhausted', done => {
            const send = sinon.stub().rejects(makeError('InvalidAccessKeyId'));
            proxy.setBackbeatClient({ send });

            proxy.getMetadata(mdParams, log, err => {
                assert(err);
                assert.strictEqual(err.name, 'InvalidAccessKeyId');
                assert.strictEqual(send.callCount,
                    MAX_STALE_CREDENTIAL_RETRIES + 1);
                return done();
            });
        });

        it('should not retry an AccessDenied failure', done => {
            const send = sinon.stub().rejects(makeError('AccessDenied'));
            proxy.setBackbeatClient({ send });

            proxy.getMetadata(mdParams, log, err => {
                assert(err);
                assert.strictEqual(err.name, 'AccessDenied');
                assert.strictEqual(send.callCount, 1);
                return done();
            });
        });

        it('should not retry an unrelated non-retryable failure', done => {
            const send = sinon.stub().rejects(makeError('MethodNotAllowed'));
            proxy.setBackbeatClient({ send });

            proxy.getMetadata(mdParams, log, err => {
                assert(err);
                assert.strictEqual(send.callCount, 1);
                return done();
            });
        });

        it('should still retry an error flagged retryable by the client', done => {
            const send = sinon.stub().resolves({ Body: '{}' });
            send.onCall(0).rejects(makeError('InternalError', true));
            proxy.setBackbeatClient({ send });

            proxy.getMetadata(mdParams, log, err => {
                assert.ifError(err);
                assert.strictEqual(send.callCount, 2);
                return done();
            });
        });

        it('should retry putMetadata with the same metadata blob', done => {
            const mdBlob = Buffer.from('{"md-model-version":2}');
            const send = sinon.stub().resolves({});
            send.onCall(0).rejects(makeError('InvalidAccessKeyId'));
            proxy.setBackbeatClient({ send });

            proxy.putMetadata({ ...mdParams, mdBlob }, log, err => {
                assert.ifError(err);
                assert.strictEqual(send.callCount, 2);
                const bodies = send.getCalls().map(c => c.args[0].input.Body);
                assert.deepStrictEqual(bodies, [mdBlob, mdBlob]);
                return done();
            });
        });

        it('should retry headLocation', done => {
            const send = sinon.stub().resolves({});
            send.onCall(0).rejects(makeError('InvalidAccessKeyId'));
            proxy.setBackbeatClient({ send });

            const params = {
                ...mdParams,
                locations: [{ dataStoreName: 'us-east-1', key: 'key' }],
            };
            proxy.headLocation(params, log, err => {
                assert.ifError(err);
                assert.strictEqual(send.callCount, 2);
                return done();
            });
        });

        it('should keep the default retry budget far below max.poll.interval.ms', () => {
            const backoff = new BackOff(newProxy().retryParams.backoff);
            let totalMs = 0;
            for (let i = 0; i < MAX_STALE_CREDENTIAL_RETRIES; i++) {
                totalMs += backoff.duration();
            }
            assert(totalMs < MAX_POLL_INTERVAL_MS / 10,
                `stale-credential retries may span ${totalMs}ms`);
        });
    });
});

describe('staleCredentialError', () => {
    it('should recognise stale-credential failures only', () => {
        assert(isStaleCredentialError({ name: 'InvalidAccessKeyId' }));
        assert(isStaleCredentialError({ code: 'SignatureDoesNotMatch' }));
        assert(!isStaleCredentialError({ name: 'AccessDenied' }));
        assert(!isStaleCredentialError({ name: 'NoSuchEntity' }));
        assert(!isStaleCredentialError(undefined));
    });

    it('should give each predicate its own budget', () => {
        const err = makeError('InvalidAccessKeyId');
        const first = retryOnStaleCredentials(1);
        const second = retryOnStaleCredentials(1);

        assert.strictEqual(first(err), true);
        assert.strictEqual(first(err), false);
        assert.strictEqual(second(err), true);
    });
});
