const assert = require('assert');
const http = require('http');
const { Client } = require('vaultclient');
const CredentialsManager = require('../../credentials/CredentialsManager');

const role = 'arn:aws:iam::1234567890:role/backbeat';
const extension = 'replication';
const AccessKeyId = 'ABCD1234567890XXXX';
const SecretAccessKey = 'qscwdvefb1234567890';
// const AssumedRoleUser = 'arn:aws:sts::1234567890:assumed-role/backbeat/1234';
const SessionToken = '1234567890-=+asdfg';
const vaultHost = '127.0.0.1';
const vaultPort = 8080;
const server = http.createServer((req, res) => {
    const Expiration = Date.now() + 1000; // expire on 1 second
    const payload = JSON.stringify({
        Credentials: {
            AccessKeyId,
            SecretAccessKey,
            SessionToken,
            Expiration,
        },
    });
    res.writeHead(200, {
        'content-type': 'application/json',
        'content-length': Buffer.byteLength(payload),
    });
    res.end(payload);
});

function _assertCredentials(err, credentialsManager, cb) {
    if (err) {
        return cb(err);
    }
    const { accessKeyId, secretAccessKey, sessionToken, expired,
        expiration } = credentialsManager;
    assert.strictEqual(accessKeyId, AccessKeyId);
    assert.strictEqual(secretAccessKey, SecretAccessKey);
    assert.strictEqual(sessionToken, SessionToken);
    assert.strictEqual(expired, false);
    assert(expiration > Date.now());
    return cb();
}


describe('Credentials Manager', () => {
    let credentialsManager = null;
    let vaultServer = null;
    before(done => {
        const vaultclient = new Client(vaultHost, vaultPort);
        credentialsManager = new CredentialsManager(vaultclient, role,
            extension);
        vaultServer = server.listen(vaultPort).on('error', done);
        done();
    });
    after(() => {
        credentialsManager = null;
        vaultServer.close();
    });

    it('should be able to acquire credentials on startup', done => {
        credentialsManager.get(err => _assertCredentials(err,
            credentialsManager, done));
    });

    it('should refresh credentials upon expiration', function test(done) {
        this.timeout(10000);
        credentialsManager.get(err => {
            if (err) {
                return done(err);
            }
            // wait for an extra second after timeout to ensure credentials
            // have expired
            const retryTimeout = (credentialsManager.expiration - Date.now()) +
                1000;
            return setTimeout(() => {
                assert(credentialsManager.expired === false,
                    'expected credentials to expire');
                credentialsManager.get(err => _assertCredentials(err,
                    credentialsManager, done));
            }, retryTimeout);
        });
    });
});
