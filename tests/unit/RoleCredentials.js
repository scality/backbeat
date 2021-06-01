const assert = require('assert');
const http = require('http');
const { Client } = require('vaultclient');
const { Logger } = require('werelogs');
const { errors } = require('arsenal');
const { proxyPath } = require('../../extensions/replication/constants');
const RoleCredentials = require('../../lib/credentials/RoleCredentials');

const role = 'arn:aws:iam::1234567890:role/backbeat';
const extension = 'replication';
const AccessKeyId = 'ABCD1234567890XXXX';
const SecretAccessKey = 'qscwdvefb1234567890';
// const AssumedRoleUser = 'arn:aws:sts::1234567890:assumed-role/backbeat/1234';
const SessionToken = '1234567890-=+asdfg';
const vaultHost = '127.0.0.1';
const vaultPort = 8080;
let simulateServerError = false;
const server = http.createServer();
server.on('request', (req, res) => {
    const Expiration = Date.now() + 1000; // expire after 2 seconds
    let payload;
    if (simulateServerError) {
        payload = '{INVALIDJSON}';
    } else {
        payload = JSON.stringify({
            Credentials: {
                AccessKeyId,
                SecretAccessKey,
                SessionToken,
                Expiration,
            },
        });
    }
    res.writeHead(200, {
        'content-type': 'application/json',
        'content-length': Buffer.byteLength(payload),
    });
    res.end(payload);
});

function _assertCredentials(err, roleCredentials, cb) {
    if (err) {
        return cb(err);
    }
    const { accessKeyId, secretAccessKey, sessionToken, expired,
        expiration } = roleCredentials;
    assert.strictEqual(accessKeyId, AccessKeyId);
    assert.strictEqual(secretAccessKey, SecretAccessKey);
    assert.strictEqual(sessionToken, SessionToken);
    assert.strictEqual(expired, false);
    assert(expiration > Date.now());
    return cb();
}

describe('Credentials Manager', () => {
    let roleCredentials = null;
    let vaultServer = null;
    before(done => {
        const vaultclient = new Client(vaultHost, vaultPort, undefined,
            undefined, undefined, undefined, undefined, undefined, undefined,
            undefined, proxyPath);
        roleCredentials = new RoleCredentials(
            vaultclient, role, extension,
            new Logger('test:RoleCredentials').newRequestLogger('requids'));
        vaultServer = server.listen(vaultPort).on('error', done);
        done();
    });
    afterEach(() => {
        simulateServerError = false;
    });
    after(() => {
        roleCredentials = null;
        vaultServer.close();
    });

    it('should be able to acquire credentials on startup', done => {
        roleCredentials.get(err => _assertCredentials(err,
            roleCredentials, done));
    });

    it('should refresh credentials upon expiration', function test(done) {
        this.timeout(10000);
        roleCredentials.get(err => {
            if (err) {
                return done(err);
            }
            // wait for an extra second after timeout to ensure credentials
            // have expired
            const retryTimeout = (roleCredentials.expiration - Date.now()) +
                1000;
            return setTimeout(() => {
                assert(roleCredentials.expired === false,
                    'expected credentials to expire');
                roleCredentials.get(err => _assertCredentials(err,
                    roleCredentials, done));
            }, retryTimeout);
        });
    });

    it('should handle non arsenal errors on refresh', function test(done) {
        this.timeout(10000);
        const retryTimeout = (roleCredentials.expiration - Date.now()) +
            1000;
        return setTimeout(() => {
            assert.strictEqual(roleCredentials.expired, false);
            simulateServerError = true;
            roleCredentials.get(err => {
                assert(err);
                // check that err is another instance distinct from
                // the global arsenal error
                assert.notStrictEqual(err, errors.InternalError);
                done();
            });
        }, retryTimeout);
    });
});
