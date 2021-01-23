const assert = require('assert');
const http = require('http');
const { Client } = require('vaultclient');
const { Logger } = require('werelogs');
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
    const Expiration = Date.now() + 1000; // expire on 1 second
    const payload = JSON.stringify({
        Credentials: {
            AccessKeyId,
            SecretAccessKey,
            SessionToken,
            Expiration,
        },
    });
    if (simulateServerError) {
        req.socket.destroy();
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
            new Logger('test:RoleCredentials').newRequestLogger('requids'),
            110);
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

    it('should use same credentials if not expired or about to expire', function test(done) {
        this.timeout(10000);
        roleCredentials.get(err => {
            if (err) {
                return done(err);
            }
            const currentExpiration = roleCredentials.expiration;
            // wait for less than the expiration time minus the
            // anticipation delay to ensure credentials have not
            // expired
            const retryTimeout = (roleCredentials.expiration - Date.now()) - 200;
            return setTimeout(() => roleCredentials.get(
                err => _assertCredentials(err, roleCredentials, err => {
                    assert.ifError(err);
                    // expiration should not have changed, meaning
                    // credentials have not been refreshed
                    assert.strictEqual(currentExpiration, roleCredentials.expiration);
                    done();
                })), retryTimeout);
        });
    });

    it('should refresh credentials upon expiration', function test(done) {
        this.timeout(10000);
        roleCredentials.get(err => {
            if (err) {
                return done(err);
            }
            const currentExpiration = roleCredentials.expiration;
            // wait for more than the expiration time to ensure
            // credentials have expired
            const retryTimeout = (roleCredentials.expiration - Date.now()) + 1000;
            return setTimeout(() => roleCredentials.get(
                err => _assertCredentials(err, roleCredentials, err => {
                    assert.ifError(err);
                    // expiration should have changed, meaning
                    // credentials have been refreshed
                    assert.notStrictEqual(currentExpiration, roleCredentials.expiration);
                    done();
                })), retryTimeout);
        });
    });

    it('should refresh credentials a bit before expiration', function test(done) {
        this.timeout(10000);
        roleCredentials.get(err => {
            if (err) {
                return done(err);
            }
            const currentExpiration = roleCredentials.expiration;
            // wait for slightly less than the expiration time but
            // more than the anticipation delay for renewing
            // credentials about to expire
            const retryTimeout = (roleCredentials.expiration - Date.now()) - 100;
            return setTimeout(() => roleCredentials.get(
                err => _assertCredentials(err, roleCredentials, err => {
                    assert.ifError(err);
                    // expiration should have changed, meaning
                    // credentials have been refreshed
                    assert.notStrictEqual(currentExpiration, roleCredentials.expiration);
                    done();
                })), retryTimeout);
        });
    });

    it('should handle non arsenal errors on refresh', function test(done) {
        this.timeout(10000);
        const retryTimeout = (roleCredentials.expiration - Date.now()) +
            1000;
        return setTimeout(() => {
            simulateServerError = true;
            roleCredentials.get(err => {
                assert(err);
                done();
            });
        }, retryTimeout);
    });

    it('RoleCredentials should use a default renewal anticipation delay if not explicit', () => {
        const vaultclient = new Client(
            vaultHost, vaultPort, undefined,
            undefined, undefined, undefined, undefined, undefined, undefined,
            undefined, proxyPath);
        const rc = new RoleCredentials(
            vaultclient, role, extension,
            new Logger('test:RoleCredentials').newRequestLogger('requids'));
        assert(rc._refreshCredsAnticipationMs > 0);
    });
});
