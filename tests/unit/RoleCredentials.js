const assert = require('assert');
const http = require('http');
const { Client } = require('vaultclient');
const { Logger } = require('werelogs');
const { errors, ArsenalError } = require('arsenal');
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
    const Expiration = Date.now() + 2000; // expire after 2 seconds
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
            new Logger('test:RoleCredentials').newRequestLogger('requids'),
            1);
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
            const retryTimeout = (roleCredentials.expiration - Date.now()) - 1500;
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

    it('should properly handle Arsenal and non-Arsenal errors', done => {
        const testCases = [
            {
                name: 'non-Arsenal error',
                error: Object.assign(new Error('custom error'), {
                    code: 'CustomError',
                    description: 'test description'
                }),
                statusCode: 400,
                expectedResult: error => {
                    assert(error instanceof ArsenalError);
                    assert.strictEqual(error.code, 400);
                    assert.strictEqual(error.description, 'test description');
                }
            },
            {
                name: 'retryable Internal error',
                error: Object.assign(new Error('internal error'), {
                    code: 'InternalError',
                    InternalError: true
                }),
                statusCode: 500,
                expectedResult: error => {
                    assert(error.retryable === true);
                }
            },
            {
                name: 'Arsenal error',
                error: errors.InternalError.customizeDescription('some error'),
                statusCode: 500,
                expectedResult: error => {
                    assert(error instanceof ArsenalError);
                    assert.strictEqual(error.code, 500);
                    assert.strictEqual(error.description, 'some error');
                }
            }
        ];

        let completedTests = 0;

        testCases.forEach(testCase => {
            const mockVaultClient = {
                assumeRoleBackbeat: (roleArn, roleSessionName, options, callback) => {
                    if (testCase.error instanceof ArsenalError) {
                        return callback(testCase.error, null, testCase.statusCode);
                    }
                    // Simulate error returned by the vault client
                    const errorWithCode = new Error(testCase.error.message);
                    Object.assign(errorWithCode, testCase.error);
                    callback(errorWithCode, null, testCase.statusCode);
                }
            };

            const credentials = new RoleCredentials(
                mockVaultClient,
                role,
                extension,
                new Logger('test:RoleCredentials').newRequestLogger('request-uid')
            );

            credentials.refresh(err => {
                try {
                    testCase.expectedResult(err);
                    completedTests += 1;

                    if (completedTests === testCases.length) {
                        done();
                    }
                } catch (assertError) {
                    done(assertError);
                }
            });
        });
    });


    it('RoleCredentials should use a default renewal anticipation delay if not explicit', () => {
        const vaultclient = new Client(
            vaultHost, vaultPort, undefined,
            undefined, undefined, undefined, undefined, undefined, undefined,
            undefined, proxyPath);
        const rc = new RoleCredentials(
            vaultclient, role, extension,
            new Logger('test:RoleCredentials').newRequestLogger('requids'));
        assert(rc._refreshCredsAnticipationSeconds > 0);
    });
});
