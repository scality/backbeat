const http = require('http');
const async = require('async');
const assert = require('assert');
const sinon = require('sinon');
const { Logger } = require('werelogs');
const AWS = require('aws-sdk');

const CredentialsManager = require('../../../lib/credentials/CredentialsManager');
const { authTypeAssumeRole } = require('../../../lib/constants');
const { stsResponseToXML } = require('./utils');

const extension = 'test-extension';
const id1 = '5a4847c9d83443885282cf38a09fa1a6193de9ed970168376a7b10c438683da0';
const accountId1 = '000000000000';
const id2 = 'de9ed970168376a7b10c438683da05a4847c9d83443885282cf38a09fa1a6193';
const accountId2 = '111111111111';
const stsPort = 8801;

const credentialsSet = {
    AccessKeyId: 'XXXXABCD1234567890',
    SecretAccessKey: '1234567890qscwdvefb',
    SessionToken: '1994f35f4022949f36b94655cc447483bdcf990f5fe307db8f12ec',
};

const invalidAuth = { type: 'invalid' };
const assumeRoleAuth = {
    type: authTypeAssumeRole,
    roleName: 'test-role',
    sts: { host: '127.0.0.1', port: 8800 },
    vault: { host: '127.0.0.1', port: 8500 },
};
const stsConfig = {
    credentials: { accessKeyId: 'accessKey', secretAccessKey: 'secretKey' },
    endpoint: `http://127.0.0.1:${stsPort}`,
    region: 'us-east-1',
};

function _genSTSResponse(expDelta) {
    return {
        Credentials: Object.assign(
            { Expiration: Date.now() + expDelta },
            credentialsSet
        ),
    };
}

describe('CredentialsManager', () => {
    const log = new Logger('test:CredentialsManager');

    describe('::getCredentials', () => {
        [
            // [msg, extensionConfig, expected]
            [
                'should return client for assumeRole type',
                {
                    id: id1, accountId: accountId2,
                    authConfig: assumeRoleAuth,
                    stsConfig,
                }, AWS.ChainableTemporaryCredentials,
            ],
        ].forEach(([msg, params, expected]) => it(msg, () => {
            const mgr = new CredentialsManager(extension, log);
            const creds2 = mgr.getCredentials({
                id: id2, accountId: accountId2,
                authConfig: assumeRoleAuth,
                stsConfig,
            });
            const creds = mgr.getCredentials(params);
            assert(creds instanceof expected);
            assert(mgr._accountCredsCache[id1] instanceof expected);
            assert.notEqual(creds, creds2);
            assert.notEqual(mgr._accountCredsCache[id1], mgr._accountCredsCache[id2]);
        }));

        [
            [
                'should return null when id is missing',
                {
                    id: id1, accountId: accountId1,
                    authConfig: assumeRoleAuth,
                }, null,
            ],
            [
                'should return null when authConfig is missing',
                {
                    id: id1, accountId: accountId1,
                    authConfig: null,
                }, null,
            ],
            [
                'should return null when stsConfig is missing for assumeRole type',
                {
                    id: id1, accountId: accountId1,
                    authConfig: assumeRoleAuth,
                    stsConfig: null,
                }, null,
            ],
            [
                'should return null when accountId is missing for assumeRole type',
                {
                    id: id1,
                    authConfig: assumeRoleAuth,
                    stsConfig,
                }, null,
            ],
            [
                'should return null for unsupported auth types',
                {
                    id: id1, accountId: accountId1,
                    authConfig: invalidAuth,
                }, null,
            ],
        ].forEach(([msg, params, expected]) => it(msg, () => {
            const mgr = new CredentialsManager(extension, log);
            const creds = mgr.getCredentials(params);
            assert.strictEqual(creds, expected);
        }));
    });

    describe('::removeInactiveCredentials', () => {
        const stsResponses = sinon.stub();
        let stsServer = null;

        before(done => {
            const server = http.createServer();
            server.on('request', (req, res) => {
                const payload = stsResponseToXML(stsResponses());
                res.writeHead(200, {
                    'content-type': 'application/json',
                    'content-length': Buffer.byteLength(payload),
                });
                res.end(payload);
            });
            stsServer = server.listen(stsPort).on('error', done);
            done();
        });

        beforeEach(() => {
            stsResponses
                .onCall(0).returns(_genSTSResponse(50000)) // not expired
                .onCall(1).returns(_genSTSResponse(-10000)) // expired
                .onCall(2).returns(_genSTSResponse(-1000)); // recent expired
        });

        afterEach(() => {
            stsResponses.reset();
        });

        after(() => {
            stsServer.close();
        });

        it('should remove inactive credentials older than max duration (long)', done => {
            const mgr = new CredentialsManager(extension, log);

            async.timesSeries(3, (n, next) => {
                const client = mgr.getCredentials({
                    id: `id${n}`,
                    accountId: `account${n}`,
                    stsConfig,
                    authConfig: assumeRoleAuth,
                });
                client.get(next);
            }, () => {
                assert(mgr._accountCredsCache.id0);
                assert(mgr._accountCredsCache.id1);
                assert(mgr._accountCredsCache.id2);
                mgr.removeInactiveCredentials(5000);
                assert(mgr._accountCredsCache.id0);
                assert(!mgr._accountCredsCache.id1);
                assert(mgr._accountCredsCache.id2);
                done();
            });
        });

        it('should remove inactive credentials older than max duration (short)', done => {
            const mgr = new CredentialsManager(extension, log);

            async.timesSeries(3, (n, next) => {
                const client = mgr.getCredentials({
                    id: `id${n}`,
                    accountId: `account${n}`,
                    stsConfig,
                    authConfig: assumeRoleAuth,
                });
                client.get(next);
            }, () => {
                assert(mgr._accountCredsCache.id0);
                assert(mgr._accountCredsCache.id1);
                assert(mgr._accountCredsCache.id2);
                mgr.removeInactiveCredentials(100);
                assert(mgr._accountCredsCache.id0);
                assert(!mgr._accountCredsCache.id1);
                assert(!mgr._accountCredsCache.id2);
                done();
            });
        });
    });

    describe('::resolveExternalFileSync', () => {
        const mgr = new CredentialsManager(extension, log);

        it('should return accessKey/secretKey if present', () => {
            const params = {
                accessKey: 'ak',
                secretKey: 'sk',
                otherParam: 'otherValue',
            };

            assert.deepStrictEqual(mgr.resolveExternalFileSync(params), params);
        });

        it('should read external file if referenced', () => {
            const params = {
                externalFile: `${__dirname}/external-creds.json`,
                otherParam: 'otherValue',
            };

            const resolvedParams = {
                accessKey: 'ak',
                secretKey: 'sk',
                otherParam: 'otherValue',
            };

            assert.deepStrictEqual(mgr.resolveExternalFileSync(params), resolvedParams);
        });

        it('should return params untouched if error parsing', () => {
            const params = {
                externalFile: 'yarn.lock',
                otherParam: 'otherValue',
            };

            assert.deepStrictEqual(mgr.resolveExternalFileSync(params), params);
        });

        it('should return params untouched if not found', () => {
            const params = {
                externalFile: '__nonexistent',
                otherParam: 'otherValue',
            };

            assert.deepStrictEqual(mgr.resolveExternalFileSync(params), params);
        });
    });
});
