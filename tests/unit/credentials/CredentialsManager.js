const async = require('async');
const assert = require('assert');
const sinon = require('sinon');
const { Logger } = require('werelogs');

const { VaultServiceCredentials } =
    require('../../../lib/credentials/VaultServiceCredentials');
const AccountCredentials = require('../../../lib/credentials/AccountCredentials');
const CredentialsManager = require('../../../lib/credentials/CredentialsManager');
const { authTypeAccount, authTypeVault } = require('../../../lib/constants');

const extension = 'test-extension';
const canonicalId = '5a4847c9d83443885282cf38a09fa1a6193de9ed970168376a7b10c438683da0';
const accountId = '000000000000';

const credentialsSet = {
    AccessKeyId: 'XXXXABCD1234567890',
    SecretAccessKey: '1234567890qscwdvefb',
    SessionToken: '1994f35f4022949f36b94655cc447483bdcf990f5fe307db8f12ec',
};

const invalidAuth = { type: 'invalid' };
const accountAuth = { type: authTypeAccount, account: 'bart' };
const invalidAccountAuth = { type: authTypeAccount, account: 'missing' };
const vaultAuth = {
    type: authTypeVault,
    vault: { roleName: 'test-role', host: '127.0.0.1', port: 8600 },
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
    const stsclient = { assumeRole: sinon.stub() };
    const log = new Logger('test:CredentialsManager');

    describe('::getCredentials', () => {
        [
            // [msg, extensionConfig, expected]
            [
                'should return client for vault type',
                { authConfig: vaultAuth, stsclient }, VaultServiceCredentials,
            ],
            [
                'should return client for account type',
                { authConfig: accountAuth }, AccountCredentials,
            ],
        ].forEach(([msg, input, expected]) => it(msg, () => {
            const mgr = new CredentialsManager(extension, log);
            const params = Object.assign({ id: canonicalId, accountId }, input);
            const creds = mgr.getCredentials(params);
            assert(creds instanceof expected);
            // check cache
            assert(mgr._accountCredsCache[canonicalId] instanceof expected);
        }));

        [
            [
                'should return null for stsclient is missing for vault type',
                { authConfig: vaultAuth, stsclient: null }, null,
            ],
            [
                'should return null for missing account type credentials',
                { authConfig: invalidAccountAuth }, null,
            ],
            [
                'should return null for invalid auth type',
                { authConfig: invalidAuth }, null,
            ],
        ].forEach(([msg, input, expected]) => it(msg, () => {
            const mgr = new CredentialsManager(extension, log);
            const params = Object.assign({ id: canonicalId, accountId }, input);
            const creds = mgr.getCredentials(params);
            assert.strictEqual(creds, expected);
        }));
    });


    describe('::removeInactiveCredentials', () => {
        beforeEach(() => {
            stsclient.assumeRole
                .onCall(0).yields(null, _genSTSResponse(50000)) // not expired
                .onCall(1).yields(null, _genSTSResponse(-10000)) // expired
                .onCall(2).yields(null, _genSTSResponse(-1000)); // recent expired
        });

        afterEach(() => {
            stsclient.assumeRole.reset();
        });

        it('should remove inactive credentials older than max duration (long)', done => {
            const mgr = new CredentialsManager(extension, log);

            async.timesSeries(3, (n, next) => {
                const client = mgr.getCredentials({
                    id: `id${n}`,
                    accountId: `account${n}`,
                    stsclient,
                    authConfig: vaultAuth,
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
                    stsclient,
                    authConfig: vaultAuth,
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

        it('should not remove credentials without expiration', () => {
            const mgr = new CredentialsManager(extension, log);
            mgr.getCredentials({
                id: 'id1',
                accountId: 'account1',
                authConfig: accountAuth,
            });
            mgr.removeInactiveCredentials(0);
            assert(mgr._accountCredsCache.id1);
        });
    });
});
