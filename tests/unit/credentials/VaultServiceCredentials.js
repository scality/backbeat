const sinon = require('sinon');
const assert = require('assert');

const { Logger } = require('werelogs');

const { VaultServiceCredentials } =
    require('../../../lib/credentials/VaultServiceCredentials');

const extension = 'test-extension';
const roleArn = 'arn:aws:iam::000000000000:role/test-role';

const credentialsSet1 = {
    AccessKeyId: 'ABCD1234567890XXXX',
    SecretAccessKey: 'qscwdvefb1234567890',
    SessionToken: '83bdcf990f5fe307db8f12ec1994f35f4022949f36b94655cc4474',
};
const credentialsSet2 = {
    AccessKeyId: 'XXXXABCD1234567890',
    SecretAccessKey: '1234567890qscwdvefb',
    SessionToken: '1994f35f4022949f36b94655cc447483bdcf990f5fe307db8f12ec',
};

function assertCredentials(vaultCredentials, expectedCredentials) {
    if (!expectedCredentials) {
        assert.strictEqual(vaultCredentials.accessKeyId, null);
        assert.strictEqual(vaultCredentials.secretAccessKey, null);
        assert.strictEqual(vaultCredentials.sessionToken, null);
    } else {
        const { AccessKeyId, SecretAccessKey, SessionToken } = expectedCredentials;
        assert.strictEqual(vaultCredentials.accessKeyId, AccessKeyId);
        assert.strictEqual(vaultCredentials.secretAccessKey, SecretAccessKey);
        assert.strictEqual(vaultCredentials.sessionToken, SessionToken);
    }
}

describe('VaultServiceCredentials', () => {
    const stsclient = { assumeRole: sinon.stub() };
    const log = new Logger('test:VaultServiceCredentials');
    let vaultCredentials = null;

    beforeEach(() => {
        vaultCredentials = new VaultServiceCredentials(
            stsclient, extension, roleArn, log.newRequestLogger(), 1);
    });

    afterEach(() => {
        stsclient.assumeRole.reset();
    });

    it('should be able to acqurie credentials on startup', done => {
        stsclient.assumeRole.yields(null, {
            Credentials: Object.assign(
                { Expiration: Date.now() + 2000 }, credentialsSet1),
        });

        assertCredentials(vaultCredentials, null);
        vaultCredentials.get(err => {
            assert.ifError(err);
            assertCredentials(vaultCredentials, credentialsSet1);
            done();
        });
    });

    it('should not refresh valid credentials', done => {
        stsclient.assumeRole
            .onCall(0).yields(null, {
                Credentials: Object.assign(
                    { Expiration: Date.now() + 2000 }, credentialsSet1),
            })
            .onCall(1).yields(null, {
                Credentials: Object.assign(
                    { Expiration: Date.now() + 2000 }, credentialsSet2),
            });

        vaultCredentials.get(err => {
            assert.ifError(err);
            assertCredentials(vaultCredentials, credentialsSet1);
            vaultCredentials.get(err => {
                assert.ifError(err);
                assertCredentials(vaultCredentials, credentialsSet1);
                done();
            });
        });
    });

    it('should refresh expired credentials', done => {
        stsclient.assumeRole
            .onCall(0).yields(null, {
                Credentials: Object.assign(
                    { Expiration: Date.now() - 10000 }, credentialsSet1),
            })
            .onCall(1).yields(null, {
                Credentials: Object.assign(
                    { Expiration: Date.now() + 2000 }, credentialsSet2),
            });

        vaultCredentials.get(err => {
            assert.ifError(err);
            assertCredentials(vaultCredentials, credentialsSet1);
            vaultCredentials.get(err => {
                assert.ifError(err);
                assertCredentials(vaultCredentials, credentialsSet2);
                done();
            });
        });
    });

    it('should refresh credentials on anticipation trigger', done => {
        stsclient.assumeRole
            .onCall(0).yields(null, {
                Credentials: Object.assign(
                    { Expiration: Date.now() + 500 }, credentialsSet1),
                    // within 1 sec anticipation
            })
            .onCall(1).yields(null, {
                Credentials: Object.assign(
                    { Expiration: Date.now() + 2000 }, credentialsSet2),
            });

        vaultCredentials.get(err => {
            assert.ifError(err);
            assertCredentials(vaultCredentials, credentialsSet1);
            vaultCredentials.get(err => {
                assert.ifError(err);
                assertCredentials(vaultCredentials, credentialsSet2);
                done();
            });
        });
    });
});
