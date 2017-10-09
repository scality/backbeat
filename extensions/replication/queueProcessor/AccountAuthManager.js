const assert = require('assert');
const AWS = require('aws-sdk');

const errors = require('arsenal').errors;
const AuthLoader = require('arsenal').auth.inMemory.AuthLoader;

const authLoader = new AuthLoader(null, false);

if (process.env.BACKBEAT_AUTH_CONFIG) {
    authLoader.addFilesByGlob(process.env.BACKBEAT_AUTH_CONFIG);
}
const authdata = authLoader.getData();

class AccountAuthManager {
    constructor(authConfig, log) {
        assert.strictEqual(authConfig.type, 'account');

        this._log = log;
        const accountInfo = authdata.accounts.find(
            account => account.name === authConfig.account);
        if (accountInfo === undefined) {
            throw Error(`No such account registered: ${authConfig.account}`);
        }
        if (accountInfo.keys.length === 0) {
            throw Error(`Account ${authConfig.account} has no configured ` +
                        'access/secret key pair');
        }
        this._credentials = new AWS.Credentials(accountInfo.keys[0].access,
                                                accountInfo.keys[0].secret);
    }

    getCredentials() {
        return this._credentials;
    }

    lookupAccountAttributes(accountId, cb) {
        return process.nextTick(() => cb(errors.NotImplemented));
    }
}

module.exports = AccountAuthManager;
