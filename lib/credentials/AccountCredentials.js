const assert = require('assert');
const AWS = require('aws-sdk');

const errors = require('arsenal').errors;
const management = require('../../lib/management');

class AccountCredentials extends AWS.Credentials {
    constructor(authConfig, getAuthDataCb, log) {
        const accountInfo = getAuthDataCb().accounts.find(
            account => account.name === authConfig.account);
        if (accountInfo === undefined) {
            throw Error(`No such account registered: ${authConfig.account}`);
        }
        if (accountInfo.arn === undefined) {
            throw Error(`Configured account ${authConfig.account} has no ` +
                        '"arn" property defined');
        }
        if (accountInfo.canonicalID === undefined) {
            throw Error(`Configured account ${authConfig.account} has no ` +
                        '"canonicalID" property defined');
        }
        if (accountInfo.displayName === undefined) {
            throw Error(`Configured account ${authConfig.account} has no ` +
                        '"displayName" property defined');
        }

        super(accountInfo.keys.access, accountInfo.keys.secret);

        this._log = log;
        this._accountArn = accountInfo.arn;
        this._canonicalID = accountInfo.canonicalID;
        this._displayName = accountInfo.displayName;
    }

    lookupAccountAttributes(accountId, cb) {
        const localAccountId = this._accountArn.split(':')[4];
        if (localAccountId !== accountId) {
            this._log.error('Target account for replication must match ' +
                            'configured destination account ARN',
                { targetAccountId: accountId,
                  localAccountId });
            return process.nextTick(() => cb(errors.AccountNotFound));
        }
        // return local account's attributes
        return process.nextTick(
            () => cb(null, { canonicalID: this._canonicalID,
                             displayName: this._displayName }));
    }
}

class StaticFileAccountCredentials extends AccountCredentials {
    constructor(authConfig, log) {
        assert.strictEqual(authConfig.type, 'account');
        super(authConfig, () => require('../../conf/authdata.json'), log);
    }
}

class ProvisionedServiceAccountCredentials extends AccountCredentials {
    constructor(authConfig, log) {
        assert.strictEqual(authConfig.type, 'service');
        super(authConfig, management.getLatestServiceAccountCredentials, log);
    }
}

module.exports = {
    StaticFileAccountCredentials,
    ProvisionedServiceAccountCredentials,
};
