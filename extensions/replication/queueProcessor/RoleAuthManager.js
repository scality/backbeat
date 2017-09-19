const errors = require('arsenal').errors;

const CredentialsManager = require('../../../credentials/CredentialsManager');

class RoleAuthManager {
    constructor(vaultclient, roleArn, log) {
        this._log = log;
        this._vaultclient = vaultclient;
        this._credentials = new CredentialsManager(
            vaultclient, 'replication', roleArn, log.getUids());
    }

    getCredentials() {
        return this._credentials;
    }

    lookupAccountAttributes(accountId, cb) {
        this._vaultclient.getCanonicalIdsByAccountIds(
            [accountId], { reqUid: this._log.getSerializedUids() },
            (err, res) => {
                if (err) {
                    return cb(err);
                }
                if (!res || !res.message || !res.message.body
                    || res.message.body.length === 0) {
                    return cb(errors.AccountNotFound);
                }
                return cb(null, {
                    canonicalID: res.message.body[0].canonicalId,
                    displayName: res.message.body[0].name,
                });
            });
    }
}

module.exports = RoleAuthManager;
