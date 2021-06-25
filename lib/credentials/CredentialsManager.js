const EventEmitter = require('events');

const {
    authTypeVault,
    authTypeAccount,
    authTypeService,
} = require('../constants');
const { VaultServiceCredentials, getRoleArn } = require('./VaultServiceCredentials');
const AccountCredentials = require('./AccountCredentials');

// TODO:role / service credentials
class CredentialsManager extends EventEmitter {
    constructor(extension, extConfig, logger) {
        super();
        this._extension = extension;
        this._extConfig = extConfig;
        this._logger = logger;

        this._stsclient = null;
        this._accountCredsCache = {};
    }

    setSTSClient(stsclient) {
        this._stsclient = stsclient;
    }

    _addVaultServiceCredentials(canonicalId, accountId) {
        if (this._stsclient === null) {
            this._logger.error('missing sts client for vault type credentials', {
                method: 'CredentialsManager::_addVaultServiceCredentials',
                type: this._extConfig.auth.type,
                extension: this._extension,
            });
            return null;
        }

        this._accountCredsCache[canonicalId] = new VaultServiceCredentials(
            this._stsclient,
            this._extension,
            getRoleArn(accountId, this._extConfig.auth.vault.roleName),
            this._logger.newRequestLogger());
        return this._accountCredsCache[canonicalId];
    }

    _addAccountCredentials(canonicalId) {
        let credentials;

        try {
            credentials = new AccountCredentials(this._extConfig.auth, this._logger);
        } catch (err) {
            this._logger.error(
                `unable get credentials for ${this._extConfig.auth.account}`,
                {
                    method: 'CredentialsManager::_addAccountCredentials',
                    error: err,
                    type: this._extConfig.auth.type,
                    extension: this._extension,
                });
            return null;
        }

        this._accountCredsCache[canonicalId] = credentials;
        return this._accountCredsCache[canonicalId];
    }

    getCredentials(canonicalId, accountId) {
        if (this._accountCredsCache[canonicalId]) {
            return this._accountCredsCache[canonicalId];
        }

        switch (this._extConfig.auth.type) {
        case authTypeVault:
            return this._addVaultServiceCredentials(canonicalId, accountId);
        case authTypeAccount: // fallthrough
        case authTypeService:
            return this._addAccountCredentials(canonicalId);
        default:
            this._logger.error(`invalid auth type ${this._extConfig.auth.type}`, {
                method: 'CredentialsManager::getCredentials',
                type: this._extConfig.auth.type,
                extension: this._extension,
            });
            return null;
        }
    }

    /*
     * removes inactive credentials
     */
    removeInactiveCredentials(maxInactiveDuration) {
        Object.keys(this._accountCredsCache)
            .forEach(canonicalId => {
                const expiration = this._accountCredsCache[canonicalId].expiration;

                if (!expiration) {
                    return;
                }

                if (Date.now() - expiration >= maxInactiveDuration) {
                    this._logger.debug('deleting stale credentials', {
                        canonicalId,
                        extension: this._extension,
                    });
                    delete this._accountCredsCache[canonicalId];
                    this.emit('deleteCredentials', canonicalId);
                }
            });
    }
}

module.exports = CredentialsManager;
