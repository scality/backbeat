const EventEmitter = require('events');

const {
    authTypeRole,
    authTypeVault,
    authTypeAccount,
    authTypeService,
} = require('../constants');
const { VaultServiceCredentials, getRoleArn } = require('./VaultServiceCredentials');
const AccountCredentials = require('./AccountCredentials');
const RoleCredentials = require('./RoleCredentials');

class CredentialsManager extends EventEmitter {
    constructor(extension, logger) {
        super();
        this._extension = extension;
        this._logger = logger;

        this._accountCredsCache = {};
    }

    _addVaultServiceCredentials(id, accountId, params) {
        const { stsclient, authConfig } = params;

        if (!stsclient) {
            this._logger.error('missing sts client for vault type credentials', {
                method: 'CredentialsManager::_addVaultServiceCredentials',
                type: authConfig.type,
                extension: this._extension,
            });
            return null;
        }

        this._accountCredsCache[id] = new VaultServiceCredentials(
            stsclient,
            this._extension,
            getRoleArn(accountId, authConfig.vault.roleName),
            this._logger.newRequestLogger());
        return this._accountCredsCache[id];
    }

    _addAccountCredentials(id, accountId, params) {
        const { authConfig } = params;
        let credentials;

        try {
            credentials = new AccountCredentials(authConfig, this._logger);
        } catch (err) {
            this._logger.error(
                `unable get credentials for ${authConfig.account}`,
                {
                    method: 'CredentialsManager::_addAccountCredentials',
                    error: err,
                    type: authConfig.type,
                    extension: this._extension,
                });
            return null;
        }

        this._accountCredsCache[id] = credentials;
        return this._accountCredsCache[id];
    }

    _addRoleCredentials(id, accountId, params) {
        const { vaultclient, roleName, authConfig } = params;

        if (!vaultclient || !roleName) {
            this._logger.error('missing requiremd params for role type credentials', {
                method: 'CredentialsManager::_addRoleCredentials',
                type: authConfig.type,
                extension: this._extension,
            });
            return null;
        }

        this._accountCredsCache[id] = new RoleCredentials(
            vaultclient,
            this._extension,
            roleName,
            this._logger.newRequestLogger());
        return this._accountCredsCache[id];
    }

    getCredentials(params) {
        const { authConfig, id, accountId } = params;

        if (this._accountCredsCache[id]) {
            return this._accountCredsCache[id];
        }

        switch (authConfig.type) {
        case authTypeRole:
            return this._addRoleCredentials(id, accountId, params);
        case authTypeVault:
            return this._addVaultServiceCredentials(id, accountId, params);
        case authTypeAccount: // fallthrough
        case authTypeService:
            return this._addAccountCredentials(id, accountId, params);
        default:
            this._logger.error(`invalid auth type ${authConfig.type}`, {
                method: 'CredentialsManager::getCredentials',
                type: authConfig.type,
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
