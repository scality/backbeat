const joi = require('@hapi/joi');
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

const vaultParamsJoi = joi.object({
    id: joi.string().required(),
    accountId: joi.string().required(),
    stsclient: joi.object().required(),
    authConfig: joi.object().required(),
}).required().unknown();

const accountParamsJoi = joi.object({
    id: joi.string().required(),
    authConfig: joi.object().required(),
}).required().unknown();

class CredentialsManager extends EventEmitter {
    constructor(extension, logger) {
        super();
        this._extension = extension;
        this._logger = logger;

        this._accountCredsCache = {};
    }

    static validateParams(params, validator) {
        try {
            joi.attempt(params, validator);
            return null;
        } catch (err) {
            return err;
        }
    }

    _addVaultServiceCredentials(params) {
        const err = CredentialsManager.validateParams(params, vaultParamsJoi);
        if (err) {
            this._logger.error('missing required params for vault type credentials', {
                method: 'CredentialsManager::_addVaultServiceCredentials',
                error: err,
                extension: this._extension,
            });
            return null;
        }

        const { id, accountId, stsclient, authConfig } = params;
        this._accountCredsCache[id] = new VaultServiceCredentials(
            stsclient,
            this._extension,
            getRoleArn(accountId, authConfig.vault.roleName),
            this._logger.newRequestLogger());
        return this._accountCredsCache[id];
    }

    _addAccountCredentials(params) {
        const err = CredentialsManager.validateParams(params, accountParamsJoi);
        if (err) {
            this._logger.error('missing required params for account type credentials', {
                method: 'CredentialsManager::_addVaultServiceCredentials',
                error: err,
                extension: this._extension,
            });
            return null;
        }

        const { id, authConfig } = params;
        let credentials;

        try {
            credentials = new AccountCredentials(authConfig, this._logger);
        } catch (err) {
            this._logger.error(`unable get credentials for ${authConfig.account}`, {
                method: 'CredentialsManager::_addAccountCredentials',
                error: err,
                extension: this._extension,
            });
            return null;
        }

        this._accountCredsCache[id] = credentials;
        return this._accountCredsCache[id];
    }

    _addRoleCredentials(params) {
        const err = CredentialsManager.validateParams(params, accountParamsJoi);
        if (err) {
            this._logger.error('missing required params for role type credentials', {
                method: 'CredentialsManager::_addVaultServiceCredentials',
                error: err,
                extension: this._extension,
            });
            return null;
        }

        const { id, vaultclient, roleName } = params;
        this._accountCredsCache[id] = new RoleCredentials(
            vaultclient,
            this._extension,
            roleName,
            this._logger.newRequestLogger());
        return this._accountCredsCache[id];
    }

    getCredentials(params) {
        const { authConfig, id } = params;

        if (!id || !authConfig) {
            this._logger.error('missing required params', {
                method: 'CredentialsManager::getCredentials',
                extension: this._extension,
            });
            return null;
        }

        if (this._accountCredsCache[id]) {
            return this._accountCredsCache[id];
        }

        switch (authConfig.type) {
        case authTypeRole:
            return this._addRoleCredentials(params);
        case authTypeVault:
            return this._addVaultServiceCredentials(params);
        case authTypeAccount: // fallthrough
        case authTypeService:
            return this._addAccountCredentials(params);
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
