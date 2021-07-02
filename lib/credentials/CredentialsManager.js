const joi = require('@hapi/joi');
const EventEmitter = require('events');

const { authTypeAssumeRole } = require('../constants');
const { AssumeRoleCredentials, getRoleArn } = require('./AssumeRoleCredentials');

const vaultParamsJoi = joi.object({
    id: joi.string().required(),
    accountId: joi.string().required(),
    stsclient: joi.object().required(),
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

    _addAssumeRoleCredentials(params) {
        const err = CredentialsManager.validateParams(params, vaultParamsJoi);
        if (err) {
            this._logger.error('missing required params for vault type credentials', {
                method: 'CredentialsManager::_addAssumeRoleCredentials',
                error: err,
                extension: this._extension,
            });
            return null;
        }

        const { id, accountId, stsclient, authConfig } = params;
        this._accountCredsCache[id] = new AssumeRoleCredentials(
            stsclient,
            this._extension,
            getRoleArn(accountId, authConfig.role),
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

        if (authConfig.type !== authTypeAssumeRole) {
            this._logger.error(`auth type "${authConfig.type}" not supported`, {
                method: 'CredentialsManager::getCredentials',
                type: authConfig.type,
                extension: this._extension,
            });
            return null;
        }

        return this._addAssumeRoleCredentials(params);
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
