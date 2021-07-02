const joi = require('@hapi/joi');
const EventEmitter = require('events');
const AWS = require('aws-sdk');

const { authTypeAssumeRole } = require('../constants');

const assumeRoleParamJoi = joi.object({
    id: joi.string().required(),
    accountId: joi.string().required(),
    authConfig: joi.object().required(),
    stsConfig: joi.object().required(),
}).required().unknown();

function getRoleArn(accountId, roleName) {
    return `arn:aws:iam::${accountId}:role/${roleName}`;
}

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
        const err = CredentialsManager.validateParams(params, assumeRoleParamJoi);
        if (err) {
            this._logger.error('missing required params for assumeRole type credentials', {
                method: 'CredentialsManager::_addAssumeRoleCredentials',
                // error: err.messsage,
                extension: this._extension,
            });
            return null;
        }

        const { id, accountId, authConfig, stsConfig } = params;
        this._accountCredsCache[id] = new AWS.ChainableTemporaryCredentials({
            params: {
                RoleArn: getRoleArn(accountId, authConfig.roleName),
                RoleSessionName: `backbeat-${this._extension}`,
                // default expiration: 1 hour,
            },
            stsConfig,
        }, this._logger.newRequestLogger());
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
                const expiration =
                    this._accountCredsCache[canonicalId].expireTime;

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
