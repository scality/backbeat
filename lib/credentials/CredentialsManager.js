const fs = require('fs');

const EventEmitter = require('events');
const joi = require('joi');
const AWS = require('aws-sdk');

const { errorUtils } = require('arsenal');

const {
    authTypeAssumeRole,
    authTypeAccount,
    authTypeService,
} = require('../constants');

const { getAccountCredentials } = require('./AccountCredentials');

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

        if (!authConfig) {
            this._logger.error('missing authConfig params', {
                method: 'CredentialsManager::getCredentials',
                extension: this._extension,
                id,
                authConfig,
            });
            return null;
        }

        if (authConfig.type === authTypeAccount ||
            authConfig.type === authTypeService) {
            return getAccountCredentials(authConfig, this._logger);
        }

        if (!id) {
            this._logger.error('missing id params for assume role', {
                method: 'CredentialsManager::getCredentials',
                extension: this._extension,
                id,
                authConfig,
            });
        }

        if (this._accountCredsCache[id]) {
            return this._accountCredsCache[id];
        }

        if (authConfig.type === authTypeAssumeRole) {
            const paramsWithKeys = this.resolveExternalFileSync(params);
            return this._addAssumeRoleCredentials(paramsWithKeys);
        }

        this._logger.error(`auth type "${authConfig.type}" not supported`, {
            method: 'CredentialsManager::getCredentials',
            type: authConfig.type,
            extension: this._extension,
        });
        return null;
    }

    resolveExternalFileSync(params) {
        let paramsWithKeys = params;

        const { externalFile, ...rest } = params;
        if (externalFile) {
            try {
                // The sync call normally accesses files of a few bytes in tmpfs so should not block
                const contents = fs.readFileSync(externalFile);
                const { accessKey, secretKey } = JSON.parse(contents); // TODO use safe parse
                if (!accessKey || !secretKey) {
                    this._logger.error('external creds file missing accessKey or secretKey', {
                        method: 'CredentialsManager::resolveExternalFileSync',
                        extension: this._extension,
                        externalFile,
                    });

                    return params;
                }

                paramsWithKeys = {
                    accessKey,
                    secretKey,
                    ...rest,
                };
            } catch (err) {
                this._logger.error('could not read external file', {
                    method: 'CredentialsManager::resolveExternalFileSync',
                    extension: this._extension,
                    externalFile,
                    error: errorUtils.reshapeExceptionError(err),
                });
            }
        }

        return paramsWithKeys;
    }

    /*
     * removes inactive credentials
     */
    removeInactiveCredentials(maxInactiveDuration) {
        Object.keys(this._accountCredsCache)
            .forEach(accountId => {
                const expiration =
                    this._accountCredsCache[accountId].expireTime;

                if (!expiration) {
                    return;
                }

                if (Date.now() - expiration >= maxInactiveDuration) {
                    this._logger.debug('deleting stale credentials', {
                        accountId,
                        extension: this._extension,
                    });
                    delete this._accountCredsCache[accountId];
                    this.emit('deleteCredentials', accountId);
                }
            });
    }
}

module.exports = CredentialsManager;
