const fs = require('fs');

const EventEmitter = require('events');
const joi = require('joi');
const { STSClient, AssumeRoleCommand } = require('@aws-sdk/client-sts');

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

/**
 * TemporaryCredentials - Wrapper class for assume role credentials
 */
class TemporaryCredentials {
    constructor(stsClient, roleArn, roleSessionName, logger) {
        this._stsClient = stsClient;
        this._roleArn = roleArn;
        this._roleSessionName = roleSessionName;
        this._logger = logger;
        
        // Credential properties
        this.accessKeyId = null;
        this.secretAccessKey = null;
        this.sessionToken = null;
        this.expireTime = null;
        this.expired = true;
    }

    /**
     * Get credentials - backward compatible with v2 API
     * @param {Function} cb - callback(err)
     */
    get(cb) {
        // Check if credentials are still valid
        if (this.accessKeyId && this.expireTime && Date.now() < this.expireTime - 60000) {
            return process.nextTick(cb);
        }
        
        return this._refresh(cb);
    }

    /**
     * Refresh credentials from STS
     * @param {Function} cb - callback(err)
     */
    _refresh(cb) {
        const command = new AssumeRoleCommand({
            RoleArn: this._roleArn,
            RoleSessionName: this._roleSessionName,
        });

        this._stsClient.send(command)
            .then(response => {
                this.accessKeyId = response.Credentials.AccessKeyId;
                this.secretAccessKey = response.Credentials.SecretAccessKey;
                this.sessionToken = response.Credentials.SessionToken;
                this.expireTime = response.Credentials.Expiration.getTime();
                this.expired = false;
                cb();
            })
            .catch(error => {
                this._logger.error('error assuming role', {
                    method: 'TemporaryCredentials::_refresh',
                    error: errorUtils.reshapeExceptionError(error),
                    roleArn: this._roleArn,
                });
                cb(error);
            });
    }

    /**
     * Get credentials provider function for AWS SDK v3
     * @return {Function} Async function that returns credentials
     */
    getCredentialsProvider() {
        return async () => new Promise((resolve, reject) => {
            this.get(err => {
                if (err) {
                    return reject(err);
                }
                return resolve({
                    accessKeyId: this.accessKeyId,
                    secretAccessKey: this.secretAccessKey,
                    sessionToken: this.sessionToken,
                    expiration: this.expireTime ? new Date(this.expireTime) : undefined,
                });
            });
        });
    }
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
        
        const roleArn = getRoleArn(accountId, authConfig.roleName);
        const roleSessionName = `backbeat-${this._extension}`;
        
        // Create STS client for assuming roles
        const stsClient = new STSClient({
            endpoint: stsConfig.endpoint,
            region: stsConfig.region || 'us-east-1',
            credentials: stsConfig.credentials,
            tls: stsConfig.sslEnabled !== false,
            maxAttempts: (stsConfig.maxRetries ?? 0) + 1,
            requestHandler: stsConfig.httpOptions ? {
                httpAgent: stsConfig.httpOptions.agent,
                httpsAgent: stsConfig.httpOptions.agent,
                connectionTimeout: stsConfig.httpOptions.timeout,
                socketTimeout: stsConfig.httpOptions.timeout,
            } : undefined,
        });
        
        // Create temporary credentials wrapper
        const credentials = new TemporaryCredentials(
            stsClient,
            roleArn,
            roleSessionName,
            this._logger
        );
        
        this._accountCredsCache[id] = credentials;
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
            const paramsWithKeys = CredentialsManager.resolveExternalFileSync(params, this._logger);
            return this._addAssumeRoleCredentials(paramsWithKeys);
        }

        this._logger.error(`auth type "${authConfig.type}" not supported`, {
            method: 'CredentialsManager::getCredentials',
            type: authConfig.type,
            extension: this._extension,
        });
        return null;
    }

    static resolveExternalFileSync(params, logger) {
        let paramsWithKeys = params;

        const { externalFile, ...rest } = params;
        if (externalFile) {
            try {
                // The sync call normally accesses files of a few bytes in tmpfs so should not block
                const contents = fs.readFileSync(externalFile);
                const { accessKey, secretKey } = JSON.parse(contents); // TODO use safe parse
                if (!accessKey || !secretKey) {
                    if (logger) {
                        logger.error('external creds file missing accessKey or secretKey', {
                            method: 'CredentialsManager::resolveExternalFileSync',
                            externalFile,
                        });
                    }

                    return params;
                }

                paramsWithKeys = {
                    accessKey,
                    secretKey,
                    ...rest,
                };
            } catch (err) {
                if (logger) {
                    logger.error('could not read external file', {
                        method: 'CredentialsManager::resolveExternalFileSync',
                        externalFile,
                        error: errorUtils.reshapeExceptionError(err),
                    });
                }
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
