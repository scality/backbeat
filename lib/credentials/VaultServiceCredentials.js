const http = require('http');
const joi = require('@hapi/joi');
const { Credentials, STS } = require('aws-sdk');

const errors = require('arsenal').errors;
const { Logger } = require('werelogs');
const AccountCredentials = require('./AccountCredentials');

const configJoi = {
    stsclient: joi.object().required(),
    extension: joi.string().required(),
    roleArn: joi.string().required(),
    refreshCredsAnticipationSeconds: joi.number().greater(0).default(60),
};

/**
* Manages and refreshes credentials as needed through assuming a role.
*
* This class extends AWS' Credentials class and implements the refresh
* method to refresh credentials once they are expired.
*/
class VaultServiceCredentials extends Credentials {
    /**
     * constructor
     * @param {object} stsclient - stsclient instance
     * @param {string} extension - name of the extension
     * @param {string} roleArn - ARN of the role
     * @param {RequestLogger} log - request logger instance
     * @param {number} refreshCredsAnticipationSeconds - credentials
     * must be refreshed earlier than their effective expiration time
     * to avoid race conditions, this sets from how many seconds
     * before expiration time credentials are refreshed
     */
    constructor(stsclient, extension, roleArn, log,
                refreshCredsAnticipationSeconds) {
        super();
        const params = joi.attempt({
            stsclient,
            extension,
            roleArn,
            refreshCredsAnticipationSeconds,
        }, configJoi);

        this._stsclient = stsclient;
        this._log = new Logger('Backbeat').newRequestLogger(log.getUids());
        this._extension = extension;
        this._roleArn = roleArn;
        this._refreshCredsAnticipationSeconds = params.refreshCredsAnticipationSeconds;
        this.accessKeyId = null;
        this.secretAccessKey = null;
        this.sessionToken = null;
        this.expiration = null;
    }

    /**
    * get credentials from cache or refresh credentials from vault
    * @param {callback} cb - callback to be called with err or credentials obj
    *   cb(null, { AccessKeyId, SecretAccessKey, SessionToken })
    * @return {undefined}
    */
    refresh(cb) {
        this._log.debug('refreshing credentials from vault', {
            method: 'VaultServiceCredentials.refresh',
            extension: this._extension,
            roleArn: this._roleArn,
        });
        const params = {
            RoleArn: this._roleArn,
            RoleSessionName: `backbeat-${this._extension}`,
            // default expiration: 1 hour,
        };

        return this._stsclient.assumeRole(params, (err, res) => {
            if (err) {
                this._log.error(`error assuming ${this._extension} role`, {
                    error: err,
                    method: 'VaultServiceCredentials::refresh',
                });

                const newErr = errors.InternalError
                    .customizeDescription(err.message);
                newErr.retryable = err.retryable;
                return cb(err);
            }

            const { AccessKeyId, SecretAccessKey, SessionToken,
                Expiration } = res.Credentials;
            this.accessKeyId = AccessKeyId;
            this.secretAccessKey = SecretAccessKey;
            this.sessionToken = SessionToken;
            this.expiration = Expiration;
            return cb();
        });
    }

    /**
     * check if credentials have expired
     * @return {boolean} - true if expired, false otherwise
     */
    needsRefresh() {
        return Date.now() >
            this.expiration - this._refreshCredsAnticipationSeconds * 1000 ||
            !this.accessKeyId ||
            !this.secretAccessKey ||
            !this.sessionToken;
    }
}

function newSTSClientFromVaultAuth(extConfig, log) {
    const { transport, vault } = extConfig.auth;
    const credentials = new AccountCredentials(vault.serviceCredentials, log);

    return new STS({
        endpoint: `${transport}://${vault.host}:${vault.port}`,
        credentials,
        region: 'us-east-1',
        signatureVersion: 'v4',
        sslEnabled: transport === 'https',
        httpOptions: {
            agent: new http.Agent({ keepAlive: true }),
            timeout: 0,
        },
        maxRetries: 0,
    });
}

function getRoleArn(accountId, roleName) {
    return `arn:aws:iam::${accountId}:role/${roleName}`;
}

module.exports = {
    VaultServiceCredentials,
    newSTSClientFromVaultAuth,
    getRoleArn,
};
