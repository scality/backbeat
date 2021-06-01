const AWS = require('aws-sdk');
const joi = require('@hapi/joi');

const errors = require('arsenal').errors;
const { Logger } = require('werelogs');

const configJoi = {
    vaultclient: joi.object().required(),
    extension: joi.string().required(),
    roleArn: joi.string().required(),
};

/**
* Manages and refreshes credentials as needed through assuming a role.
*
* This class extends AWS' Credentials class and implements the refresh
* method to refresh credentials once they are expired.
*/
class RoleCredentials extends AWS.Credentials {
    /**
     * constructor
     * @param {object} vaultclient - vaultclient instance
     * @param {string} extension - name of the extension
     * @param {string} roleArn - ARN of the role
     * @param {RequestLogger} log - request logger instance
     */
    constructor(vaultclient, extension, roleArn, log) {
        super();
        joi.attempt({ vaultclient, extension, roleArn }, configJoi);

        this._vaultclient = vaultclient;
        this._log = new Logger('Backbeat').newRequestLogger(log.getUids());
        this._extension = extension;
        this._roleArn = roleArn;
        this.accessKeyId = null;
        this.secretAccessKey = null;
        this.sessionToken = null;
        this.expiration = null;
        this.expired = true;
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

    /**
    * get credentials from cache or refresh credentials from vault
    * @param {callback} cb - callback to be called with err or credentials obj
    *   cb(null, { AccessKeyId, SecretAccessKey, SessionToken })
    * @return {undefined}
    */
    refresh(cb) {
        this._log.debug('refreshing credentials from vault', {
            method: 'RoleCredentials.refresh',
            extension: this._extension,
            roleArn: this._roleArn,
        });
        const roleSessionName = `backbeat-${this._extension}`;
        return this._vaultclient.assumeRoleBackbeat(this._roleArn,
            roleSessionName, { reqUid: this._log.getSerializedUids() },
            (err, res) => {
                if (err) {
                    this._log.error('error assuming backbeat role', {
                        error: err,
                        method: 'RoleCredentials.refresh',
                    });
                    // We need to generate a new error instance using
                    // customizeDescription() because AWS client is
                    // transforming it its own way.
                    // Any uncaught error or non arsenal error is treated as
                    // internal error.
                    const newErr = err.customizeDescription ?
                        err.customizeDescription(err.description)
                        : errors.InternalError.customizeDescription(err.message);

                    // Stick with the AWS SDK way of returning whether
                    // the error is retryable
                    newErr.retryable =
                        (err.InternalError ||
                            err.code === 'InternalError' ||
                            err.code === 500 ||
                            err.ServiceUnavailable ||
                            err.code === 'ServiceUnavailable' ||
                            err.code === 503);
                    return cb(newErr);
                }
                /*
                    {
                        data: {
                            Credentials: {
                                AccessKeyId: 'xxxxx',
                                SecretAccessKey: 'xxxxx',
                                SessionToken: 'xxxxx',
                                Expiration: 1499389378705
                            },
                            AssumedRoleUser: 'xxxx'
                        },
                        code: 200
                    }
                */
                const { AccessKeyId, SecretAccessKey, SessionToken,
                    Expiration } = res.data.Credentials;
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
        return Date.now() > this.expiration || !this.accessKeyId ||
            !this.secretAccessKey;
    }
}

module.exports = RoleCredentials;
