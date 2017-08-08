const { Credentials } = require('aws-sdk');
const joi = require('joi');
const vaultclient = require('vaultclient');
const { Logger } = require('werelogs');

const configJoi = {
    host: joi.string().required(),
    port: joi.number().required(),
    extension: joi.string().required(),
    roleArn: joi.string().required(),
};
/**
* Manages and refreshes credentials as needed. This class extends AWS'
* Credentials class and implements the refresh method to refresh credentials
* once they are expired.
*/
class CredentialsManager extends Credentials {
    /**
    * constructor
    * @param {string} host - hostname/ip of Vault server
    * @param {number} port - port of Vault server
    * @param {string} extension - name of the extension
    * @param {string} roleArn - ARN of the role
    * @return {object} this - current instance
    */
    constructor(host, port, extension, roleArn) {
        super();
        joi.attempt({ host, port, extension, roleArn }, configJoi);

        this._vaultclient = new vaultclient.Client(host, port);
        this._log = new Logger('Backbeat').newRequestLogger();
        this._extension = extension;
        this._roleArn = roleArn;
        this.accessKeyId = null;
        this.secretAccessKey = null;
        this.sessionToken = null;
        this.expiration = null;
        this.expired = true;
        return this;
    }

    /**
    * get credentials from cache or refresh credentials from vault
    * @param {callback} cb - callback to be called with err or credentials obj
    *   cb(null, { AccessKeyId, SecretAccessKey, SessionToken })
    * @return {undefined}
    */
    refresh(cb) {
        this._log.debug('refreshing credentials from vault', {
            method: 'CredentialsManager.refresh',
            extension: this._extension,
            roleArn: this._roleArn,
        });
        const roleSessionName = `backbeat-${this._extension}`;
        return this._vaultclient.assumeRoleBackbeat(this._roleArn,
            roleSessionName, {}, (err, res) => {
                if (err) {
                    this._log.error('error assuming backbeat role', {
                        error: err,
                        method: 'CredentialsManager.refresh',
                    });
                    // We need to generate a new error instance
                    // instead of passing a possibly global arsenal
                    // error returned by vault client, because AWS
                    // client is transforming it its own way.
                    return cb(err.customizeDescription(err.description));
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

module.exports = CredentialsManager;
