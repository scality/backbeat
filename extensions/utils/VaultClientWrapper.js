const http = require('http');
const https = require('https');
const { ChainableTemporaryCredentials } = require('aws-sdk');
const { errorUtils } = require('arsenal');

const { authTypeAssumeRole } = require('../../lib/constants');
const VaultClientCache = require('../../lib/clients/VaultClientCache');
const CredentialsManager = require('../../lib/credentials/CredentialsManager');

class VaultClientWrapper {
    constructor(id, vaultConf, authConfig, logger) {
        this._authConfig = authConfig;
        this._transport = this._authConfig.transport;
        this._clientId = id;
        this._vaultConf = vaultConf;
        this.logger = logger;

        const Agent = this._transport === 'https' ? https.Agent : http.Agent;
        this.stsAgent = new Agent({ keepAlive: true });

        this._tempCredsPromiseResolved = false;
    }

    init() {
        if (this._authConfig.type !== authTypeAssumeRole) {
            return;
        }

        this._storeAWSCredentialsPromise();
        this._vaultClientCache = new VaultClientCache();
        this._vaultClientCache
            .setHost(this._clientId, this._vaultConf.host)
            .setPort(this._clientId, this._vaultConf.port);
    }


    // directly manages temp creds lifecycle, not going through CredentialsManager,
    // as vaultclient does not use `AWS.Credentials` objects, and the same set
    // can be reused forever as the role is assumed in only one account
    _storeAWSCredentialsPromise() {
        const { sts, roleName, type } = this._authConfig;

        if (type !== authTypeAssumeRole) {
            return;
        }

        const stsWithCreds = CredentialsManager.resolveExternalFileSync(sts, this.logger);
        const stsConfig = {
            endpoint: `${this._transport}://${sts.host}:${sts.port}`,
            credentials: {
                accessKeyId: stsWithCreds.accessKey,
                secretAccessKey: stsWithCreds.secretKey,
            },
            region: 'us-east-1',
            signatureVersion: 'v4',
            sslEnabled: this._transport === 'https',
            httpOptions: { agent: this.stsAgent, timeout: 0 },
            maxRetries: 0,
        };

        // FIXME: works with vault 7.10 but not 8.3 (return 501)
        // https://scality.atlassian.net/browse/VAULT-238
        // new STS(stsConfig)
        //     .getCallerIdentity()
        //     .promise()
        this._tempCredsPromise =
            Promise.resolve({
                Account: '000000000000',
            })
            .then(res =>
                new ChainableTemporaryCredentials({
                    params: {
                        RoleArn: `arn:aws:iam::${res.Account}:role/${roleName}`,
                        // RoleSessionName: `backbeat-vaultclient-${roleName}`,
                        RoleSessionName: `${this._clientId}`,
                        // default expiration: 1 hour,
                    },
                    stsConfig,
                }))
            .then(creds => {
                this._tempCredsPromiseResolved = true;
                return creds;
            })
            .catch(err => {
                if (err.retryable) {
                    const retryDelayMs = 5000;

                    this.logger.error('could not set up temporary credentials, retrying', {
                        retryDelayMs,
                        error: err,
                    });

                    setTimeout(() => this._storeAWSCredentialsPromise(), retryDelayMs);
                } else {
                    this.logger.error('could not set up temporary credentials', {
                        error: errorUtils.reshapeExceptionError(err),
                    });
                }
            });
    }

    getAccountId(canonicalId, cb) {
        this.getAccountIds([canonicalId], (err, res) => {
            if (err) {
                return cb(err);
            }

            return cb(null, res[canonicalId]);
        });
    }

    getAccountIds(canonicalIds, cb) {
        console.log('canonicalIds!!!', canonicalIds);
        if (this._authConfig.type !== authTypeAssumeRole) {
            return process.nextTick(cb, null, {});
        }

        return this._tempCredsPromise
            .then(creds => this._vaultClientCache.getClientWithAWSCreds(this._clientId, creds))
            .then(client => client.enableIAMOnAdminRoutes())
            .then(client => {
                const opts = {};
                return client.getAccountIds(canonicalIds, opts, (err, res) => {
                    if (err) {
                        return cb(err);
                    }
                    return cb(null, res.message.body);
                });
            })
            .catch(err => cb(err));
    }

    tempCredentialsReady() {
        if (this._authConfig.type !== authTypeAssumeRole) {
            return true;
        }

        return this._tempCredsPromiseResolved;
    }
}

module.exports = VaultClientWrapper;
