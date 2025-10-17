const { fromTemporaryCredentials } = require('@aws-sdk/credential-providers');
const { errorUtils } = require('arsenal');

const { authTypeAssumeRole, authTypeNone } = require('../../lib/constants');
const VaultClientCache = require('../../lib/clients/VaultClientCache');
const CredentialsManager = require('../../lib/credentials/CredentialsManager');
const { http: HttpAgent, https: HttpsAgent } = require('httpagent');

class VaultClientWrapper {
    constructor(id, vaultConf, authConfig, logger) {
        this._authConfig = authConfig;
        this._transport = this._authConfig.transport;
        this._clientId = id;
        this._vaultConf = vaultConf || this._authConfig.vault;
        this.logger = logger;

        const Agent = this._transport === 'https' ? HttpsAgent.Agent : HttpAgent.Agent;
        this.stsAgent = new Agent({ keepAlive: true });

        this._tempCredsPromiseResolved = false;
    }

    init() {
        if (![authTypeAssumeRole, authTypeNone].includes(this._authConfig.type)) {
            return;
        }

        this._storeAWSCredentialsPromise();
        this._vaultClientCache = new VaultClientCache();
        this._vaultClientCache
            .setHost(this._clientId, this._vaultConf.host)
            .setPort(this._clientId, this._vaultConf.port);
    }


    // directly manages temp creds lifecycle, not going through CredentialsManager,
    // as vaultclient does not use credential provider functions, and the same set
    // can be reused forever as the role is assumed in only one account
    _storeAWSCredentialsPromise() {
        const { sts, roleName, type } = this._authConfig;

        if (type !== authTypeAssumeRole) {
            return;
        }

        const stsWithCreds = CredentialsManager.resolveExternalFileSync(sts, this.logger);

        // FIXME: works with vault 7.10 but not 8.3 (return 501)
        // https://scality.atlassian.net/browse/VAULT-238
        this._tempCredsPromise = Promise.resolve({ Account: '000000000000' })
            .then(res => {
                const roleArn = `arn:aws:iam::${res.Account}:role/${roleName}`;
                const roleSessionName = `${this._clientId}`;

                const masterCredentials = {
                    accessKeyId: stsWithCreds.accessKey,
                    secretAccessKey: stsWithCreds.secretKey,
                };

                // Create a credential provider that assumes the role
                return fromTemporaryCredentials({
                    masterCredentials,
                    params: {
                        RoleArn: roleArn,
                        RoleSessionName: roleSessionName,
                        // default expiration: 1 hour
                    },
                    clientConfig: {
                        endpoint: `${this._transport}://${sts.host}:${sts.port}`,
                        region: 'us-east-1',
                        tls: this._transport === 'https',
                        maxAttempts: 1,
                        requestHandler: {
                            httpAgent: this._transport === 'http' ? this.stsAgent : undefined,
                            httpsAgent: this._transport === 'https' ? this.stsAgent : undefined,
                            connectionTimeout: 0,
                            socketTimeout: 0,
                        },
                    },
                });
            })
            .then(creds => {
                this._tempCredsPromiseResolved = true;
                return creds;
            })
            .catch(err => {
                if (err.retryable) {
                    const retryDelayMs = 5000;

                    this.logger.error('could not set up temporary credentials, retrying', {
                        retryDelayMs,
                        error: errorUtils.reshapeExceptionError(err),
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
        if (![authTypeAssumeRole, authTypeNone].includes(this._authConfig.type)) {
            return process.nextTick(cb, null, {});
        }

        if (this._authConfig.type === authTypeAssumeRole) {
            return this.getAccountIdsWithTempCredentials(canonicalIds, cb);
        }

        const client = this._vaultClientCache.getClient(this._clientId);
        const opts = {};
        return client.getAccountIds(canonicalIds, opts, (err, res) => cb(err, res?.message?.body));
    }

    getAccountIdsWithTempCredentials(canonicalIds, cb) {
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
