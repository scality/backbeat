const { fromTemporaryCredentials } = require('@aws-sdk/credential-providers');
const { errorUtils } = require('arsenal');
const { GetCallerIdentityCommand } = require('@aws-sdk/client-sts');

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
        const endpoint = `${sts.transport || 'https'}://${sts.host}:${sts.port}`;

        const getCallerIdentity = new GetCallerIdentityCommand({});
        this._tempCredsPromise = stsWithCreds.send(getCallerIdentity)
            .then(res => {
                const roleArn = `arn:aws:iam::${res.Account}:role/${roleName}`;
                const roleSessionName = `${this._clientId}`;

                const masterCredentials = {
                    accessKeyId: stsWithCreds.accessKey,
                    secretAccessKey: stsWithCreds.secretKey,
                };

                const creds = fromTemporaryCredentials({
                    params: {
                        RoleArn: roleArn,
                        RoleSessionName: roleSessionName,
                    },
                    clientConfig: {
                        endpoint,
                        region: sts.region,
                        credentials: masterCredentials,
                        requestHandler: this.stsAgent,
                    },
                });
                return creds();
            })
            .then(res => {
                this._tempCreds = {
                    accessKey: res.accessKeyId,
                    secretKey: res.secretAccessKey,
                    sessionToken: res.sessionToken,
                };
            })
            .catch(err => {
                this.logger.error('failed to get temporary credentials', {
                    error: errorUtils.reshapeExceptionError(err),
                });
                throw err;
            });
    }

    getSTSCredentials() {
        if (this._authConfig.type !== authTypeAssumeRole) {
            return null;
        }

        return this._tempCreds;
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
