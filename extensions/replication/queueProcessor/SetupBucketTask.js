const async = require('async');
const AWS = require('aws-sdk');

const VaultClient = require('vaultclient').Client;

const { proxyPath } = require('../constants');

const trustPolicy = {
    Version: '2012-10-17',
    Statement: [
        {
            Effect: 'Allow',
            Principal: {
                Service: 'backbeat',
            },
            Action: 'sts:AssumeRole',
        },
    ],
};

function _buildResourcePolicy(source, target) {
    return {
        Version: '2012-10-17',
        Statement: [
            {
                Effect: 'Allow',
                Action: [
                    's3:GetObjectVersion',
                    's3:GetObjectVersionAcl',
                ],
                Resource: [
                    `arn:aws:s3:::${source}/*`,
                ],
            },
            {
                Effect: 'Allow',
                Action: [
                    's3:ListBucket',
                    's3:GetReplicationConfiguration',
                ],
                Resource: [
                    `arn:aws:s3:::${source}`,
                ],
            },
            {
                Effect: 'Allow',
                Action: [
                    's3:ReplicateObject',
                    's3:ReplicateDelete',
                ],
                Resource: `arn:aws:s3:::${target}/*`,
            },
        ],
    };
}

class SetupBucketTask {
    /**
     * Process a single replication entry
     *
     * @constructor
     * @param {QueueProcessor} qp - queue processor instance
     */
    constructor(qp) {
        Object.assign(this, qp.getStateVars());

        this.sourceRole = null;
        this.targetRole = null;
        this.destBackbeatHost = null;
        this.s3sourceAuthManager = null;
        this.s3destAuthManager = null;
        this.S3source = null;
        this.backbeatSource = null;
        this.backbeatDest = null;
    }

    _getDestAdminVaultClient() {
        const { host } = this.destHosts.pickHost();
        // FIXME this should go through the nginx proxy eventually
        // XXX we're using the source vault port in place of the
        // destination port to avoid having to extend the config,
        // until we can use nginx for vault admin
        const { adminPort } = this.sourceConfig.auth.vault;
        const key = `${host}:${adminPort}`;
        if (this.destVaults[key] === undefined) {
            this.destVaults[key] = new VaultClient(
                host, adminPort,
                undefined, undefined, undefined, undefined,
                undefined,
                this.adminCreds.accessKey,
                this.adminCreds.secretKey);
        }
        return this.destVaults[key];
    }

    _getDestS3VaultClient() {
        const { host, port } = this.destHosts.pickHost();
        const key = `${host}:${port}`;
        if (this.destVaults[key] === undefined) {
            this.destVaults[key] = new VaultClient(
                host, port,
                undefined, undefined, undefined, undefined,
                undefined, undefined, undefined,
                undefined, proxyPath);
        }
        return this.destVaults[key];
    }

    _getSourceAccountCreds(sourceEntry, log, done) {
        const displayName = sourceEntry.getOwnerDisplayName();
        const canonicalId = sourceEntry.getOwnerCanonicalId();
        let email;
        let accountCreds;

        async.waterfall([
            done => this.sourceS3Vault.getEmailAddresses(
                [canonicalId], { reqUid: log.getSerializedUids() }, done),
            (res, done) => {
                email = res.message.body[canonicalId];
                accountCreds = this.accountCredsCache[canonicalId];
                if (accountCreds) {
                    return done(null, null);
                }
                return this.sourceAdminVault.generateAccountAccessKey(
                    displayName, done);
            }, (res, done) => {
                if (res) {
                    accountCreds = {
                        accessKeyId: res.id,
                        secretAccessKey: res.value,
                    };
                    this.accountCredsCache[canonicalId] = accountCreds;
                }
                return done();
            },
        ], err => {
            if (err) {
                return done(err);
            }
            return done(null, email, accountCreds);
        });
    }

    _getTargetAccountCreds(sourceEntry, email, log, done) {
        const displayName = sourceEntry.getOwnerDisplayName();
        let canonicalId;
        let accountCreds;
        const destAdminVault = this._getDestAdminVaultClient();
        const destS3Vault = this._getDestS3VaultClient();

        async.waterfall([
            done => destS3Vault.getCanonicalIds(
                [email], { reqUid: log.getSerializedUids() }, done),
            (res, done) => {
                if (res.message.body[email] === 'NotFound') {
                    return destAdminVault.createAccount(displayName, { email },
                                                        done);
                }
                return done(null,
                            { account: {
                                canonicalId: res.message.body[email],
                            } });
            }, (res, done) => {
                canonicalId = res.account.canonicalId;
                accountCreds = this.accountCredsCache[canonicalId];
                if (accountCreds) {
                    return done(null, null);
                }
                return destAdminVault.generateAccountAccessKey(displayName,
                                                               done);
            }, (res, done) => {
                if (res) {
                    accountCreds = {
                        accessKeyId: res.id,
                        secretAccessKey: res.value,
                    };
                    this.accountCredsCache[canonicalId] = accountCreds;
                }
                return done();
            },
        ], err => {
            if (err) {
                return done(err);
            }
            return done(null, accountCreds);
        });
    }

    _setupClients(srcAccessCreds, tgtAccessCreds) {
        // Disable retries, use our own retry policy (mandatory for
        // putData route in order to fetch data again from source).

        const sourceS3 = this.sourceConfig.s3;
        const destS3 = this.destHosts.pickHost();
        // XXX port used for both source and destination IAM servers
        // to avoid changing the config format (will be obsolete when
        // nginx is used for destination IAM)
        const iamPort = this.sourceConfig.auth.vault.adminPort;
        this._s3Clients = {
            source: new AWS.S3({
                endpoint: `${this.sourceConfig.transport}://` +
                    `${sourceS3.host}:${sourceS3.port}`,
                credentials: srcAccessCreds,
                sslEnabled: this.sourceConfig.transport === 'https',
                s3ForcePathStyle: true,
                signatureVersion: 'v4',
                httpOptions: { agent: this.sourceHTTPAgent, timeout: 0 },
                maxRetries: 0,
            }),
            target: new AWS.S3({
                endpoint: `${this.destConfig.transport}://` +
                    `${destS3.host}:${destS3.port}`,
                credentials: tgtAccessCreds,
                sslEnabled: this.destConfig.transport === 'https',
                s3ForcePathStyle: true,
                signatureVersion: 'v4',
                httpOptions: { agent: this.destHTTPAgent, timeout: 0 },
                maxRetries: 0,
            }),
        };
        // FIXME this should go through the nginx proxy eventually
        this._iamClients = {
            source: new AWS.IAM({
                endpoint: `${this.sourceConfig.transport}://` +
                    `${sourceS3.host}:${iamPort}`,
                credentials: srcAccessCreds,
                sslEnabled: this.sourceConfig.transport === 'https',
                region: 'us-east-2',
                signatureCache: false,
                httpOptions: { agent: this.sourceHTTPAgent, timeout: 0 },
                maxRetries: 0,
            }),
            target: new AWS.IAM({
                endpoint: `${this.destConfig.transport}://` +
                    `${destS3.host}:${iamPort}`,
                credentials: tgtAccessCreds,
                sslEnabled: this.destConfig.transport === 'https',
                region: 'us-east-2',
                signatureCache: false,
                httpOptions: { agent: this.destHTTPAgent, timeout: 0 },
                maxRetries: 0,
            }),
        };
    }

    _createBucket(where, log, cb) {
        const bucket = where === 'source' ? this._sourceBucket :
            this._targetBucket;
        this._s3Clients[where].createBucket({ Bucket: bucket }, (err, res) => {
            if (err && err.code !== 'BucketAlreadyOwnedByYou') {
                log.error('error creating a bucket', {
                    bucket: where === 'source' ? this._sourceBucket :
                        this._targetBucket,
                    errCode: err.code,
                    error: err.message,
                    method: '_SetupReplication._createBucket',
                });
                return cb(err);
            }
            if (err && err.code === 'BucketAlreadyOwnedByYou') {
                log.debug('Bucket already exists. Continuing setup.', {
                    bucket: where === 'source' ? this._sourceBucket :
                        this._targetBucket,
                    errCode: err.code,
                    error: err.message,
                    method: '_SetupReplication._createBucket',
                });
            } else {
                log.debug('Created bucket', {
                    bucket: where === 'source' ? this._sourceBucket :
                        this._targetBucket,
                    method: '_SetupReplication._createBucket',
                });
            }
            return cb(null, res);
        });
    }

    _createRole(where, log, cb) {
        const params = {
            AssumeRolePolicyDocument: JSON.stringify(trustPolicy),
            RoleName: `bb-replication-${Date.now()}`,
            Path: '/',
        };

        this._iamClients[where].createRole(params, (err, res) => {
            if (err) {
                log.error('error creating a role', {
                    bucket: where === 'source' ? this._sourceBucket :
                        this._targetBucket,
                    errCode: err.code,
                    error: err.message,
                    method: '_SetupReplication._createRole',
                });
                return cb(err);
            }
            log.debug('Created role', {
                bucket: where === 'source' ? this._sourceBucket :
                    this._targetBucket,
                method: '_createRole',
            });
            return cb(null, res);
        });
    }

    _createPolicy(where, log, cb) {
        const params = {
            PolicyDocument: JSON.stringify(
                _buildResourcePolicy(this._sourceBucket, this._targetBucket)),
            PolicyName: `bb-replication-${Date.now()}`,
        };
        this._iamClients[where].createPolicy(params, (err, res) => {
            if (err) {
                log.error('error creating policy', {
                    bucket: where === 'source' ? this._sourceBucket :
                        this._targetBucket,
                    errCode: err.code,
                    error: err.message,
                    method: '_SetupReplication._createPolicy',
                });
                return cb(err);
            }
            log.debug('Created policy', {
                bucket: where === 'source' ? this._sourceBucket :
                    this._targetBucket,
                method: '_createPolicy',
            });
            return cb(null, res);
        });
    }

    _enableVersioning(where, log, cb) {
        const bucket = where === 'source' ? this._sourceBucket :
            this._targetBucket;
        const params = {
            Bucket: bucket,
            VersioningConfiguration: {
                Status: 'Enabled',
            },
        };
        this._s3Clients[where].putBucketVersioning(params, (err, res) => {
            if (err) {
                log.error('error enabling versioning', {
                    bucket: where === 'source' ? this._sourceBucket :
                        this._targetBucket,
                    errCode: err.code,
                    error: err.message,
                    method: '_SetupReplication._enableVersioning',
                });
                return cb(err);
            }
            log.debug('Versioning enabled', {
                bucket: where === 'source' ? this._sourceBucket :
                    this._targetBucket,
                method: '_enableVersioning',
            });
            return cb(null, res);
        });
    }

    _attachResourcePolicy(policyArn, roleName, where, log, cb) {
        const params = {
            PolicyArn: policyArn,
            RoleName: roleName,
        };
        this._iamClients[where].attachRolePolicy(params, (err, res) => {
            if (err) {
                log.error('error attaching resource policy', {
                    bucket: where === 'source' ? this._sourceBucket :
                        this._targetBucket,
                    errCode: err.code,
                    error: err.message,
                    method: '_SetupReplication._attachResourcePolicy',
                });
                return cb(err);
            }
            log.debug('Attached resource policy', {
                bucket: where === 'source' ? this._sourceBucket :
                    this._targetBucket,
                method: '_attachResourcePolicy',
            });
            return cb(null, res);
        });
    }

    _enableReplication(roleArns, log, cb) {
        const params = {
            Bucket: this._sourceBucket,
            ReplicationConfiguration: {
                Role: roleArns,
                Rules: [{
                    Destination: {
                        Bucket: `arn:aws:s3:::${this._targetBucket}`,
                    },
                    Prefix: '',
                    Status: 'Enabled',
                }],
            },
        };
        this._s3Clients.source.putBucketReplication(params, (err, res) => {
            if (err) {
                log.error('error enabling replication', {
                    errCode: err.code,
                    error: err.message,
                    method: '_SetupReplication._enableReplication',
                });
                return cb(err);
            }
            log.debug('Bucket replication enabled', {
                method: '_enableReplication',
            });
            return cb(null, res);
        });
    }

    processQueueEntry(sourceEntry, done) {
        const log = this.logger.newRequestLogger();
        let email;
        let srcCreds;
        let tgtCreds;
        let sourceRole;
        let targetRole;
        let sourcePolicyArn;
        let targetPolicyArn;

        this._sourceBucket = sourceEntry.getObjectKey();
        this._targetBucket = sourceEntry.getObjectKey();

        async.waterfall([
            done => this._getSourceAccountCreds(sourceEntry, log, done),
            (_email, _srcCreds, done) => {
                email = _email;
                srcCreds = _srcCreds;
                this._getTargetAccountCreds(sourceEntry, email, log, done);
            },
            (_tgtCreds, done) => {
                tgtCreds = _tgtCreds;
                this._setupClients(srcCreds, tgtCreds);
                done();
            },
            done => async.series({
                targetBucket: done => this._createBucket(
                    'target', log, done),
                sourceRole: done => this._createRole('source', log, done),
                targetRole: done => this._createRole('target', log, done),
                sourcePolicy: done => this._createPolicy('source', log, done),
                targetPolicy: done => this._createPolicy('target', log, done),
            }, done),
            (data, next) => {
                sourceRole = data.sourceRole.Role;
                targetRole = data.targetRole.Role;
                sourcePolicyArn = data.sourcePolicy.Policy.Arn;
                targetPolicyArn = data.targetPolicy.Policy.Arn;
                const roleArns = `${sourceRole.Arn},${targetRole.Arn}`;
                async.series([
                    done => this._enableVersioning('source', log, done),
                    done => this._enableVersioning('target', log, done),
                    done => this._attachResourcePolicy(sourcePolicyArn,
                                                       sourceRole.RoleName,
                                                       'source', log, done),
                    done => this._attachResourcePolicy(targetPolicyArn,
                                                       targetRole.RoleName,
                                                       'target', log, done),
                    done => this._enableReplication(roleArns, log, done),
                ], next);
            },
        ], err => {
            if (err) {
                log.end().error(
                    'echo mode: error during replication configuration',
                    { bucket: sourceEntry.getObjectKey(),
                      userName: sourceEntry.getOwnerDisplayName(),
                      userEmail: email,
                      error: err,
                    });
                return done(err);
            }
            log.end().info(
                'echo mode: configured replication for bucket',
                { bucket: sourceEntry.getObjectKey(),
                  userName: sourceEntry.getOwnerDisplayName(),
                  userEmail: email,
                  sourceRoleArn: sourceRole.Arn,
                  targetRoleArn: targetRole.Arn,
                  sourcePolicyArn,
                  targetPolicyArn,
                });
            return done();
        });
    }
}

module.exports = SetupBucketTask;
