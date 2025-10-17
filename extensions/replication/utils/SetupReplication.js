const async = require('async');
const {
    S3Client,
    CreateBucketCommand,
    HeadBucketCommand,
    GetBucketVersioningCommand,
    PutBucketVersioningCommand,
    GetBucketReplicationCommand,
    PutBucketReplicationCommand,
} = require('@aws-sdk/client-s3');
const {
    IAMClient,
    GetRoleCommand,
    CreateRoleCommand,
    CreatePolicyCommand,
    AttachRolePolicyCommand,
} = require('@aws-sdk/client-iam');
const BackbeatTask = require('../../../lib/tasks/BackbeatTask');
const { isRetryableMiddleware } = require('../../../lib/clients/utils');

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

function _setupS3Client(transport, endpoint, credentials, https) {
    const client = new S3Client({
        endpoint: `${transport}://${endpoint}`,
        credentials,
        region: 'us-east-1',
        forcePathStyle: true,
        tls: transport === 'https',
        maxAttempts: 1,
        requestHandler: {
            connectionTimeout: 0,
            socketTimeout: 0,
            httpsAgent: https && {
                key: https.key,
                cert: https.cert,
                ca: https.ca,
            },
        },
    });
    client.middlewareStack.add(isRetryableMiddleware(), {
        step: 'deserialize',
        priority: 'high',
    });

    return client;
}

function _setupIAMClient(transport, endpoint, credentials, https) {
    const client = new IAMClient({
        endpoint: `${transport}://${endpoint}`,
        credentials,
        region: 'us-east-2',
        tls: transport === 'https',
        maxAttempts: 1,
        requestHandler: {
            connectionTimeout: 30000,
            socketTimeout: 30000,
            httpsAgent: https && {
                key: https.key,
                cert: https.cert,
                ca: https.ca,
            },
        },
    });
    client.middlewareStack.add(isRetryableMiddleware(), {
        step: 'deserialize',
        priority: 'high',
    });

    return client;
}

class SetupReplication extends BackbeatTask {
    /**
     * This class sets up two buckets for replication.
     * @constructor
     * @param {Object} params - constructor params
     * @param {String} params.source.bucket - source bucket name
     * @param {Object} params.source.credentials - source aws-sdk
     *   Credentials object
     * @param {Object} params.source.s3.host - source S3 host name
     * @param {String|Number} params.source.s3.port - source S3 port
     * @param {Object} params.source.vault.host - source vault host name
     * @param {String|Number} params.source.vault.adminPort - source
     *   vault admin port
     * @param {String} [params.source.transport] - transport protocol for
     *   source (http/https)
     * @param {String} params.target.bucket - target bucket name
     * @param {Object} params.target.credentials - target aws-sdk
     *   Credentials object
     * @param {RoundRobin} params.target.hosts - destination hosts
     * @param {String} [params.target.transport] - transport protocol for
     *   target (http/https)
     * @param {Boolean} params.target.isExternal - whether target bucket
     *   is on an external location
     * @param {String} [params.target.siteName] - the site name where the target
     *   bucket exists, if the target is on an external location
     * @param {Object} [params.https] - destination SSL termination
     *   HTTPS configuration object
     * @param {String} [params.https.key] - client private key in PEM format
     * @param {String} [params.https.cert] - client certificate in PEM format
     * @param {String} [params.https.ca] - alternate CA bundle in PEM format
     * @param {Object} [params.internalHttps] - internal HTTPS
     *   configuration object
     * @param {String} [params.internalHttps.key] - client private key
     *   in PEM format
     * @param {String} [params.internalHttps.cert] - client
     *   certificate in PEM format
     * @param {String} [params.internalHttps.ca] - alternate CA bundle
     *   in PEM format
     * @param {Boolean} [params.checkSanity=false] - whether to check
     *   sanity of the config after setup, in case something would
     *   have gone wrong but unnoticed
     * @param {Boolean} [params.skipSourceBucketCreation=false] - can
     *   be set to true if the source bucket is guaranteed to exist to
     *   spare a request
     * @param {Object} params.log - werelogs request logger object
     */
    constructor(params) {
        const { source, target, https, internalHttps, checkSanity,
                repConfig, skipSourceBucketCreation, log } = params;
        super(repConfig.queueProcessor.retry.scality);
        this._log = log;
        this._sourceBucket = source.bucket;
        this._targetBucket = target.bucket;
        this._targetIsExternal = target.isExternal;
        this._targetSiteName = target.siteName;
        this._checkSanityEnabled = checkSanity || false;
        this._skipSourceBucketCreation = skipSourceBucketCreation || false;
        this.destHosts = target.hosts;
        const destHost = target.isExternal ?
            undefined : this.destHosts.pickHost();
        this._s3Clients = {
            source: _setupS3Client(
                source.transport, `${source.s3.host}:${source.s3.port}`,
                source.credentials, internalHttps),
            target: target.isExternal ? undefined : _setupS3Client(
                target.transport, `${destHost.host}:${destHost.port}`,
                target.credentials, https),
        };
        this._iamClients = {
            source: _setupIAMClient(
                source.transport,
                `${source.vault.host}:${source.vault.adminPort}`,
                source.credentials, internalHttps),
            target: target.isExternal ? undefined : _setupIAMClient(
                target.transport, `${destHost.host}:${destHost.port}`,
                target.credentials, https),
        };
    }

    checkSanity(cb) {
        return async.waterfall([
            next => this._isValidBucket('source', next),
            next => (this._targetIsExternal ? next() :
                this._isValidBucket('target', next)),
            next => this._isVersioningEnabled('source', next),
            next => (this._targetIsExternal ? next() :
                this._isVersioningEnabled('target', next)),
            next => this._isReplicationEnabled('source', next),
            (arns, next) => this._arnParser(arns, next),
            (arnObj, next) => this._isValidRole('source', arnObj, next),
            (arnObj, next) => (this._targetIsExternal ? next() :
                this._isValidRole('target', arnObj, next)),
        ], cb);
    }

    _arnParser(arns, cb) {
        if (this._targetIsExternal) {
            return cb(null, { source: arns });
        }
        const [src, des] = arns.split(',');

        return cb(null, {
            source: src,
            target: des,
        });
    }

    _isValidBucket(where, cb) {
        // Does the bucket exist and is it reachable?
        const bucket = where === 'source' ? this._sourceBucket :
            this._targetBucket;
        const command = new HeadBucketCommand({ Bucket: bucket });
        this._s3Clients[where].send(command)
            .then(() => cb())
            .catch(err => {
                this._log.error('bucket sanity check error', {
                    bucket: where === 'source' ? this._sourceBucket :
                        this._targetBucket,
                    errCode: err.name,
                    error: err.message,
                    method: 'SetupReplication._isValidBucket',
                });
                cb(err);
            });
    }

    _isVersioningEnabled(where, cb) {
        // Does the bucket have versioning enabled?
        const bucket = where === 'source' ? this._sourceBucket :
            this._targetBucket;
        const command = new GetBucketVersioningCommand({ Bucket: bucket });
        this._s3Clients[where].send(command)
            .then(res => {
                if (res.Status === 'Disabled') {
                    const error = new Error('Expected bucket versioning to ' +
                        'be Enabled. Status is still Disabled.');
                    this._log.error('versioning sanity check error: ' +
                        'Status Disabled', {
                            bucket: where === 'source' ? this._sourceBucket :
                                this._targetBucket,
                            error: error.message,
                            method: 'SetupReplication._isVersioningEnabled',
                        }
                    );
                    return cb(error);
                }
                return cb();
            })
            .catch(err => {
                this._log.error('versioning sanity check error: ' +
                    'Cannot retrieve versioning configuration', {
                        bucket: where === 'source' ? this._sourceBucket :
                            this._targetBucket,
                        errCode: err.name,
                        error: err.message,
                        method: 'SetupReplication._isVersioningEnabled',
                    }
                );
                cb(err);
            });
    }

    _isValidRole(where, arnObj, cb) {
        // Is the role mentioned in the replication config available in IAM

        // Goal is to get Role given known ARN.
        // If err, there is no matching role
        const arn = arnObj[where];
        const roleName = arn.split('/').pop();

        const command = new GetRoleCommand({ RoleName: roleName });
        this._iamClients[where].send(command)
            .then(res => {
                if (arn !== res.Role.Arn) {
                    const error = new Error('Expected ARN to match. A mis-match ' +
                        'was found between the ARN found in ' +
                        '`getBucketReplication` and ARN found in `getRole`.');
                    this._log.error('role validation sanity check error: ' +
                        'ARN mis-match', {
                            bucket: where === 'source' ? this._sourceBucket :
                                this._targetBucket,
                            error: error.message,
                            method: 'SetupReplication._isVersioningEnabled',
                        }
                    );
                    return cb(error);
                }
                return cb(null, arnObj);
            })
            .catch(err => {
                this._log.error('role validation sanity check error: ' +
                    'Cannot retrieve role configuration', {
                        bucket: where === 'source' ? this._sourceBucket :
                            this._targetBucket,
                        errCode: err.name,
                        error: err.message,
                        method: 'SetupReplication._isValidRole',
                    }
                );
                cb(err);
            });
    }

    _isReplicationEnabled(src, cb) {
        // Is the Replication config enabled?
        const command = new GetBucketReplicationCommand({
            Bucket: this._sourceBucket,
        });
        this._s3Clients[src].send(command)
            .then(res => {
                const r = res.ReplicationConfiguration;
                if (r.Rules[0].Status === 'Disabled') {
                    const error = new Error('Expected bucket replication ' +
                        'to be Enabled. Status is still Disabled.');
                    this._log.error('replication status sanity check error: ' +
                        'Status Disabled', {
                            error: error.message,
                            method: 'SetupReplication._isReplicationEnabled',
                        }
                    );
                    return cb(error);
                }
                return cb(null, r.Role);
            })
            .catch(err => {
                this._log.error('replication status sanity check error: ' +
                    'Cannot retrieve replication configuration', {
                        errCode: err.name,
                        error: err.message,
                        method: 'SetupReplication._isReplicationEnabled',
                    }
                );
                cb(err);
            });
    }

    _createBucket(where, cb) {
        if (where === 'source' && this._skipSourceBucketCreation) {
            return process.nextTick(() => cb());
        }
        return this.retry({
            actionDesc: `create ${where} bucket`,
            logFields: {},
            actionFunc: done => this._createBucketOnce(where, done),
            shouldRetryFunc: err => err.retryable,
            log: this._log,
        }, cb);
    }

    _createBucketOnce(where, cb) {
        const bucket = where === 'source' ?
                  this._sourceBucket : this._targetBucket;
        const command = new CreateBucketCommand({ Bucket: bucket });
        this._s3Clients[where].send(command)
            .then(res => {
                this._log.debug('Created bucket', {
                    where,
                    bucket: where === 'source' ? this._sourceBucket :
                        this._targetBucket,
                    method: 'SetupReplication._createBucket',
                });
                cb(null, res);
            })
            .catch(err => {
                if (err.name === 'BucketAlreadyOwnedByYou') {
                    this._log.debug('Bucket already exists. Continuing setup.', {
                        where,
                        bucket: where === 'source' ? this._sourceBucket :
                            this._targetBucket,
                        errCode: err.name,
                        error: err.message,
                        method: 'SetupReplication._createBucket',
                    });
                    return cb(null, {});
                }
                this._log.error('error creating a bucket', {
                    where,
                    bucket: where === 'source' ? this._sourceBucket :
                        this._targetBucket,
                    errCode: err.name,
                    error: err.message,
                    method: 'SetupReplication._createBucket',
                });
                return cb(err);
            });
    }

    _createRole(where, cb) {
        this.retry({
            actionDesc: `create ${where} role`,
            logFields: {},
            actionFunc: done => this._createRoleOnce(where, done),
            shouldRetryFunc: err => err.retryable,
            log: this._log,
        }, cb);
    }

    _createRoleOnce(where, cb) {
        const params = {
            AssumeRolePolicyDocument: JSON.stringify(trustPolicy),
            RoleName: `bb-replication-${Date.now()}`,
            Path: '/',
        };

        const command = new CreateRoleCommand(params);
        this._iamClients[where].send(command)
            .then(res => {
                this._log.debug('Created role', {
                    where,
                    bucket: where === 'source' ? this._sourceBucket :
                        this._targetBucket,
                    method: 'SetupReplication._createRole',
                });
                cb(null, res);
            })
            .catch(err => {
                this._log.error('error creating a role', {
                    where,
                    bucket: where === 'source' ? this._sourceBucket :
                        this._targetBucket,
                    errCode: err.name,
                    error: err.message,
                    method: 'SetupReplication._createRole',
                });
                cb(err);
            });
    }

    _createPolicy(where, cb) {
        this.retry({
            actionDesc: `create ${where} policy`,
            logFields: {},
            actionFunc: done => this._createPolicyOnce(where, done),
            shouldRetryFunc: err => err.retryable,
            log: this._log,
        }, cb);
    }

    _createPolicyOnce(where, cb) {
        const params = {
            PolicyDocument: JSON.stringify(this._buildResourcePolicy(where)),
            PolicyName: `bb-replication-${Date.now()}`,
        };
        
        const command = new CreatePolicyCommand(params);
        this._iamClients[where].send(command)
            .then(res => {
                this._log.debug('Created policy', {
                    where,
                    bucket: where === 'source' ? this._sourceBucket :
                        this._targetBucket,
                    method: 'SetupReplication._createPolicy',
                });
                cb(null, res);
            })
            .catch(err => {
                this._log.error('error creating policy', {
                    where,
                    bucket: where === 'source' ? this._sourceBucket :
                        this._targetBucket,
                    errCode: err.name,
                    error: err.message,
                    method: 'SetupReplication._createPolicy',
                });
                cb(err);
            });
    }

    _buildResourcePolicy(where) {
        const policy = {
            Version: '2012-10-17',
            Statement: [],
        };
        const bucket = where === 'source' ? this._sourceBucket :
                  this._targetBucket;
        if (where === 'source') {
            policy.Statement.push({
                Effect: 'Allow',
                Action: [
                    's3:ListBucket',
                    's3:GetReplicationConfiguration',
                ],
                Resource: [`arn:aws:s3:::${bucket}`],
            });
            const objActions = [
                's3:GetObjectVersion',
                's3:GetObjectVersionAcl',
                's3:ReplicateObject',
            ];
            if (this._targetIsExternal) {
                objActions.push('s3:GetObjectVersionTagging');
            }
            policy.Statement.push({
                Effect: 'Allow',
                Action: objActions,
                Resource: [`arn:aws:s3:::${bucket}/*`],
            });
        }
        if (where === 'target') {
            const actions = [
                's3:ReplicateObject',
                's3:ReplicateDelete',
            ];
            if (this._targetIsExternal) {
                actions.push('s3:ReplicateTags');
            }
            policy.Statement.push({
                Effect: 'Allow',
                Action: actions,
                Resource: [`arn:aws:s3:::${bucket}/*`],
            });
        }
        return policy;
    }

    _enableVersioning(where, cb) {
        this.retry({
            actionDesc: `enable versioning on ${where}`,
            logFields: {},
            actionFunc: done => this._enableVersioningOnce(where, done),
            shouldRetryFunc: err => err.retryable,
            log: this._log,
        }, cb);
    }

    _enableVersioningOnce(where, cb) {
        const bucket = where === 'source' ? this._sourceBucket :
            this._targetBucket;
        const params = {
            Bucket: bucket,
            VersioningConfiguration: {
                Status: 'Enabled',
            },
        };
        
        const command = new PutBucketVersioningCommand(params);
        this._s3Clients[where].send(command)
            .then(res => {
                this._log.debug('Versioning enabled', {
                    where,
                    bucket: where === 'source' ? this._sourceBucket :
                        this._targetBucket,
                    method: 'SetupReplication._enableVersioning',
                });
                cb(null, res);
            })
            .catch(err => {
                this._log.error('error enabling versioning', {
                    where,
                    bucket: where === 'source' ? this._sourceBucket :
                        this._targetBucket,
                    errCode: err.name,
                    error: err.message,
                    method: 'SetupReplication._enableVersioning',
                });
                cb(err);
            });
    }

    _attachResourcePolicy(policyArn, roleName, where, cb) {
        this.retry({
            actionDesc: `attach resource policy on ${where}`,
            logFields: {},
            actionFunc: done =>
                this._attachResourcePolicyOnce(policyArn, roleName,
                                               where, done),
            shouldRetryFunc: err => err.retryable,
            log: this._log,
        }, cb);
    }

    _attachResourcePolicyOnce(policyArn, roleName, where, cb) {
        const params = {
            PolicyArn: policyArn,
            RoleName: roleName,
        };
        
        const command = new AttachRolePolicyCommand(params);
        this._iamClients[where].send(command)
            .then(res => {
                this._log.debug('Attached resource policy', {
                    where,
                    bucket: where === 'source' ? this._sourceBucket :
                        this._targetBucket,
                    method: 'SetupReplication._attachResourcePolicy',
                });
                cb(null, res);
            })
            .catch(err => {
                this._log.error('error attaching resource policy', {
                    where,
                    bucket: where === 'source' ? this._sourceBucket :
                        this._targetBucket,
                    errCode: err.name,
                    error: err.message,
                    method: 'SetupReplication._attachResourcePolicy',
                });
                cb(err);
            });
    }

    _enableReplication(roleArns, cb) {
        this.retry({
            actionDesc: 'enable bucket replication',
            logFields: {},
            actionFunc: done => this._enableReplicationOnce(roleArns, done),
            shouldRetryFunc: err => err.retryable,
            log: this._log,
        }, cb);
    }

    _enableReplicationOnce(roleArns, cb) {
        const destination = { Bucket: `arn:aws:s3:::${this._targetBucket}` };
        if (this._targetSiteName !== undefined) {
            destination.StorageClass = this._targetSiteName;
        }
        const params = {
            Bucket: this._sourceBucket,
            ReplicationConfiguration: {
                Role: roleArns,
                Rules: [{
                    Destination: destination,
                    Prefix: '',
                    Status: 'Enabled',
                }],
            },
        };
        
        const command = new PutBucketReplicationCommand(params);
        this._s3Clients.source.send(command)
            .then(res => {
                this._log.debug('Bucket replication enabled', {
                    method: 'SetupReplication._enableReplication',
                });
                cb(null, res);
            })
            .catch(err => {
                this._log.error('error enabling replication', {
                    bucket: this._sourceBucket,
                    errCode: err.name,
                    error: err.message,
                    method: 'SetupReplication._enableReplication',
                });
                cb(err);
            });
    }

    setupReplication(cb) {
        let sourceRole;
        let targetRole;
        let sourcePolicyArn;
        let targetPolicyArn;
        return async.waterfall([
            next => async.series({
                sourceBucket: done => this._createBucket('source', done),
                targetBucket: done => (this._targetIsExternal ? done() :
                    this._createBucket('target', done)),
                sourceRole: done => this._createRole('source', done),
                targetRole: done => (this._targetIsExternal ? done() :
                    this._createRole('target', done)),
                sourcePolicy: done => this._createPolicy('source', done),
                targetPolicy: done => (this._targetIsExternal ? done() :
                        this._createPolicy('target', done)),
            }, next),
            (data, next) => {
                sourceRole = data.sourceRole.Role;
                targetRole = this._targetIsExternal ? undefined :
                    data.targetRole.Role;
                sourcePolicyArn = data.sourcePolicy.Policy.Arn;
                targetPolicyArn = this._targetIsExternal ? undefined :
                    data.targetPolicy.Policy.Arn;
                const roleArns = this._targetIsExternal ? sourceRole.Arn :
                    `${sourceRole.Arn},${targetRole.Arn}`;
                async.series([
                    done => this._enableVersioning('source', done),
                    done => (this._targetIsExternal ? done() :
                        this._enableVersioning('target', done)),
                    done => this._attachResourcePolicy(sourcePolicyArn,
                        sourceRole.RoleName, 'source', done),
                    done => (this._targetIsExternal ? done() :
                        this._attachResourcePolicy(targetPolicyArn,
                        targetRole.RoleName, 'target', done)),
                    done => this._enableReplication(roleArns, done),
                ], next);
            },
            (args, next) => (this._checkSanityEnabled ?
                             this.checkSanity(next) : next()),
        ], err => {
            if (err) {
                return cb(err);
            }
            return cb(null, { sourceRoleArn: sourceRole.Arn,
                              targetRoleArn: targetRole && targetRole.Arn,
                              sourcePolicyArn, targetPolicyArn });
        });
    }
}

module.exports = SetupReplication;
