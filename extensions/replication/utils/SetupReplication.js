const async = require('async');
const { S3, IAM } = require('aws-sdk');

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
    const policy = {
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
    if (this._targetIsExternal) {
        policy.Statement[0].Action.push('s3:GetObjectVersionTagging');
        policy.Statement[2].Action.push('s3:ReplicateTags');
    }
    return policy;
}

function _setupS3Client(transport, endpoint, credentials) {
    return new S3({
        endpoint: `${endpoint}`,
        sslEnabled: transport === 'https',
        credentials,
        s3ForcePathStyle: true,
        signatureVersion: 'v4',
        httpOptions: { timeout: 0 },
        maxRetries: 0,
    });
}

function _setupIAMClient(where, transport, endpoint, credentials) {
    const httpOptions = { timeout: 1000 };
    let iamEndpoint = endpoint;
    if (where === 'target') {
        const [host] = endpoint.split(':');
        const destIAMPort = 8600;
        iamEndpoint = `${host}:${destIAMPort}`;
    }

    return new IAM({
        endpoint: `${transport}://${iamEndpoint}`,
        sslEnabled: transport === 'https',
        credentials,
        maxRetries: 0,
        region: 'us-east-2',
        signatureCache: false,
        httpOptions,
    });
}

class SetupReplication {
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
     * @param {Object} params.log - werelogs request logger object
     */
    constructor(params) {
        const { source, target, log } = params;
        this._log = log;
        this._sourceBucket = source.bucket;
        this._targetBucket = target.bucket;
        this._targetIsExternal = target.isExternal;
        this._targetSiteName = target.siteName;
        this.destHosts = target.hosts;
        const destHost = this.destHosts.pickHost();
        this._s3Clients = {
            source: _setupS3Client(source.transport,
                `${source.s3.host}:${source.s3.port}`, source.credentials),
            target: target.isExternal ? undefined :
                _setupS3Client(target.transport,
                `${destHost.host}:${destHost.port}`, target.credentials),
        };
        this._iamClients = {
            source: _setupIAMClient('source', source.transport,
                `${source.vault.host}:${source.vault.adminPort}`,
                source.credentials),
            // XXX use target port through nginx gateway
            target: target.isExternal ? undefined : _setupIAMClient('target',
                target.transport, `${destHost.host}:${source.vault.adminPort}`,
                target.credentials),
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
        this._s3Clients[where].headBucket({ Bucket: bucket }, err => {
            if (err) {
                this._log.error('bucket sanity check error', {
                    bucket: where === 'source' ? this._sourceBucket :
                        this._targetBucket,
                    errCode: err.code,
                    error: err.message,
                    method: 'SetupReplication._isValidBucket',
                });
                return cb(err);
            }
            return cb();
        });
    }

    _isVersioningEnabled(where, cb) {
        // Does the bucket have versioning enabled?
        const bucket = where === 'source' ? this._sourceBucket :
            this._targetBucket;
        this._s3Clients[where].getBucketVersioning({ Bucket: bucket },
            (err, res) => {
                if (err) {
                    this._log.error('versioning sanity check error: ' +
                        'Cannot retrieve versioning configuration', {
                            bucket: where === 'source' ? this._sourceBucket :
                                this._targetBucket,
                            errCode: err.code,
                            error: err.message,
                            method: 'SetupReplication._isVersioningEnabled',
                        }
                    );
                    return cb(err);
                }
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
            }
        );
    }

    _isValidRole(where, arnObj, cb) {
        // Is the role mentioned in the replication config available in IAM

        // Goal is to get Role given known ARN.
        // If err, there is no matching role
        const arn = arnObj[where];
        const roleName = arn.split('/').pop();

        this._iamClients[where].getRole({ RoleName: roleName }, (err, res) => {
            if (err) {
                this._log.error('role validation sanity check error: ' +
                    'Cannot retrieve role configuration', {
                        bucket: where === 'source' ? this._sourceBucket :
                            this._targetBucket,
                        errCode: err.code,
                        error: err.message,
                        method: 'SetupReplication._isValidRole',
                    }
                );
                return cb(err);
            }
            if (arn !== res.Role.Arn) {
                const error = new Error('Expected ARN to match. A mis-match ' +
                    'was found between the ARN found in ' +
                    '`getBucketReplication` and ARN found in `getRole`.');
                this._log.error('role validation sanity check error: ' +
                    'ARN mis-match', {
                        bucket: where === 'source' ? this._sourceBucket :
                            this._targetBucket,
                        error: err.message,
                        method: 'SetupReplication._isVersioningEnabled',
                    }
                );
                return cb(error);
            }
            return cb(null, arnObj);
        });
    }

    _isReplicationEnabled(src, cb) {
        // Is the Replication config enabled?
        this._s3Clients[src].getBucketReplication(
            { Bucket: this._sourceBucket },
            (err, res) => {
                if (err) {
                    this._log.error('replication status sanity check error: ' +
                        'Cannot retrieve replication configuration', {
                            errCode: err.code,
                            error: err.message,
                            method: 'SetupReplication._isReplicationEnabled',
                        }
                    );
                    return cb(err);
                }
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
            }
        );
    }

    _createBucket(where, cb) {
        const bucket = where === 'source' ? this._sourceBucket :
            this._targetBucket;
        this._s3Clients[where].createBucket({ Bucket: bucket }, (err, res) => {
            if (err && err.code !== 'BucketAlreadyOwnedByYou') {
                this._log.error('error creating a bucket', {
                    bucket: where === 'source' ? this._sourceBucket :
                        this._targetBucket,
                    errCode: err.code,
                    error: err.message,
                    method: 'SetupReplication._createBucket',
                });
                return cb(err);
            }
            if (err && err.code === 'BucketAlreadyOwnedByYou') {
                this._log.debug('Bucket already exists. Continuing setup.', {
                    bucket: where === 'source' ? this._sourceBucket :
                        this._targetBucket,
                    errCode: err.code,
                    error: err.message,
                    method: 'SetupReplication._createBucket',
                });
            } else {
                this._log.debug('Created bucket', {
                    bucket: where === 'source' ? this._sourceBucket :
                        this._targetBucket,
                    method: 'SetupReplication._createBucket',
                });
            }
            return cb(null, res);
        });
    }

    _createRole(where, cb) {
        const params = {
            AssumeRolePolicyDocument: JSON.stringify(trustPolicy),
            RoleName: `bb-replication-${Date.now()}`,
            Path: '/',
        };

        this._iamClients[where].createRole(params, (err, res) => {
            if (err) {
                this._log.error('error creating a role', {
                    bucket: where === 'source' ? this._sourceBucket :
                        this._targetBucket,
                    errCode: err.code,
                    error: err.message,
                    method: 'SetupReplication._createRole',
                });
                return cb(err);
            }
            this._log.debug('Created role', {
                bucket: where === 'source' ? this._sourceBucket :
                    this._targetBucket,
                method: '_createRole',
            });
            return cb(null, res);
        });
    }

    _createPolicy(where, cb) {
        const params = {
            PolicyDocument: JSON.stringify(
                _buildResourcePolicy(this._sourceBucket, this._targetBucket)),
            PolicyName: `bb-replication-${Date.now()}`,
        };
        this._iamClients[where].createPolicy(params, (err, res) => {
            if (err) {
                this._log.error('error creating policy', {
                    bucket: where === 'source' ? this._sourceBucket :
                        this._targetBucket,
                    errCode: err.code,
                    error: err.message,
                    method: 'SetupReplication._createPolicy',
                });
                return cb(err);
            }
            this._log.debug('Created policy', {
                bucket: where === 'source' ? this._sourceBucket :
                    this._targetBucket,
                method: '_createPolicy',
            });
            return cb(null, res);
        });
    }

    _enableVersioning(where, cb) {
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
                this._log.error('error enabling versioning', {
                    bucket: where === 'source' ? this._sourceBucket :
                        this._targetBucket,
                    errCode: err.code,
                    error: err.message,
                    method: 'SetupReplication._enableVersioning',
                });
                return cb(err);
            }
            this._log.debug('Versioning enabled', {
                bucket: where === 'source' ? this._sourceBucket :
                    this._targetBucket,
                method: '_enableVersioning',
            });
            return cb(null, res);
        });
    }

    _attachResourcePolicy(policyArn, roleName, where, cb) {
        const params = {
            PolicyArn: policyArn,
            RoleName: roleName,
        };
        this._iamClients[where].attachRolePolicy(params, (err, res) => {
            if (err) {
                this._log.error('error attaching resource policy', {
                    bucket: where === 'source' ? this._sourceBucket :
                        this._targetBucket,
                    errCode: err.code,
                    error: err.message,
                    method: 'SetupReplication._attachResourcePolicy',
                });
                return cb(err);
            }
            this._log.debug('Attached resource policy', {
                bucket: where === 'source' ? this._sourceBucket :
                    this._targetBucket,
                method: '_attachResourcePolicy',
            });
            return cb(null, res);
        });
    }

    _enableReplication(roleArns, cb) {
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
        this._s3Clients.source.putBucketReplication(params, (err, res) => {
            if (err) {
                this._log.error('error enabling replication', {
                    errCode: err.code,
                    error: err.message,
                    method: 'SetupReplication._enableReplication',
                });
                return cb(err);
            }
            this._log.debug('Bucket replication enabled', {
                method: '_enableReplication',
            });
            return cb(null, res);
        });
    }

    setupReplication(cb) {
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
                const sourceRole = data.sourceRole.Role;
                const targetRole = this._targetIsExternal ? undefined :
                    data.targetRole.Role;
                const sourcePolicyArn = data.sourcePolicy.Policy.Arn;
                const targetPolicyArn = this._targetIsExternal ? undefined :
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
            (args, next) => this.checkSanity(next),
        ], cb);
    }
}

module.exports = SetupReplication;
