const async = require('async');
const program = require('commander');
const { S3, IAM, SharedIniFileCredentials } = require('aws-sdk');

const { Logger } = require('werelogs');

const config = require('../conf/Config');
const { proxyIAMPath } = require('../extensions/replication/constants');

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

function _setupS3Client(transport, endpoint, profile) {
    const credentials = new SharedIniFileCredentials({ profile });
    return new S3({
        endpoint: `${transport}://${endpoint}`,
        sslEnabled: transport === 'https',
        credentials,
        s3ForcePathStyle: true,
    });
}


function _setupIAMClient(transport, endpoint, profile) {
    const credentials = new SharedIniFileCredentials({ profile });
    return new IAM({
        endpoint: `${transport}://${endpoint}`,
        sslEnabled: transport === 'https',
        credentials,
        maxRetries: 0,
        region: 'us-east-1',
        signatureCache: false,
        httpOptions: {
            timeout: 1000,
            proxy: proxyIAMPath,
        },
    });
}

class _SetupReplication {
    /**
     * This class sets up two buckets for replication.
     * @constructor
     * @param {String} sourceBucket - Source Bucket Name
     * @param {String} targetBucket - Target Bucket Name
     * @param {String} sourceProfile - Source Credentials Profile
     * @param {String} targetProfile - Target Credentials Profile
     * @param {Object} log - Werelogs Request Logger object
     * @param {Object} config - bucket configurations
     */
    constructor(sourceBucket, targetBucket, sourceProfile, targetProfile, log,
        config) {
        const { source, destination } = config.extensions.replication;
        this._log = log;
        this._sourceBucket = sourceBucket;
        this._targetBucket = targetBucket;
        // TODO: support bootstrap list failover
        const destEndpoint = destination.bootstrapList[0].servers[0];
        this._s3Clients = {
            source: _setupS3Client(source.transport,
                `${source.s3.host}:${source.s3.port}`,
                sourceProfile),
            target: _setupS3Client(destination.transport, destEndpoint,
                targetProfile),
        };
        this._iamClients = {
            source: _setupIAMClient(source.transport,
                `${source.auth.vault.host}:${source.auth.vault.iamPort}`,
                sourceProfile),
            target: _setupIAMClient(source.transport, destEndpoint,
                targetProfile),
        };
    }

    checkSanity(cb) {
        return async.waterfall([
            next => this._isValidBucket('source', next),
            next => this._isValidBucket('target', next),
            next => this._isVersioningEnabled('source', next),
            next => this._isVersioningEnabled('target', next),
            next => this._isReplicationEnabled('source', next),
            (arns, next) => this._arnParser(arns, next),
            (arnObj, next) => this._isValidRole('source', arnObj, next),
            (arnObj, next) => this._isValidRole('target', arnObj, next),
        ], cb);
    }

    _arnParser(arns, cb) {
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
                    error: err.message,
                    method: '_SetupReplication._isValidBucket',
                    errorStack: err.stack,
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
                            error: err.message,
                            method: '_SetupReplication._isVersioningEnabled',
                            errorStack: err.stack,
                        }
                    );
                    return cb(err);
                }
                if (res.Status === 'Disabled') {
                    const error = new Error('Expected bucket versioning to ' +
                        'be Enabled. Status is still Disabled.');
                    this._log.error('versioning sanity check error: ' +
                        'Status Disabled', {
                            error: error.message,
                            method: '_SetupReplication._isVersioningEnabled',
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
                        error: err.message,
                        method: '_SetupReplication._isValidRole',
                        errorStack: err.stack,
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
                        error: err.message,
                        method: '_SetupReplication._isVersioningEnabled',
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
                            error: err.message,
                            method: '_SetupReplication._isReplicationEnabled',
                            errorStack: err.stack,
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
                            method: '_SetupReplication._isReplicationEnabled',
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
            if (err) {
                this._log.error('error creating a bucket', {
                    error: err.message,
                    method: '_SetupReplication._createBucket',
                    errorStack: err.stack,
                });
                return cb(err);
            }
            this._log.debug('Created bucket', {
                bucket: where,
                response: res,
                method: '_createBucket',
            });
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
                    error: err.message,
                    method: '_SetupReplication._createRole',
                    errorStack: err.stack,
                });
                return cb(err);
            }
            this._log.debug('Created role', {
                bucket: where,
                response: res,
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
                    error: err.message,
                    method: '_SetupReplication._createPolicy',
                    errorStack: err.stack,
                });
                return cb(err);
            }
            this._log.debug('Created policy', {
                bucket: where,
                response: res,
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
                    error: err.message,
                    method: '_SetupReplication._enableVersioning',
                    errorStack: err.stack,
                });
                return cb(err);
            }
            this._log.debug('Versioning enabled', {
                bucket: where,
                response: res,
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
                    error: err.message,
                    method: '_SetupReplication._attachResourcePolicy',
                    errorStack: err.stack,
                });
                return cb(err);
            }
            this._log.debug('Attached resource policy', {
                bucket: where,
                response: res,
                method: '_attachResourcePolicy',
            });
            return cb(null, res);
        });
    }

    _enableReplication(roleArns, cb) {
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
                this._log.error('error enabling replication', {
                    error: err.message,
                    method: '_SetupReplication._enableReplication',
                    errorStack: err.stack,
                });
                return cb(err);
            }
            this._log.debug('Bucket replication enabled', {
                response: res,
                method: '_enableReplication',
            });
            return cb(null, res);
        });
    }

    setupReplication(cb) {
        return async.waterfall([
            next => async.parallel({
                sourceBucket: done => this._createBucket('source', done),
                targetBucket: done => this._createBucket('target', done),
                sourceRole: done => this._createRole('source', done),
                targetRole: done => this._createRole('target', done),
                sourcePolicy: done => this._createPolicy('source', done),
                targetPolicy: done => this._createPolicy('target', done),
            }, next),
            (data, next) => {
                const sourceRole = data.sourceRole.Role;
                const targetRole = data.targetRole.Role;
                const sourcePolicyArn = data.sourcePolicy.Policy.Arn;
                const targetPolicyArn = data.targetPolicy.Policy.Arn;
                const roleArns = `${sourceRole.Arn},${targetRole.Arn}`;

                async.series([
                    done => this._enableVersioning('source', done),
                    done => this._enableVersioning('target', done),
                    done => this._attachResourcePolicy(sourcePolicyArn,
                        sourceRole.RoleName, 'source', done),
                    done => this._attachResourcePolicy(targetPolicyArn,
                        targetRole.RoleName, 'target', done),
                    done => this._enableReplication(roleArns, done),
                ], next);
            },
            (args, next) => this.checkSanity(next),
        ], cb);
    }
}

program
    .version('1.0.0')
    .command('setup')
    // .arguments('<source-bucket> <destination-bucket>')
    .option('--source-bucket', 'source bucket name')
    .option('--source-profile', 'aws/credentials profile to use for source')
    .option('--target-bucket', 'target bucket name')
    .option('--target-profile', 'aws/credentials profile to use for target')
    .action((cmd, options) => {
        const log = new Logger('BackbeatSetup').newRequestLogger();
        const sourceBucket = options['source-bucket'];
        const targetBucket = options['target-bucket'];
        const sourceProfile = options['source-profile'];
        const targetProfile = options['target-profile'];
        const s = new _SetupReplication(sourceBucket, targetBucket,
            sourceProfile, targetProfile, log, config);
        s.setupReplication(err => {
            if (err) {
                log.error('replication setup failed', {
                    error: err.message,
                    errorStack: err.stack,
                });
                return process.exit(1);
            }
            log.info('replication setup successful');
            return process.exit();
        });
    });

program
    .command('validate')
    .option('--source-bucket', 'source bucket name')
    .option('--source-profile', 'aws/credentials profile to use for source')
    .option('--target-bucket', 'target bucket name')
    .option('--target-profile', 'aws/credentials profile to use for target')
    .action((cmd, options) => {
        const sourceBucket = options['source-bucket'];
        const targetBucket = options['target-bucket'];
        const sourceProfile = options['source-profile'];
        const targetProfile = options['target-profile'];

        const log = new Logger('BackbeatSetup').newRequestLogger();
        const s = new _SetupReplication(sourceBucket, targetBucket,
            sourceProfile, targetProfile, log, config);
        s.checkSanity(err => {
            if (err) {
                log.error('replication validation check failed', {
                    error: err.message,
                    errorStack: err.stack,
                });
                return process.exit(1);
            }
            log.info('replication is correctly setup');
            return process.exit();
        });
    });

program.parse(process.argv);
const validCommands = program.commands.map(n => n.name());

// Is the command given invalid or are there too few or
// too many arguments passed
if (validCommands.indexOf(process.argv[2]) < 0 || program.args.length !== 3) {
    program.help();
    process.exit(1);
}
