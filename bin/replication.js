const async = require('async');
const program = require('commander');
const { S3, IAM, SharedIniFileCredentials } = require('aws-sdk');

const werelogs = require('werelogs');
const { RoundRobin } = require('arsenal').network;
const Logger = werelogs.Logger;
const config = require('../conf/Config');
werelogs.configure({
    level: config.log.logLevel,
    dump: config.log.dumpLevel,
});

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
        endpoint: `${endpoint}`,
        sslEnabled: transport === 'https',
        credentials,
        s3ForcePathStyle: true,
        signatureVersion: 'v4',
    });
}

function _setupIAMClient(where, transport, endpoint, profile) {
    const credentials = new SharedIniFileCredentials({ profile });
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
        this.destHosts =
            new RoundRobin(destination.bootstrapList[0].servers);
        const verifySourceProfile = sourceProfile === undefined ?
            'default' : sourceProfile;
        const verifyTargetProfile = targetProfile === undefined ?
            'default' : targetProfile;
        const destHost = this.destHosts.pickHost().host;
        this._s3Clients = {
            source: _setupS3Client(source.transport,
                `${source.s3.host}:${source.s3.port}`,
                verifySourceProfile),
            target: _setupS3Client(destination.transport, destHost,
                verifyTargetProfile),
        };
        this._iamClients = {
            source: _setupIAMClient('source', source.transport,
                `${source.auth.vault.host}:${source.auth.vault.adminPort}`,
                verifySourceProfile),
            target: _setupIAMClient('target', destination.transport,
                `${destHost}:${source.auth.vault.adminPort}`,
                verifyTargetProfile),
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
                    bucket: where === 'source' ? this._sourceBucket :
                        this._targetBucket,
                    errCode: err.code,
                    error: err.message,
                    method: '_SetupReplication._isValidBucket',
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
                            method: '_SetupReplication._isVersioningEnabled',
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
                        bucket: where === 'source' ? this._sourceBucket :
                            this._targetBucket,
                        errCode: err.code,
                        error: err.message,
                        method: '_SetupReplication._isValidRole',
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
                            errCode: err.code,
                            error: err.message,
                            method: '_SetupReplication._isReplicationEnabled',
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
            if (err && err.code !== 'BucketAlreadyOwnedByYou') {
                this._log.error('error creating a bucket', {
                    bucket: where === 'source' ? this._sourceBucket :
                        this._targetBucket,
                    errCode: err.code,
                    error: err.message,
                    method: '_SetupReplication._createBucket',
                });
                return cb(err);
            }
            if (err && err.code === 'BucketAlreadyOwnedByYou') {
                this._log.debug('Bucket already exists. Continuing setup.', {
                    bucket: where === 'source' ? this._sourceBucket :
                        this._targetBucket,
                    errCode: err.code,
                    error: err.message,
                    method: '_SetupReplication._createBucket',
                });
            } else {
                this._log.debug('Created bucket', {
                    bucket: where === 'source' ? this._sourceBucket :
                        this._targetBucket,
                    method: '_SetupReplication._createBucket',
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
                    method: '_SetupReplication._createRole',
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
                    method: '_SetupReplication._createPolicy',
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
                    method: '_SetupReplication._enableVersioning',
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
                    method: '_SetupReplication._attachResourcePolicy',
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
                    errCode: err.code,
                    error: err.message,
                    method: '_SetupReplication._enableReplication',
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
    .option('--source-bucket <name>', '[required] source bucket name')
    .option('--source-profile <name>',
            'aws/credentials profile to use for source')
    .option('--target-bucket <name>', 'target bucket name')
    .option('--target-profile <name>',
            'aws/credentials profile to use for target')
    .action(options => {
        const log = new Logger('BackbeatSetup').newRequestLogger();

        const sourceBucket = options.sourceBucket;
        const targetBucket = options.targetBucket;
        const sourceProfile = options.sourceProfile;
        const targetProfile = options.targetProfile;

        // Required options
        if (!sourceBucket || !targetBucket) {
            program.commands.find(n => n._name === 'setup').help();
            process.exit(1);
        }

        const s = new _SetupReplication(sourceBucket, targetBucket,
            sourceProfile, targetProfile, log, config);
        s.setupReplication(err => {
            if (err) {
                log.error('replication setup failed', {
                    errCode: err.code,
                    error: err.message,
                });
                process.exit(1);
            }
            log.info('replication setup successful');
            process.exit();
        });
    });

program
    .command('validate')
    .option('--source-bucket <name>', 'source bucket name')
    .option('--source-profile <name>',
            'aws/credentials profile to use for source')
    .option('--target-bucket <name>', 'target bucket name')
    .option('--target-profile <name>',
            'aws/credentials profile to use for target')
    .action(options => {
        const log = new Logger('BackbeatSetup').newRequestLogger();

        const sourceBucket = options.sourceBucket;
        const targetBucket = options.targetBucket;
        const sourceProfile = options.sourceProfile;
        const targetProfile = options.targetProfile;

        // Required options
        if (!sourceBucket || !targetBucket ||
            !sourceProfile || !targetProfile) {
            program.commands.find(n => n._name === 'validate').help();
            process.exit(1);
        }

        const s = new _SetupReplication(sourceBucket, targetBucket,
            sourceProfile, targetProfile, log, config);
        s.checkSanity(err => {
            if (err) {
                log.error('replication validation check failed', {
                    errCode: err.code,
                    error: err.message,
                });
                process.exit(1);
            }
            log.info('replication is correctly setup');
            process.exit();
        });
    });

program.parse(process.argv);
const validCommands = program.commands.map(n => n._name);

// Is the command given invalid or are there too few arguments passed
if (!validCommands.includes(process.argv[2])) {
    program.help();
    process.exit(1);
}
