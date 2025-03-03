process.env.BACKBEAT_CONFIG_FILE = 'tests/functional/queuePopulator/config/s3c-config.json';

const assert = require('assert');
const async = require('async');
const AWS = require('aws-sdk');

const config = require('../../../lib/Config');
const zkConfig = config.zookeeper;
const kafkaConfig = config.kafka;
const extConfigs = config.extensions;
const qpConfig = config.queuePopulator;
const httpsConfig = config.internalHttps;
const mConfig = config.metrics;
const rConfig = config.redis;
const vConfig = config.vaultAdmin;

const QueuePopulator = require('../../../lib/queuePopulator/QueuePopulator');

const S3 = AWS.S3;
const s3config = {
    endpoint: `http://${config.s3.host}:${config.s3.port}`,
    s3ForcePathStyle: true,
    credentials: new AWS.Credentials('accessKey1', 'verySecretKey1'),
};

const maxRead = qpConfig.batchMaxRead;
const timeoutMs = qpConfig.batchTimeoutMs;

class S3Helper {
    constructor(client) {
        this.s3 = client;
        this.bucket = undefined;

        this._scenario = [
            {
                keyNames: ['object-1', 'object-2', 'object-3'],
            },
        ];
    }

    setAndCreateBucket(name, cb) {
        this.bucket = name;
        this.s3.createBucket({
            Bucket: name,
        }, err => {
            assert.ifError(err);
            cb();
        });
    }

    setBucketVersioning(status, cb) {
        this.s3.putBucketVersioning({
            Bucket: this.bucket,
            VersioningConfiguration: {
                Status: status,
            },
        }, cb);
    }

    createObjects(scenarioNumber, cb) {
        async.forEachOf(this._scenario[scenarioNumber].keyNames,
        (key, i, done) => {
            this.s3.putObject({
                Body: '',
                Bucket: this.bucket,
                Key: key,
            }, done);
        }, err => {
            assert.ifError(err);
            return cb();
        });
    }

    createVersions(scenarioNumber, cb) {
        async.series([
            next => this.setBucketVersioning('Enabled', next),
            next => this.createObjects(scenarioNumber, next),
        ], err => {
            assert.ifError(err);
            return cb();
        });
    }

    emptyAndDeleteBucket(cb) {
        if (!this.bucket) {
            return cb();
        }
        return async.waterfall([
            next => this.s3.getBucketVersioning({ Bucket: this.bucket }, next),
            (data, next) => {
                if (data.Status === 'Enabled' || data.Status === 'Suspended') {
                    // listObjectVersions
                    return this.s3.listObjectVersions({
                        Bucket: this.bucket,
                    }, (err, data) => {
                        assert.ifError(err);

                        const list = [
                            ...data.Versions.map(v => ({
                                Key: v.Key,
                                VersionId: v.VersionId,
                            })),
                            ...data.DeleteMarkers.map(dm => ({
                                Key: dm.Key,
                                VersionId: dm.VersionId,
                            })),
                        ];

                        if (list.length === 0) {
                            return next(null, null);
                        }

                        return this.s3.deleteObjects({
                            Bucket: this.bucket,
                            Delete: { Objects: list },
                        }, next);
                    });
                }

                return this.s3.listObjects({ Bucket: this.bucket },
                (err, data) => {
                    assert.ifError(err);

                    const list = data.Contents.map(c => ({ Key: c.Key }));

                    return this.s3.deleteObjects({
                        Bucket: this.bucket,
                        Delete: { Objects: list },
                    }, next);
                });
            },
            (data, next) => this.s3.deleteBucket({ Bucket: this.bucket }, next),
        ], cb);
    }

    setBucketReplicationConfigurations(cb) {
        const params = {
            Bucket: this.bucket,
            ReplicationConfiguration: {
                Role: 'arn:aws:iam::0000:role/role-src,arn:aws:iam::0000:role/role-dst',
                Rules: [{
                    Destination: {
                        Bucket: 'arn:aws:s3:::destination-bucket',
                    },
                    Prefix: '',
                    Status: 'Enabled'
                }]
            }
        };
        return this.s3.putBucketReplication(params, cb);
    }
}

describe('Queue Populator', () => {
    let qp;
    let s3;
    let s3Helper;

    before(done => {
        s3 = new S3(s3config);
        s3Helper = new S3Helper(s3);
        qp = new QueuePopulator(zkConfig, kafkaConfig, qpConfig,
            httpsConfig, mConfig, rConfig, vConfig, extConfigs);
        qp.open(done);
    });

    afterEach(done => {
        s3Helper.emptyAndDeleteBucket(err => {
            assert.ifError(err);
            done();
        });
    });

    // TODO: BB-660
    // after(function after(done) {
    //     this.timeout(60000);
    //     qp.close(() => {
    //         // ignoring errors that may occur when metrics consumer
    //         // tries to consume at shutdown
    //         done();
    //     });
    // });

    it('should process log entries without failure', done => {
        async.series([
            next => s3Helper.setAndCreateBucket('bucket-1', next),
            next => s3Helper.setBucketVersioning('Enabled', next),
            next => s3Helper.setBucketReplicationConfigurations(next),
            next => s3Helper.createVersions(0, next),
            next => qp.processLogEntries({ maxRead, timeoutMs }, next),
        ], err => {
            assert.ifError(err);
            done();
        });
    });
});
