process.env.BACKBEAT_CONFIG_FILE = 'tests/functional/queuePopulator/config/s3c-config.json';

const assert = require('assert');
const async = require('async');
const {
    S3Client,
    CreateBucketCommand,
    PutBucketVersioningCommand,
    GetBucketVersioningCommand,
    PutObjectCommand,
    ListObjectVersionsCommand,
    ListObjectsCommand,
    DeleteObjectsCommand,
    DeleteBucketCommand,
    PutBucketReplicationCommand,
} = require('@aws-sdk/client-s3');

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

const S3 = S3Client;
const s3config = {
    endpoint: `http://${config.s3.host}:${config.s3.port}`,
    forcePathStyle: true,
    region: 'us-east-1',
    credentials: {
        accessKeyId: 'accessKey1',
        secretAccessKey: 'verySecretKey1',
    },
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
        (async () => {
            try {
                const command = new CreateBucketCommand({
                    Bucket: name,
                });
                await this.s3.send(command);
                cb();
            } catch (err) {
                assert.ifError(err);
                cb(err);
            }
        })();
    }

    setBucketVersioning(status, cb) {
        (async () => {
            try {
                const command = new PutBucketVersioningCommand({
                    Bucket: this.bucket,
                    VersioningConfiguration: {
                        Status: status,
                    },
                });
                await this.s3.send(command);
                cb();
            } catch (err) {
                cb(err);
            }
        })();
    }

    createObjects(scenarioNumber, cb) {
        (async () => {
            try {
                for (const key of this._scenario[scenarioNumber].keyNames) {
                    const command = new PutObjectCommand({
                        Body: '',
                        Bucket: this.bucket,
                        Key: key,
                    });
                    await this.s3.send(command);
                }
                cb();
            } catch (err) {
                assert.ifError(err);
                cb(err);
            }
        })();
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
            process.nextTick(cb);
            return;
        }
        
        // Defensive: ensure callback is called even if something goes wrong
        let callbackCalled = false;
        const safeCallback = err => {
            if (!callbackCalled) {
                callbackCalled = true;
                cb(err);
            }
        };
        
        // Safety timeout - call callback after 28 seconds no matter what
        const timeoutId = setTimeout(() => {
            safeCallback();
        }, 28000);
        
        // Perform cleanup
        (async () => {
            try {
                // Get bucket versioning status
                const getVersioningCommand = new GetBucketVersioningCommand({
                    Bucket: this.bucket,
                });
                const versioningData = await this.s3.send(getVersioningCommand);
                
                if (versioningData.Status === 'Enabled' ||
                    versioningData.Status === 'Suspended') {
                    // List object versions
                    const listVersionsCommand = new ListObjectVersionsCommand({
                        Bucket: this.bucket,
                    });
                    const versionsData = await this.s3.send(listVersionsCommand);
                    
                    const list = [
                        ...(versionsData.Versions || []).map(v => ({
                            Key: v.Key,
                            VersionId: v.VersionId,
                        })),
                        ...(versionsData.DeleteMarkers || []).map(dm => ({
                            Key: dm.Key,
                            VersionId: dm.VersionId,
                        })),
                    ];

                    if (list.length > 0) {
                        const deleteObjectsCommand = new DeleteObjectsCommand({
                            Bucket: this.bucket,
                            Delete: { Objects: list },
                        });
                        await this.s3.send(deleteObjectsCommand);
                    }
                } else {
                    // List objects without versions
                    const listObjectsCommand = new ListObjectsCommand({
                        Bucket: this.bucket,
                    });
                    const objectsData = await this.s3.send(listObjectsCommand);
                    
                    if (objectsData.Contents && objectsData.Contents.length > 0) {
                        const list = objectsData.Contents.map(c => ({ Key: c.Key }));
                        const deleteObjectsCommand = new DeleteObjectsCommand({
                            Bucket: this.bucket,
                            Delete: { Objects: list },
                        });
                        await this.s3.send(deleteObjectsCommand);
                    }
                }
                
                // Delete the bucket
                const deleteBucketCommand = new DeleteBucketCommand({
                    Bucket: this.bucket,
                });
                await this.s3.send(deleteBucketCommand);
                
                clearTimeout(timeoutId);
                safeCallback();
            } catch (err) {
                clearTimeout(timeoutId);
                // For cleanup, don't fail the test if bucket doesn't exist
                if (err.name === 'NoSuchBucket' || err.name === 'NotFound' ||
                    err.message?.includes('does not exist')) {
                    safeCallback();
                } else {
                    safeCallback(err);
                }
            }
        })();
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
                    Status: 'Enabled',
                }],
            },
        };
        
        (async () => {
            try {
                const command = new PutBucketReplicationCommand(params);
                await this.s3.send(command);
                cb();
            } catch (err) {
                cb(err);
            }
        })();
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

    afterEach(function afterEachHook(done) {
        // Add safety timeout
        this.timeout(35000);
        
        s3Helper.emptyAndDeleteBucket(() => {
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
