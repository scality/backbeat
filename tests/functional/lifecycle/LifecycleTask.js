const assert = require('assert');
const async = require('async');
const AWS = require('aws-sdk');

const Logger = require('werelogs').Logger;
const LifecycleRule = require('arsenal').models.LifecycleRule;

const ReplicationAPI =
      require('../../../extensions/replication/ReplicationAPI');
const LifecycleTask = require('../../../extensions/lifecycle/tasks/LifecycleTask');
const testConfig = require('../../config.json');
const { objectMD } = require('../utils/MetadataMock');

const S3 = AWS.S3;
const s3config = {
    endpoint: `http://${testConfig.s3.host}:${testConfig.s3.port}`,
    s3ForcePathStyle: true,
    credentials: new AWS.Credentials('accessKey1', 'verySecretKey1'),
};

const backbeatMetadataProxyMock = {
    headLocation: (headLocationParams, log, cb) => {
        cb(null, {
            lastModified: 'Thu, 28 Mar 2019 21:33:15 GMT',
        });
    },
    getMetadata: (params, log, cb) => {
        cb(null, {
            Body: JSON.stringify(objectMD.object1),
        });
    },
};

/*
    NOTE:
    When using CURRENT, running the lifecycle tasks will internally use
    `Date.now` to compare dates. This means that CURRENT date usage will most
    likely be in the past.
    To avoid flakiness, I will be setting the day of CURRENT back 2 day to
    avoid any flakiness.
    When comparing rules, if I set a rule that says "Days: 1", then any objects
    using CURRENT as LastModified should pass. To avoid flakiness, set a rule
    that says "Days: 3" to have any objects using CURRENT as LastModified to
    not pass.
    Also, when using rules that require Days/NoncurrentDays/DaysAfterInitiation,
    XML rule restrictions don't allow "0" day rules to be applied. So when
    testing, always use ENV `CI=true` and LifecycleTask will expire based
    on 1 Day rules. Applying 2+ Days should mean the object(s) should not
    expire.
*/
// current date
const CURRENT = new Date();
CURRENT.setDate(CURRENT.getDate() - 2);
// 5 days prior to currentDate
const PAST = new Date(CURRENT);
PAST.setDate(PAST.getDate() - 5);
// 5 days after currentDate
const FUTURE = new Date(CURRENT);
FUTURE.setDate(FUTURE.getDate() + 5);

const OWNER = 'testOwner';

class S3Helper {
    constructor(client) {
        this.s3 = client;
        this.bucket = undefined;

        this._scenario = [
            // 0. all unique object names, no pagination
            //    Useful for: non-versioned
            {
                keyNames: ['object-1', 'object-2', 'object-3'],
                tags: ['key1=value1', 'key1=value1&key2=value2', 'key2=value2'],

            },
            // 1. all unique object names, pagination, prefix
            //    Useful for: non-versioned
            {
                keyNames: [
                    'test/obj-1', 'obj-2', 'test/obj-3',
                    'atest/obj-4', 'testx/obj-5', 'test/obj-6',
                    'obj-7', 'atest/obj-8', 'an-obj-9',
                ],
                tags: [
                    'key1=value1', 'key1=value1', 'key1=value1&key2=value2',
                    'key2=value2', 'key2=value2', 'key1=value1&key3=value3',
                    'key3=value3', 'key3=value3', 'key4=value4',
                ],
            },
            // 2. same object names
            //    Useful for: versioned
            {
                keyNames: ['version-1', 'version-1', 'version-1'],
                tags: ['key1=value1', 'key2=value2', 'key1=value1&key2=value2'],
            },
            // 3. multiple object names, some duplicates, prefix
            //    Useful for: versioned
            {
                keyNames: [
                    'test/obj-1', 'test/obj-1', 'src/obj-2',
                    'tests/obj-1', 'test/obj-1', 'src/obj-2',
                    'src/obj-2', 'obj-3', 'test/obj-1',
                    'test/obj-1', 'test/obj-1', 'src/obj-2',
                    'test/obj-1', 'test/obj-1', 'obj-3',
                    'obj-4', 'obj-5', 'obj-6',
                ],
                tags: [
                    'key1=value1', 'key1=value1', 'key1=value1',
                    'key1=value1', 'key1=value1', 'key1=value1&key2=value2',
                    'key2=value2', 'key2=value2', 'key2=value2',
                    'key2=value2', 'key2=value2', 'key1=value1&key2=value2',
                    'key2=value2', 'key2=value2', 'key2=value2',
                    'key1=value1', 'key1=value1', 'key1=value1',
                ],
            },
        ];
    }

    getBucket() {
        return this.bucket;
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

    createObjects(scenarioNumber, cb) {
        async.forEachOf(this._scenario[scenarioNumber].keyNames,
        (key, i, done) => {
            this.s3.putObject({
                Body: '',
                Bucket: this.bucket,
                Key: key,
                Tagging: this._scenario[scenarioNumber].tags[i],
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

    createDeleteMarkers(scenarioNumber, cb) {
        this.setBucketVersioning('Enabled', err => {
            assert.ifError(err);

            return async.eachOfLimit(this._scenario[scenarioNumber].keyNames, 1,
            (key, i, done) => (
                async.series([
                    next => this.s3.putObject({
                        Body: '',
                        Bucket: this.bucket,
                        Key: key,
                        Tagging: this._scenario[scenarioNumber].tags[i],
                    }, next),
                    next => this.s3.deleteObject({
                        Bucket: this.bucket,
                        Key: key,
                    }, next),
                ], err => {
                    assert.ifError(err);
                    done();
                })
            ), err => {
                assert.ifError(err);
                cb();
            });
        });
    }

    /**
     * creates an mpu object and uploads parts
     * @param {number} scenarioNumber - scenario number
     * @param {function} cb - callback(err)
     * @return {undefined}
     */
    createMPU(scenarioNumber, cb) {
        const scenarioKeys = this._scenario[scenarioNumber].keyNames;
        async.timesSeries(scenarioKeys.length, (n, next) => {
            this.s3.createMultipartUpload({
                Bucket: this.bucket,
                Key: scenarioKeys[n],
            }, next);
        }, cb);
    }

    /**
     * Helper method to set bucket versioning
     * @param {string} status - 'Enabled' or 'Suspended'
     * @param {function} cb - callback(error, response)
     * @return {undefined}
     */
    setBucketVersioning(status, cb) {
        this.s3.putBucketVersioning({
            Bucket: this.bucket,
            VersioningConfiguration: {
                Status: status,
            },
        }, cb);
    }

    emptyAndDeleteBucket(cb) {
        // won't need to worry about 1k+ objects pagination
        async.waterfall([
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

    setBucketLifecycleConfigurations(rules, cb) {
        const lcParams = {
            Bucket: this.bucket,
            LifecycleConfiguration: {
                Rules: rules,
            },
        };
        return this.s3.putBucketLifecycleConfiguration(lcParams, cb);
    }

    setupEODM(bucket, key, cb) {
        async.waterfall([
            next => this.setAndCreateBucket(bucket, next),
            next => this.setBucketVersioning('Enabled', next),
            (data, next) => this.s3.putObject({
                Bucket: bucket,
                Key: key,
                Body: '',
            }, next),
            // first create delete marker
            (data, next) => this.s3.deleteObject({
                Bucket: bucket,
                Key: key,
            }, err => {
                if (err) {
                    return next(err);
                }
                return next(null, data.VersionId);
            }),
            // delete only version so we are left with just a delete marker
            (versionId, next) => this.s3.deleteObject({
                Bucket: bucket,
                Key: key,
                VersionId: versionId,
            }, next),
        ], cb);
    }
}

class ProducerMock {
    constructor() {
        this.reset();
    }

    reset() {
        this.sendCount = {
            bucket: 0,
            object: 0,
            transitions: 0,
        };
        this.entries = {
            bucket: [],
            object: [],
            transitions: [],
        };
    }

    sendToTopic(topicName, entries, cb) {
        const entry = JSON.parse(entries[0].message);
        if (topicName === 'bucket-tasks') {
            this.sendCount.bucket++;
            this.entries.bucket.push(entry);
        } else if (topicName === 'object-tasks') {
            this.sendCount.object++;
            this.entries.object.push(entry.target.key);
        } else if (topicName === 'data-mover') {
            this.sendCount.transitions++;
            this.entries.transitions.push(entry);
        }
        return process.nextTick(() => cb(null, entries.map(() => ({
            topic: topicName,
            partition: 1,
            offset: 2,
        }))));
    }

    getCount() {
        return this.sendCount;
    }

    getEntries() {
        return this.entries;
    }

    getKafkaProducer() {
        return null;
    }
}

class KafkaBacklogMetricsMock {
    constructor() {
        this.reset();
        this.producer = null;
    }

    setProducer(producer) {
        this.producer = producer;
    }

    reset() {
        this.sendCountAtLastSnapshot = null;
    }

    snapshotTopicOffsets(kafkaClient, topic, snapshotName, cb) {
        this.sendCountAtLastSnapshot = JSON.parse(
            JSON.stringify(this.producer.sendCount));
        return process.nextTick(cb);
    }
}

class LifecycleBucketProcessorMock {
    constructor() {
        this._log = new Logger(
            'LifecycleBucketProcessor:test:LifecycleBucketProcessorMock');

        // TODO: only added current working features
        this._lcConfig = {
            rules: {
                expiration: { enabled: true },
                noncurrentVersionExpiration: { enabled: true },
                abortIncompleteMultipartUpload: { enabled: true },
                transitions: { enabled: true },
                // below are features not implemented yet
                noncurrentVersionTransition: { enabled: false },
            },
        };

        this._producer = new ProducerMock();
        this._kafkaBacklogMetrics = new KafkaBacklogMetricsMock();
        this._kafkaBacklogMetrics.setProducer(this._producer);

        // set test topic name
        ReplicationAPI.setDataMoverTopic('data-mover');
    }

    getCount() {
        return this._producer.getCount();
    }

    getEntries() {
        return this._producer.getEntries();
    }

    getStateVars() {
        const { lifecycle } = testConfig.extensions;
        return {
            producer: this._producer,
            removeBucketFromQueued: () => {},
            enabledRules: this._lcConfig.rules,
            // Corresponds to the default endpoint in the cloudserver config.
            bootstrapList: [{ site: 'us-east-2', type: 'aws_s3' }],
            s3Endpoint: s3config.endpoint,
            s3Auth: lifecycle.auth,
            bucketTasksTopic: 'bucket-tasks',
            objectTasksTopic: 'object-tasks',
            kafkaBacklogMetrics: this._kafkaBacklogMetrics,
            log: this._log,
        };
    }

    reset() {
        this._producer.reset();
        this._kafkaBacklogMetrics.reset();
    }
}


/*
    To override MaxKeys or MaxUploads, set `testMaxKeys` or `testMaxUploads`
    respectively in params.
*/

function wrapProcessBucketEntry(bucketLCRules, bucketEntry,
s3mock, params, cb) {
    params.lcTask.processBucketEntry(bucketLCRules, bucketEntry,
    s3mock, backbeatMetadataProxyMock, err => {
        assert.ifError(err);
        const entries = params.lcp.getEntries();

        if (params.counter < entries.bucket.length) {
            /* eslint-disable no-param-reassign */
            return wrapProcessBucketEntry(bucketLCRules,
                entries.bucket[params.counter++],
                s3mock, params, cb);
            /* eslint-enable no-param-reassign */
        }
        // end of recursion
        // Processing of bucket data is performed in the background, hence we
        // cannot know when the messages will finish being pushed. Allow some
        // timeout for it to complete.
        const timeout = params.timeout || 0;
        return setTimeout(() => {
            const count = params.lcp.getCount();
            assert.deepStrictEqual(
                count, params.lcp._kafkaBacklogMetrics.sendCountAtLastSnapshot);
            cb(null, { count, entries });
        }, timeout);
    });
}

describe('lifecycle task functional tests', function dF() {
    this.timeout(10000);

    let lcp;
    let lcTask;
    let s3;
    let s3Helper;

    before(() => {
        lcp = new LifecycleBucketProcessorMock();
        s3 = new S3(s3config);
        lcTask = new LifecycleTask(lcp);
        lcTask.setSupportedRules([
            'expiration',
            'noncurrentVersionExpiration',
            'abortIncompleteMultipartUpload',
            'transitions',
        ]);
        s3Helper = new S3Helper(s3);
    });

    // Assert that the results from the bucket processor have the expected data.
    function assertTransitionResult(params) {
        const { data, expectedKeys } = params;
        const { count, entries } = data;
        assert.strictEqual(count.transitions, expectedKeys.length);
        assert.strictEqual(entries.transitions.length, expectedKeys.length);
        // TODO modify this test when ActionQueueEntry messages are
        // generated for transition purposes
        entries.transitions.sort(
            (t1, t2) => (t1.target.key < t2.target.key ? -1 : 1));
        expectedKeys.sort();
        assert.deepStrictEqual(
            entries.transitions.map(t => t.target.key),
            expectedKeys);
        entries.transitions.forEach(transition => {
            assert.strictEqual(transition.target.bucket, 'test-bucket');
            assert.strictEqual(transition.toLocation, 'us-east-2');
        });
    }

    // Perform the bucket setup as defined in the parameters, and call the
    // lifecycle task bucket processor method.
    function testTransition(params, cb) {
        const bucketName = 'test-bucket';
        const { rules, scenarioNumber, hasVersioning, expectedKeys } = params;
        async.waterfall([
            next => s3Helper.setAndCreateBucket(bucketName, next),
            next => {
                if (!hasVersioning) {
                    return next();
                }
                return s3Helper.setBucketVersioning('Enabled', err =>
                    next(err));
            },
            next => s3Helper.setBucketLifecycleConfigurations(rules, next),
            (data, next) => s3Helper.createObjects(scenarioNumber, next),
            next => s3.getBucketLifecycleConfiguration({
                Bucket: bucketName,
            }, next),
            (data, next) => {
                const entry = {
                    action: 'test-action',
                    target: {
                        bucket: bucketName,
                        owner: OWNER,
                    },
                    details: {},
                };
                const params = {
                    lcTask,
                    lcp,
                    counter: 0,
                    timeout: 1000,
                };
                wrapProcessBucketEntry(data.Rules, entry, s3, params, next);
            },
        ], (err, data) => {
            assert.ifError(err);
            assertTransitionResult({ data, expectedKeys });
            return cb();
        });
    }

    // Example lifecycle configs
    // {
    //     "Filter": {
    //         "And": {
    //             "Prefix": "myprefix",
    //             "Tags": [
    //                 {
    //                     "Value": "value1",
    //                     "Key": "tag1"
    //                 },
    //                 {
    //                     "Value": "value2",
    //                     "Key": "tag2"
    //                 }
    //             ]
    //         }
    //     },
    //     "Status": "Enabled",
    //     "Expiration": {
    //         "Days": 5
    //     },
    //     "ID": "rule1"
    // },

    describe('non-versioned bucket tests', () => {
        afterEach(done => {
            lcp.reset();

            s3Helper.emptyAndDeleteBucket(err => {
                assert.ifError(err);
                done();
            });
        });

        it('transition rules: no matching prefix', done => {
            const params = {
                scenarioNumber: 0,
                hasVersioning: false,
                rules: [
                    new LifecycleRule()
                        .addPrefix('test/')
                        .addTransitions([{
                            Days: 0,
                            StorageClass: 'us-east-2',
                        }])
                        .build(),
                ],
                expectedKeys: [],
            };
            testTransition(params, done);
        });

        it('transition rules: matching prefix', done => {
            const params = {
                scenarioNumber: 1,
                hasVersioning: false,
                rules: [
                    new LifecycleRule()
                        .addPrefix('test/')
                        .addTransitions([{
                            Days: 0,
                            StorageClass: 'us-east-2',
                        }])
                        .build(),
                ],
                expectedKeys: [
                    'test/obj-1',
                    'test/obj-3',
                    'test/obj-6',
                ],
            };
            testTransition(params, done);
        });

        it('transition rules: matching tag', done => {
            const params = {
                scenarioNumber: 1,
                hasVersioning: false,
                rules: [
                    new LifecycleRule()
                        .addTag('key1', 'value1')
                        .addTransitions([{
                            Days: 0,
                            StorageClass: 'us-east-2',
                        }])
                        .build(),
                ],
                expectedKeys: [
                    'test/obj-1',
                    'obj-2',
                    'test/obj-3',
                    'test/obj-6',
                ],
            };
            testTransition(params, done);
        });

        it('should verify changes in lifecycle rules will apply to the ' +
        'correct objects', done => {
            // kafka bucket entry
            const bucketEntry = {
                action: 'testing-nonversioned',
                target: {
                    bucket: 'test-bucket',
                    owner: OWNER,
                },
                details: {},
            };
            const params = {
                lcTask,
                lcp,
                counter: 0,
            };

            async.waterfall([
                next => s3Helper.setAndCreateBucket('test-bucket', next),
                next => s3Helper.setBucketLifecycleConfigurations([
                    new LifecycleRule().addExpiration('Date', FUTURE).build(),
                ], next),
                (data, next) => s3Helper.createObjects(0, next),
                next => s3.getBucketLifecycleConfiguration({
                    Bucket: 'test-bucket',
                }, next),
                (data, next) => {
                    wrapProcessBucketEntry(data.Rules, bucketEntry, s3, params,
                    (err, data) => {
                        assert.ifError(err);

                        assert.equal(data.count.bucket, 0);
                        assert.equal(data.count.object, 0);
                        assert.deepStrictEqual(data.entries.object, []);
                        next();
                    });
                },
                next => s3Helper.setBucketLifecycleConfigurations([
                    new LifecycleRule().addExpiration('Date', PAST).build(),
                ], next),
                (data, next) => {
                    s3.getBucketLifecycleConfiguration({
                        Bucket: 'test-bucket',
                    }, next);
                },
                (data, next) => {
                    lcp.reset();
                    params.counter = 0;

                    wrapProcessBucketEntry(data.Rules, bucketEntry, s3, params,
                    (err, data) => {
                        assert.ifError(err);

                        assert.equal(data.count.bucket, 0);
                        assert.equal(data.count.object, 3);

                        const expectedObjects = ['object-1', 'object-2',
                            'object-3'];
                        assert.deepStrictEqual(data.entries.object.sort(),
                            expectedObjects);
                        next();
                    });
                },
            ], err => {
                assert.ifError(err);
                done();
            });
        });

        [
            // expire: pagination, prefix
            {
                message: 'should verify that EXPIRED objects are sent to ' +
                    'object kafka topic with pagination, with prefix',
                bucketLCRules: [
                    new LifecycleRule().addID('task-1').addExpiration('Date', PAST)
                        .addPrefix('test/').build(),
                ],
                // S3Helper.buildScenario, chooses type of objects to create
                scenario: 1,
                bucketEntry: {
                    action: 'testing-prefix',
                    target: {
                        bucket: 'test-expire',
                        owner: OWNER,
                    },
                    details: {},
                },
                expected: {
                    objects: ['test/obj-1', 'test/obj-3', 'test/obj-6'],
                    bucketCount: 2,
                    objectCount: 3,
                },
            },
            // expire: basic test using Days
            {
                message: 'should verify the Expiration rule in Days ' +
                    'are properly handled',
                bucketLCRules: [
                    new LifecycleRule().addID('task-1').addExpiration('Days', 1)
                        .addPrefix('atest/').build(),
                ],
                scenario: 1,
                bucketEntry: {
                    action: 'testing-days-expiration',
                    target: {
                        bucket: 'test-expire',
                        owner: OWNER,
                    },
                    details: {},
                },
                expected: {
                    objects: ['atest/obj-4', 'atest/obj-8'],
                    bucketCount: 2,
                    objectCount: 2,
                },
            },
            // expire: pagination, tagging
            {
                message: 'should verify that EXPIRED objects are sent to ' +
                    'object kafka topic with pagination, with tags',
                bucketLCRules: [
                    new LifecycleRule().addID('task-1').addExpiration('Date', PAST)
                        .addTag('key1', 'value1').build(),
                ],
                scenario: 1,
                bucketEntry: {
                    action: 'testing-tagging',
                    target: {
                        bucket: 'test-expire',
                        owner: OWNER,
                    },
                    details: {},
                },
                expected: {
                    objects: ['test/obj-1', 'obj-2', 'test/obj-3',
                        'test/obj-6'],
                    bucketCount: 2,
                    objectCount: 4,
                },
            },
            // expire: multiple bucket rules, pagination, tagging, prefix
            {
                message: 'should verify that multiple EXPIRED rules are ' +
                    'properly being handled',
                bucketLCRules: [
                    new LifecycleRule().addID('task-1').addExpiration('Date', FUTURE)
                        .build(),
                    new LifecycleRule().addID('task-2').addExpiration('Date', FUTURE)
                        .addTag('key1', 'value1').build(),
                    new LifecycleRule().addID('task-3').addExpiration('Date', PAST)
                        .addPrefix('test/').addTag('key3', 'value3').build(),
                    new LifecycleRule().addID('task-4').disable()
                        .addExpiration('Date', PAST).build(),
                    new LifecycleRule().addID('task-5').addExpiration('Date', PAST)
                        .addTag('key4', 'value4').build(),
                ],
                scenario: 1,
                bucketEntry: {
                    action: 'testing-multiple',
                    target: {
                        bucket: 'test-expire',
                        owner: OWNER,
                    },
                    details: {},
                },
                expected: {
                    objects: ['test/obj-6', 'an-obj-9'],
                    bucketCount: 2,
                    objectCount: 2,
                },
            },
        ].forEach(item => {
            it(item.message, done => {
                const params = {
                    lcTask,
                    lcp,
                    counter: 0,
                };
                const bucket = item.bucketEntry.target.bucket;
                async.waterfall([
                    next => s3Helper.setAndCreateBucket(bucket, next),
                    next => s3Helper.setBucketLifecycleConfigurations(
                        item.bucketLCRules, next),
                    (data, next) => s3Helper.createObjects(item.scenario, next),
                    next => s3.getBucketLifecycleConfiguration({
                        Bucket: bucket,
                    }, next),
                    (data, next) => {
                        wrapProcessBucketEntry(data.Rules, item.bucketEntry, s3,
                        params, (err, data) => {
                            assert.ifError(err);

                            assert.equal(data.count.bucket,
                                item.expected.bucketCount);
                            assert.equal(data.count.object,
                                item.expected.objectCount);
                            assert.deepStrictEqual(data.entries.object.sort(),
                                item.expected.objects.sort());
                            next();
                        });
                    },
                ], err => {
                    assert.ifError(err);
                    done();
                });
            });
        });
    }); // end non-versioned describe block

    describe('versioned bucket tests', () => {
        afterEach(done => {
            lcp.reset();

            s3Helper.emptyAndDeleteBucket(err => {
                assert.ifError(err);
                done();
            });
        });

        it('transition rules: no matching prefix', done => {
            const params = {
                scenarioNumber: 2,
                hasVersioning: true,
                rules: [
                    new LifecycleRule()
                        .addPrefix('test/')
                        .addTransitions([{
                            Days: 0,
                            StorageClass: 'us-east-2',
                        }])
                        .build(),
                ],
                expectedKeys: [],
            };
            testTransition(params, done);
        });

        it('transition rules: matching prefix', done => {
            const params = {
                scenarioNumber: 2,
                hasVersioning: true,
                rules: [
                    new LifecycleRule()
                        .addPrefix('version-1')
                        .addTransitions([{
                            Days: 0,
                            StorageClass: 'us-east-2',
                        }])
                        .build(),
                ],
                expectedKeys: ['version-1'],
            };
            testTransition(params, done);
        });

        it('transition rules: matching tag', done => {
            const params = {
                scenarioNumber: 2,
                hasVersioning: false,
                rules: [
                    new LifecycleRule()
                        .addTag('key1', 'value1')
                        .addTransitions([{
                            Days: 0,
                            StorageClass: 'us-east-2',
                        }])
                        .build(),
                ],
                expectedKeys: ['version-1'],
            };
            testTransition(params, done);
        });

        it('should verify changes in lifecycle rules will apply to ' +
        'the correct objects', done => {
            const bucketEntry = {
                action: 'testing-versioned',
                target: {
                    bucket: 'test-bucket',
                    owner: OWNER,
                },
                details: {},
            };
            const params = {
                lcTask,
                lcp,
                counter: 0,
            };

            async.waterfall([
                next => s3Helper.setAndCreateBucket('test-bucket', next),
                next => s3Helper.setBucketLifecycleConfigurations([
                    new LifecycleRule().addID('task-1').addNCVExpiration(2).build(),
                ], next),
                (data, next) => s3Helper.createVersions(2, next),
                next => s3.getBucketLifecycleConfiguration({
                    Bucket: 'test-bucket',
                }, next),
                (data, next) => {
                    // Should not expire anything
                    wrapProcessBucketEntry(data.Rules, bucketEntry, s3,
                    params, (err, data) => {
                        assert.ifError(err);

                        assert.equal(data.count.bucket, 0);
                        assert.equal(data.count.object, 0);
                        assert.deepStrictEqual(data.entries.object, []);
                        next();
                    });
                },
                next => s3Helper.setBucketLifecycleConfigurations([
                    new LifecycleRule().addNCVExpiration(1).build(),
                ], next),
                (data, next) => s3.getBucketLifecycleConfiguration({
                    Bucket: 'test-bucket',
                }, next),
                (data, next) => {
                    lcp.reset();
                    params.counter = 0;

                    // should now expire all versions
                    wrapProcessBucketEntry(data.Rules, bucketEntry, s3, params,
                    (err, data) => {
                        assert.ifError(err);

                        assert.equal(data.count.bucket, 0);
                        // one version `IsLatest`, so only 2 objects expire
                        assert.equal(data.count.object, 2);
                        assert.deepStrictEqual(data.entries.object.sort(),
                            Array(2).fill('version-1'));

                        next();
                    });
                },
            ], err => {
                assert.ifError(err);

                done();
            });
        });

        it('should expire a version in a versioning enabled bucket using ' +
        'basic expiration rule', done => {
            const bucket = 'test-bucket';
            const keyName = 'test-key1';
            const bucketEntry = {
                action: 'testing-islatest',
                target: {
                    bucket,
                    owner: OWNER,
                },
                details: {},
            };
            const params = {
                lcTask,
                lcp,
                counter: 0,
            };
            async.waterfall([
                next => s3Helper.setAndCreateBucket(bucket, next),
                next => s3Helper.setBucketLifecycleConfigurations([
                    new LifecycleRule().addID('task-1')
                        .addExpiration('Date', PAST)
                        .build(),
                ], next),
                (data, next) => s3Helper.setBucketVersioning('Enabled', next),
                (data, next) => s3.putObject({
                    Bucket: bucket,
                    Key: keyName,
                    Body: '',
                }, next),
                (data, next) => s3.getBucketLifecycleConfiguration({
                    Bucket: bucket,
                }, next),
                (data, next) => {
                    wrapProcessBucketEntry(data.Rules, bucketEntry, s3, params,
                    (err, data) => {
                        assert.ifError(err);

                        assert.equal(data.count.object, 1);
                        next();
                    });
                },
            ], err => {
                assert.ifError(err);
                done();
            });
        });

        it('should NOT expire a delete marker in a versioning enabled bucket ' +
        'where there are at least 1 or more non-current versions', done => {
            const bucket = 'test-bucket';
            const bucketEntry = {
                action: 'testing-islatest',
                target: {
                    bucket,
                    owner: OWNER,
                },
                details: {},
            };
            const params = {
                lcTask,
                lcp,
                counter: 0,
            };
            async.waterfall([
                next => s3Helper.setAndCreateBucket(bucket, next),
                next => s3Helper.setBucketLifecycleConfigurations([
                    new LifecycleRule().addID('task-1').addExpiration('Date', PAST)
                        .build(),
                    new LifecycleRule().addID('task-2')
                        .addExpiration('ExpiredObjectDeleteMarker', true)
                        .build(),
                ], next),
                (data, next) => s3Helper.setBucketVersioning('Enabled', next),
                (data, next) => s3Helper.createDeleteMarkers(2, next),
                next => s3.getBucketLifecycleConfiguration({
                    Bucket: bucket,
                }, next),
                (data, next) => {
                    wrapProcessBucketEntry(data.Rules, bucketEntry, s3, params,
                    (err, data) => {
                        assert.ifError(err);

                        assert.equal(data.count.object, 0);
                        next();
                    });
                },
            ], err => {
                assert.ifError(err);
                done();
            });
        });

        // 1 Day Expiration rule - handling the IsLatest versions
        [
            {
                message: 'should expire a version in a versioning enabled ' +
                    'bucket with 0 non-current versions using basic ' +
                    'expiration rule',
                isDeleteMarker: false,
                hasNonCurrentVersions: false,
                versionStatus: 'Enabled',
                expected: {
                    objectCount: 1,
                },
            },
            {
                message: 'should expire a version in a versioning suspended ' +
                    'bucket with 0 non-current versions using basic ' +
                    'expiration rule',
                isDeleteMarker: false,
                hasNonCurrentVersions: false,
                versionStatus: 'Suspended',
                expected: {
                    objectCount: 1,
                },
            },
            {
                message: 'should expire a delete marker in a versioning ' +
                    'enabled bucket with 0 non-current versions using basic ' +
                    'expiration rule',
                isDeleteMarker: true,
                hasNonCurrentVersions: false,
                versionStatus: 'Enabled',
                expected: {
                    objectCount: 1,
                },
            },
            {
                message: 'should expire a delete marker in a versioning ' +
                    'suspended bucket with 0 non-current versions using ' +
                    'basic expiration rule',
                isDeleteMarker: true,
                hasNonCurrentVersions: false,
                versionStatus: 'Suspended',
                expected: {
                    objectCount: 1,
                },
            },
            {
                message: 'should expire a version in a versioning enabled ' +
                    'bucket with 1 or more non-current versions using basic ' +
                    'expiration rule',
                isDeleteMarker: false,
                hasNonCurrentVersions: true,
                versionStatus: 'Enabled',
                expected: {
                    objectCount: 1,
                },
            },
            {
                message: 'should expire a version in a versioning suspended ' +
                    'bucket with 1 or more non-current versions using basic ' +
                    'expiration rule',
                isDeleteMarker: false,
                hasNonCurrentVersions: true,
                versionStatus: 'Suspended',
                expected: {
                    objectCount: 1,
                },
            },
            {
                message: 'should NOT expire a delete marker in a versioning ' +
                    'enabled bucket with 1 or more non-current versions ' +
                    'using basic expiration rule',
                isDeleteMarker: true,
                hasNonCurrentVersions: true,
                versionStatus: 'Enabled',
                expected: {
                    objectCount: 0,
                },
            },
            {
                message: 'should NOT expire a delete marker in a versioning ' +
                    'suspended bucket with 1 or more non-current versions ' +
                    'using basic expiration rule',
                isDeleteMarker: true,
                hasNonCurrentVersions: true,
                versionStatus: 'Suspended',
                expected: {
                    objectCount: 0,
                },
            },
        ].forEach(item => {
            it(item.message, done => {
                const Bucket = 'test-bucket';
                const Key = 'test-key1';
                const bucketEntry = {
                    action: 'testing-islatest',
                    target: {
                        bucket: Bucket,
                        owner: OWNER,
                    },
                    details: {},
                };
                const params = {
                    lcTask,
                    lcp,
                    counter: 0,
                };

                async.waterfall([
                    next => s3Helper.setAndCreateBucket(Bucket, next),
                    next => s3Helper.setBucketVersioning('Enabled', next),
                    (data, next) => s3.putObject({ Bucket, Key, Body: '' },
                        next),
                    (data, next) => {
                        if (item.isDeleteMarker) {
                            return async.series([
                                cb => s3.deleteObject({ Bucket, Key },
                                    err => {
                                        if (err) {
                                            return cb(err);
                                        }
                                        return cb();
                                    }),
                                cb => {
                                    if (!item.hasNonCurrentVersions) {
                                        return s3.deleteObject({
                                            Bucket, Key,
                                            VersionId: data.VersionId,
                                        }, cb);
                                    }
                                    return cb();
                                },
                            ], next);
                        }
                        if (item.hasNonCurrentVersions) {
                            return s3.putObject({ Bucket, Key, Body: '' },
                                next);
                        }
                        return next(null, null);
                    },
                    (data, next) => s3Helper.setBucketVersioning(
                        item.versionStatus, next),
                    (data, next) => {
                        s3Helper.setBucketLifecycleConfigurations([
                            new LifecycleRule().addID('task-1')
                                .addExpiration('Date', PAST).build(),
                        ], next);
                    },
                    (data, next) => s3.getBucketLifecycleConfiguration(
                        { Bucket }, next),
                    (data, next) => {
                        wrapProcessBucketEntry(data.Rules, bucketEntry, s3,
                        params, (err, data) => {
                            assert.ifError(err);
                            assert.equal(data.count.object,
                                item.expected.objectCount);
                            next();
                        });
                    },
                ], err => {
                    assert.ifError(err);
                    done();
                });
            });
        });

        [
            {
                message: 'should apply ExpiredObjectDeleteMarker rule on ' +
                    'only a delete marker in a versioning enabled bucket ' +
                    'with zero non-current versions',
                bucketLCRules: [
                    new LifecycleRule().addID('task-1')
                        .addExpiration('ExpiredObjectDeleteMarker', true)
                        .build(),
                ],
                owner: OWNER,
                expected: {
                    objectCount: 1,
                },
            },
            {
                message: 'should not apply ExpiredObjectDeleteMarker rule ' +
                    'when EODM is set to false',
                bucketLCRules: [
                    new LifecycleRule().addID('task-1')
                        .addExpiration('ExpiredObjectDeleteMarker', false)
                        .build(),
                ],
                owner: OWNER,
                expected: {
                    objectCount: 0,
                },
            },
            {
                message: 'should not remove an expired object delete marker ' +
                    'when the ExpiredObjectDeleteMarker rule is set to false',
                bucketLCRules: [
                    new LifecycleRule().addID('task-1')
                        .addExpiration('ExpiredObjectDeleteMarker', false)
                        .build(),
                    new LifecycleRule().addID('task-2')
                        .addExpiration('Days', 1).build(),
                ],
                owner: OWNER,
                expected: {
                    objectCount: 1,
                },
            },
        ].forEach(item => {
            it(item.message, done => {
                const bucket = 'test-bucket';
                const keyName = 'test-key';
                const bucketEntry = {
                    action: 'testing-eodm',
                    target: {
                        bucket,
                        owner: item.owner,
                    },
                    details: {},
                };
                const params = {
                    lcTask,
                    lcp,
                    counter: 0,
                };
                async.waterfall([
                    next => s3Helper.setupEODM(bucket, keyName, next),
                    (data, next) => s3Helper.setBucketLifecycleConfigurations(
                        item.bucketLCRules, next),
                    (data, next) => s3.getBucketLifecycleConfiguration({
                        Bucket: bucket,
                    }, next),
                    (data, next) => {
                        wrapProcessBucketEntry(data.Rules, bucketEntry, s3,
                        params, (err, data) => {
                            assert.ifError(err);

                            assert.equal(data.count.object,
                                item.expected.objectCount);
                            next();
                        });
                    },
                ], err => {
                    assert.ifError(err);
                    done();
                });
            });
        });

        [
            // ncve: basic 1 day rule should expire, no pagination
            {
                message: 'should verify that NoncurrentVersionExpiration rule' +
                    ' applies to each versioned object, no pagination',
                bucketLCRules: [
                    new LifecycleRule().addID('task-1').addNCVExpiration(1).build(),
                ],
                scenarioFxn: 'createVersions',
                scenario: 2,
                bucketEntry: {
                    action: 'testing-ncve',
                    target: {
                        bucket: 'test-ncve',
                        owner: OWNER,
                    },
                    details: {},
                },
                expected: {
                    objects: ['version-1', 'version-1'],
                    bucketCount: 0,
                    objectCount: 2,
                },
            },
            // ncve: pagination, prefix, should expire some
            {
                message: 'should verify that NoncurrentVersionExpiration rule' +
                    ' applies to correct versions with pagination and prefix',
                bucketLCRules: [
                    new LifecycleRule().addID('task-1').addNCVExpiration(3).build(),
                    new LifecycleRule().addID('task-2').addPrefix('test/')
                        .addNCVExpiration(1).build(),
                    new LifecycleRule().addID('task-3').addPrefix('src/')
                        .addNCVExpiration(2).build(),
                ],
                scenarioFxn: 'createVersions',
                scenario: 3,
                bucketEntry: {
                    action: 'testing-ncve',
                    target: {
                        bucket: 'test-ncve',
                        owner: OWNER,
                    },
                    details: {},
                },
                expected: {
                    objects: Array(7).fill('test/obj-1'),
                    bucketCount: 5,
                    objectCount: 7,
                },
            },
            // ncve: pagination, tagging, should expire some
            {
                message: 'should verify that NoncurrentVersionExpiration rule' +
                    ' applies to correct versions with tagging and pagination',
                bucketLCRules: [
                    new LifecycleRule().addID('task-1').addTag('key1', 'value1')
                        .addPrefix('src/').addNCVExpiration(1).build(),
                    new LifecycleRule().addID('task-2').addTag('key2', 'value2')
                        .addNCVExpiration(2).build(),
                ],
                scenarioFxn: 'createVersions',
                scenario: 3,
                bucketEntry: {
                    action: 'testing-ncve',
                    target: {
                        bucket: 'test-ncve',
                        owner: OWNER,
                    },
                    details: {},
                },
                expected: {
                    objects: Array(2).fill('src/obj-2'),
                    bucketCount: 5,
                    objectCount: 2,
                },
            },
            // ncve: pagination, delete markers, expire all versions
            {
                message: 'should verify that NoncurrentVersionExpiration rule' +
                    ' applies to delete markers as well with pagination',
                bucketLCRules: [
                    new LifecycleRule().addID('task-1').addNCVExpiration(1).build(),
                ],
                scenarioFxn: 'createDeleteMarkers',
                scenario: 2,
                bucketEntry: {
                    action: 'testing-ncve',
                    target: {
                        bucket: 'test-ncve-deletemarkers',
                        owner: OWNER,
                    },
                    details: {},
                },
                expected: {
                    objects: Array(5).fill('version-1'),
                    bucketCount: 1,
                    objectCount: 5,
                },
            },
        ].forEach(item => {
            it(item.message, done => {
                const params = {
                    lcTask,
                    lcp,
                    counter: 0,
                    queuedEntries: [],
                };
                const bucket = item.bucketEntry.target.bucket;
                async.waterfall([
                    next => s3Helper.setAndCreateBucket(bucket, next),
                    next => s3Helper.setBucketLifecycleConfigurations(
                        item.bucketLCRules, next),
                    (data, next) => s3Helper[item.scenarioFxn](item.scenario,
                        next),
                    next => s3.getBucketLifecycleConfiguration({
                        Bucket: bucket,
                    }, next),
                    (data, next) => {
                        wrapProcessBucketEntry(data.Rules, item.bucketEntry, s3,
                        params, (err, data) => {
                            assert.ifError(err);

                            assert.equal(data.count.bucket,
                                item.expected.bucketCount);
                            assert.equal(data.count.object,
                                item.expected.objectCount);
                            assert.deepStrictEqual(data.entries.object.sort(),
                                item.expected.objects.sort());

                            next();
                        });
                    },
                ], err => {
                    assert.ifError(err);
                    done();
                });
            });
        });
    }); // end versioned describe block

    describe('incomplete mpu objects', () => {
        const bucketName = 'test-mpu-bucket';

        before(done => {
            s3Helper.setAndCreateBucket(bucketName, done);
        });

        afterEach(done => {
            lcp.reset();

            // cleanup existing mpu (if any)
            s3.listMultipartUploads({ Bucket: bucketName }, (err, data) => {
                assert.ifError(err);
                async.eachLimit(data.Uploads, 1, (upload, next) => {
                    s3.abortMultipartUpload({
                        Bucket: bucketName,
                        Key: upload.Key,
                        UploadId: upload.UploadId,
                    }, next);
                }, err => {
                    assert.ifError(err);

                    done();
                });
            });
        });

        after(done => {
            s3.deleteBucket({ Bucket: bucketName }, done);
        });

        it('should verify changes in lifecycle rules will apply', done => {
            const bucketEntry = {
                action: 'testing-abortmpu',
                target: {
                    bucket: bucketName,
                    owner: OWNER,
                },
                details: {},
            };
            const params = {
                lcTask,
                lcp,
                counter: 0,
            };

            async.waterfall([
                next => s3Helper.setBucketLifecycleConfigurations([
                    new LifecycleRule().addAbortMPU(2).build(),
                ], next),
                (data, next) => s3Helper.createMPU(0, next),
                (data, next) => s3.getBucketLifecycleConfiguration({
                    Bucket: bucketName,
                }, next),
                (data, next) => {
                    // should not expire anything
                    wrapProcessBucketEntry(data.Rules, bucketEntry, s3,
                    params, (err, data) => {
                        assert.ifError(err);

                        assert.equal(data.count.bucket, 0);
                        assert.equal(data.count.object, 0);
                        assert.deepStrictEqual(data.entries.object, []);
                        next();
                    });
                },
                next => s3Helper.setBucketLifecycleConfigurations([
                    new LifecycleRule().addAbortMPU(1).build(),
                ], next),
                (data, next) => s3.getBucketLifecycleConfiguration({
                    Bucket: bucketName,
                }, next),
                (data, next) => {
                    lcp.reset();
                    params.counter = 0;

                    // should abort
                    wrapProcessBucketEntry(data.Rules, bucketEntry, s3,
                    params, (err, data) => {
                        assert.ifError(err);

                        assert.equal(data.count.bucket, 0);
                        assert.equal(data.count.object, 3);

                        const expected = ['object-1', 'object-2', 'object-3'];
                        assert.deepStrictEqual(data.entries.object.sort(),
                            expected);
                        next();
                    });
                },
            ], err => {
                assert.ifError(err);
                done();
            });
        });

        it('should verify that AbortIncompleteMultipartUpload rule applies ' +
        'to correct objects with pagination and prefix', done => {
            const bucketEntry = {
                action: 'testing-abortmpu',
                target: {
                    bucket: bucketName,
                    owner: OWNER,
                },
                details: {},
            };
            const params = {
                lcTask,
                lcp,
                counter: 0,
            };

            async.waterfall([
                next => s3Helper.setBucketLifecycleConfigurations([
                    new LifecycleRule().addID('rule-1').addPrefix('test/').addAbortMPU(1)
                        .build(),
                    new LifecycleRule().addID('rule-2').addPrefix('obj-').addAbortMPU(2)
                        .build(),
                ], next),
                (data, next) => s3Helper.createMPU(1, next),
                (data, next) => s3.getBucketLifecycleConfiguration({
                    Bucket: bucketName,
                }, next),
                (data, next) => {
                    wrapProcessBucketEntry(data.Rules, bucketEntry, s3,
                    params, (err, data) => {
                        assert.ifError(err);

                        assert.equal(data.count.bucket, 2);
                        assert.equal(data.count.object, 3);

                        const expected = ['test/obj-1', 'test/obj-3',
                            'test/obj-6'];
                        assert.deepStrictEqual(data.entries.object.sort(),
                            expected);
                        next();
                    });
                },
            ], err => {
                assert.ifError(err);
                done();
            });
        });
    }); // end incomplete mpu objects block
});
