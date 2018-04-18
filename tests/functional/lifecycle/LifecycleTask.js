const assert = require('assert');
const async = require('async');
const AWS = require('aws-sdk');

const Logger = require('werelogs').Logger;

const LifecycleTask = require('../../../extensions/lifecycle' +
    '/tasks/LifecycleTask');
const Rule = require('../../utils/Rule');
const testConfig = require('../../config.json');

const S3 = AWS.S3;
const s3config = {
    endpoint: `${testConfig.s3.transport}://` +
        `${testConfig.s3.host}:${testConfig.s3.port}`,
    s3ForcePathStyle: true,
    credentials: new AWS.Credentials(testConfig.s3.accessKey,
                                     testConfig.s3.secretKey),
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
    testing, always use ENV `TEST_SWITCH` and LifecycleTask will expire based
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
}

class LifecycleProducerMock {
    constructor() {
        this._log = new Logger('LifecycleProducer:test:LifecycleProducerMock');

        // TODO: only added current working features
        this._lcConfig = {
            rules: {
                expiration: { enabled: true },
                noncurrentVersionExpiration: { enabled: true },
                abortIncompleteMultipartUpload: { enabled: true },
                // below are features not implemented yet
                transition: { enabled: false },
                noncurrentVersionTransition: { enabled: false },
            },
        };

        this.sendCount = {
            bucket: 0,
            object: 0,
        };
        this.entries = {
            bucket: [],
            object: [],
        };
    }

    sendBucketEntry(entry, cb) {
        this.sendCount.bucket++;
        this.entries.bucket.push(entry);

        cb();
    }

    sendObjectEntry(entry, cb) {
        this.sendCount.object++;
        this.entries.object.push(entry.target.key);

        cb();
    }

    getCount() {
        return this.sendCount;
    }

    getEntries() {
        return this.entries;
    }

    getStateVars() {
        return {
            sendBucketEntry: this.sendBucketEntry.bind(this),
            sendObjectEntry: this.sendObjectEntry.bind(this),
            removeBucketFromQueued: () => {},
            enabledRules: this._lcConfig.rules,
            log: this._log,
        };
    }

    reset() {
        this.sendCount = {
            bucket: 0,
            object: 0,
        };
        this.entries = {
            bucket: [],
            object: [],
        };
        this.sendBucketEntry = null;
        this.sendObjectEntry = null;
    }
}


/*
    To override MaxKeys or MaxUploads, set `testMaxKeys` or `testMaxUploads`
    respectively in params.
*/

function wrapProcessBucketEntry(bucketLCRules, bucketEntry,
s3mock, params, cb) {
    params.lcTask.processBucketEntry(bucketLCRules, bucketEntry,
    s3mock, err => {
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
        return cb(null, { count: params.lcp.getCount(), entries });
    });
}

describe('lifecycle task functional tests', () => {
    let lcp;
    let lcTask;
    let s3;
    let s3Helper;

    before(() => {
        lcp = new LifecycleProducerMock();
        s3 = new S3(s3config);
        lcTask = new LifecycleTask(lcp);
        s3Helper = new S3Helper(s3);
    });

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
                    new Rule().addExpiration('Date', FUTURE).build(),
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
                    new Rule().addExpiration('Date', PAST).build(),
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
                    new Rule().addID('task-1').addExpiration('Date', PAST)
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
            // expire: pagination, tagging
            {
                message: 'should verify that EXPIRED objects are sent to ' +
                    'object kafka topic with pagination, with tags',
                bucketLCRules: [
                    new Rule().addID('task-1').addExpiration('Date', PAST)
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
                    new Rule().addID('task-1').addExpiration('Date', FUTURE)
                        .build(),
                    new Rule().addID('task-2').addExpiration('Date', FUTURE)
                        .addTag('key1', 'value1').build(),
                    new Rule().addID('task-3').addExpiration('Date', PAST)
                        .addPrefix('test/').addTag('key3', 'value3').build(),
                    new Rule().addID('task-4').disable()
                        .addExpiration('Date', PAST).build(),
                    new Rule().addID('task-5').addExpiration('Date', PAST)
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
                    new Rule().addID('task-1').addNCVExpiration(2).build(),
                ], next),
                (data, next) => s3Helper.createVersions(2, next),
                next => s3.getBucketLifecycleConfiguration({
                    Bucket: 'test-bucket',
                }, next),
                (data, next) => {
                    // Should not expire anything but paginates once
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
                    new Rule().addNCVExpiration(1).build(),
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

        [
            // ncve: basic 1 day rule should expire, no pagination
            {
                message: 'should verify that NoncurrentVersionExpiration rule' +
                    ' applies to each versioned object, no pagination',
                bucketLCRules: [
                    new Rule().addID('task-1').addNCVExpiration(1).build(),
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
                    new Rule().addID('task-1').addNCVExpiration(3).build(),
                    new Rule().addID('task-2').addPrefix('test/')
                        .addNCVExpiration(1).build(),
                    new Rule().addID('task-3').addPrefix('src/')
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
                    new Rule().addID('task-1').addTag('key1', 'value1')
                        .addPrefix('src/').addNCVExpiration(1).build(),
                    new Rule().addID('task-2').addTag('key2', 'value2')
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
                    new Rule().addID('task-1').addNCVExpiration(1).build(),
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
});
