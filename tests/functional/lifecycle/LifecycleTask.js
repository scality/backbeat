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
            // 0. create objects: non-versioned, no pagination
            {
                keyNames: ['object-1', 'object-2', 'object-3'],
                tags: ['key1=value1', 'key1=value1&key2=value2', 'key2=value2'],

            },
            // 1. create objects: non-versioned, pagination, prefix
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

    createVersions() {
        // first make bucket versioned
        // then add objects of same name and maybe different name
    }

    emptyAndDeleteBucket(cb) {
        // won't need to worry about 1k+ objects pagination
        async.waterfall([
            next => this.s3.getBucketVersioning({ Bucket: this.bucket }, next),
            (data, next) => {
                if (data.Status === 'Enabled' || data.Status === 'Suspended') {
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

                    return this.s3.deleteObjects({
                        Bucket: this.bucket,
                        Delete: { Objects: list },
                    }, next);
                }
                return process.nextTick(() => next(null));
            },
            next => this.s3.listObjects({ Bucket: this.bucket }, next),
            (data, next) => {
                const list = data.Contents.map(c => ({ Key: c.Key }));

                return this.s3.deleteObjects({
                    Bucket: this.bucket,
                    Delete: { Objects: list },
                }, next);
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

    /**
     * build bucket configs and bucket objects based on a fixed scenario
     * @param {array} rules - array of rules to add to bucket lifecycle configs
     * @param {number} scenarioNumber - scenario to build
     * @param {function} cb - callback(err)
     * @return {undefined}
     */
    buildScenario(rules, scenarioNumber, cb) {
        async.waterfall([
            next => this.setBucketLifecycleConfigurations(rules, next),
            (data, next) => {
                if (!this._scenario[scenarioNumber]) {
                    return next(new Error('incorrect scenario number'));
                }
                return this.createObjects(scenarioNumber, next);
            },
        ], cb);
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
        // eslint-disable-next-line
        params.counter++;
        const entries = params.lcp.getEntries();

        if (params.counter === entries.bucket.length) {
            return wrapProcessBucketEntry(bucketLCRules,
                entries.bucket[entries.bucket.length - 1],
                s3mock, params, cb);
        }
        // end of recursion
        return cb(null, { count: params.lcp.getCount(), entries });
    });
}

describe('lifecycle producer functional', () => {
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

        it.only('should verify changes in lifecycle rules will apply to the ' +
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
                next => s3Helper.buildScenario([
                    new Rule().addExpiration('Date', FUTURE).build(),
                ], 0, next),
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
                        assert.deepStrictEqual(data.entries.object,
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
                    next => s3Helper.buildScenario(item.bucketLCRules,
                        item.scenario, next),
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
});
