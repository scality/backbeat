'use strict';

const assert = require('assert');
const sinon = require('sinon');
const fakeLogger = require('../../utils/fakeLogger');
const { splitter } = require('arsenal').constants;

const LifecycleConductor = require(
    '../../../extensions/lifecycle/conductor/LifecycleConductor');
const {
    lifecycleTaskVersions,
    indexesForFeature,
    masterKeyPrefix,
    masterKeyPrefixEnd,
} = require('../../../lib/constants');

const {
    zkConfig,
    kafkaConfig,
    lcConfig,
    repConfig,
    s3Config,
} = require('../../functional/lifecycle/configObjects');
const { BackbeatMetadataProxyMock } = require('../mocks');

function getTask(isLifecycled = true) {
    return {
        bucketName: 'testbucket',
        canonicalId: 'testid',
        isLifecycled,
    };
}

const accountName1 = 'account1';
const account1 = 'ab288756448dc58f61482903131e7ae533553d20b52b0e2ef80235599a1b9143';
const account2 = 'cd288756448dc58f61482903131e7ae533553d20b52b0e2ef80235599a1b9144';
const bucket1 = 'bucket1';
const bucket2 = 'bucket2';

class Queue {
    constructor() {
        this.tasks = [];
    }

    push(task) {
        // task can be either an object or an array of object.
        if (Array.isArray(task)) {
            this.tasks.push(...task);
        } else {
            this.tasks.push(task);
        }
    }

    list() {
        return this.tasks;
    }

    length() {
        return this.tasks.length;
    }

    clean() {
        this.tasks = [];
    }
}

function makeLifecycleConductorWithFilters(options, markers) {
    const { bucketsDenied, accountsDenied, bucketSource } = options;
    const lifecycleConfig = {
        ...lcConfig,
        zookeeperPath: '/test/lifecycle',
        conductor: {
            bucketSource,
            filter: {
                deny: {
                    buckets: bucketsDenied,
                    accounts: accountsDenied,
                },
            },
        },
    };

    const lcConductor = new LifecycleConductor(zkConfig.zookeeper,
        kafkaConfig, lifecycleConfig, repConfig, s3Config);

    lcConductor._zkClient = {
        getChildren: (a, b, cb) => {
            cb(null, [
                `${account1}:bucketuid123:${bucket1}`,
                `${account2}:bucketuid456:${bucket2}`,
            ]);
        },
        getData: (a, cb) => cb(null, null, null),
        setData: (a, b, c, cb) => cb(null, {}),
    };

    lcConductor._bucketClient = {
        listObject: (a, b, { marker }, cb) => {
            if (marker) {
                markers.push(marker);
                return cb(null, JSON.stringify({
                    Contents: [
                        {
                            key: `${account2}..|..${bucket2}`,
                        },
                    ],
                }));
            }
            return cb(null, JSON.stringify({
                Contents: [
                    {
                        key: `${account1}..|..${bucket1}`,
                    },
                ],
                IsTruncated: true,
            }));
        },
    };

    return lcConductor;
}

describe('indexesForFeature.lifecycle.v2', () => {
    it('should restrict both indexes to master entries via partialFilterExpression', () => {
        const expected = { _id: { $gte: masterKeyPrefix, $lt: masterKeyPrefixEnd } };
        const indexes = indexesForFeature.lifecycle.v2;
        assert.strictEqual(indexes.length, 2);
        for (const idx of indexes) {
            assert.deepStrictEqual(idx.partialFilterExpression, expected,
                `${idx.name} should have partialFilterExpression restricting to masters`);
        }
    });

    it('should derive master key range from arsenal DbPrefixes', () => {
        assert.strictEqual(masterKeyPrefix, '\x7fM');
        assert.strictEqual(masterKeyPrefixEnd, '\x7fN');
    });
});

describe('Lifecycle Conductor', () => {
    let conductor;
    let lcc;
    let log;

    beforeEach(() => {
        lcc = JSON.parse(JSON.stringify(lcConfig));
        lcc.forceLegacyListing = false;
        lcc.autoCreateIndexes = true;

        conductor = new LifecycleConductor(
            zkConfig, kafkaConfig, lcc, repConfig, s3Config);
        log = conductor.logger.newRequestLogger();
        conductor._accountIdCache.set('testid', '123456789012');
    });

    afterEach(() => {
        sinon.restore();
    });

    describe('processBuckets', function test() {
        // timeout set to 4000 to account for 2s for async ops + 1s for bucket queue completion check interval
        this.timeout(4000);

        it('should return error when mongodbclient returns an error', done => {
            conductor._mongodbClient = {
                getIndexingJobs: (_, cb) => cb(null, ['job1', 'job2']),
                getCollection: () => ({
                    find: () => ({
                        project: () => ({
                            hasNext: () => new Promise((resolve, reject) => {
                                reject(new Error('error'));
                            })
                        })
                    })
                })
            };
            conductor._zkClient = { getData: (_, cb) => cb(null, null, cb) };

            sinon.stub(conductor, '_controlBacklog')
                .callsFake(cb => cb(null));

            conductor.processBuckets(err => {
                assert.strictEqual(err.message, 'error');
                done();
            });
        });

        it('should process buckets without errors', done => {
            conductor._mongodbClient = {
                getIndexingJobs: (_, cb) => cb(null, ['job1', 'job2']),
                getCollection: () => ({
                    find: () => ({
                        project: () => ({
                            hasNext: () => new Promise(resolve => {
                                resolve(null, false);
                            })
                        })
                    })
                })
            };
            conductor._zkClient = {
                getData: (_, cb) => cb(null, null, cb),
                setData: (path, data, version, cb) => cb(null, cb)
            };

            sinon.stub(conductor, '_controlBacklog')
                .callsFake(cb => cb(null));

            conductor.processBuckets(done);
        });

        // tests that `activeIndexingJobRetrieved` is not reset until the e
        it('should not reset `activeIndexingJobsRetrieved` while async operations are in progress', done => {
            const order = [];

            conductor._mongodbClient = { getIndexingJobs: () => {} };
            conductor._producer = { send: (msg, cb) => cb() };

            sinon.stub(conductor, '_controlBacklog')
                .callsFake(cb => cb(null));
            sinon.stub(conductor._mongodbClient, 'getIndexingJobs')
                .callsFake((_, cb) => cb(null, ['job1', 'job2']));
            sinon.stub(conductor, 'listBuckets')
                .callsFake((mQueue, log, cb) => {
                    mQueue.push({
                        bucketName: 'testbucket',
                        canonicalId: 'testId',
                        isLifecycle: true,
                    });
                    cb(null, 1);
                });
            sinon.stub(conductor, '_getAccountIds')
                .callsFake((_a, _b, cb) => {
                    order.push(1);
                    // assert that activeIndexingJobRetrieved is set to
                    // true while async operations are in progress
                    assert(conductor.activeIndexingJobsRetrieved);
                    setTimeout(() => cb(null, []), 1000);
                });
            sinon.stub(conductor, '_createBucketTaskMessages')
                .callsFake((_a, _b, cb) => {
                    order.push(2);
                    assert(conductor.activeIndexingJobsRetrieved);
                    setTimeout(() => cb(null, []), 1000);
                });

            conductor.processBuckets(() => {
                order.push(3);

                // assert that all async operation are completed in order.
                // this check is used to ensure that all async operations are
                // executed and that processBuckets is not terminated before the
                // completion of all necessary async operations.
                assert.deepStrictEqual(order, [1, 2, 3]);

                // assert that activeIndexinJobRetrieved is reset after async
                // operations have completed
                assert(!conductor.activeIndexingJobsRetrieved);

                done();
            });
        });
    });

    describe('_indexesGetOrCreate', () => {
        it('should return v2 for bucketd bucket sources', done => {
            conductor._bucketSource = 'bucketd';
            conductor._indexesGetOrCreate(getTask(undefined), log, (err, taskVersion) => {
                assert.ifError(err);
                assert.deepStrictEqual(taskVersion, lifecycleTaskVersions.v2);
                done();
            });
        });

        it('should return v2 for zookeeper bucket sources', done => {
            conductor._bucketSource = 'zookeeper';
            conductor._indexesGetOrCreate(getTask(undefined), log, (err, taskVersion) => {
                assert.ifError(err);
                assert.deepStrictEqual(taskVersion, lifecycleTaskVersions.v2);
                done();
            });
        });

        it('should return v1 for non-lifecyled buckets', done => {
            conductor._bucketSource = 'mongodb';
            conductor._indexesGetOrCreate(getTask(false), log, (err, taskVersion) => {
                assert.ifError(err);
                assert.deepStrictEqual(taskVersion, lifecycleTaskVersions.v1);
                done();
            });
        });

        it('should return v1 if backbeat client cannot be created', done => {
            conductor._bucketSource = 'mongodb';
            conductor.clientManager.getBackbeatMetadataProxy = () => null;
            conductor._indexesGetOrCreate(getTask(true), log, (err, taskVersion) => {
                assert.ifError(err);
                assert.deepStrictEqual(taskVersion, lifecycleTaskVersions.v1);
                done();
            });
        });

        const tests = [
            [
                'should return v2',
                [
                    [], // job state
                    indexesForFeature.lifecycle.v2, // getIndex response
                    null, // metadata proxy error
                    true, // flag for status ofin progress job retrieval
                    'mongodb',
                ],
                [
                    [], // updated job state
                    null, // expected putIndex object
                    lifecycleTaskVersions.v2, // expected version
                ],
            ],
            [
                'should return v2 if backend is not MongoDB',
                [
                    [],
                    null,
                    null,
                    true,
                    'bucketd',
                ],
                [
                    [],
                    null,
                    lifecycleTaskVersions.v2,
                ],
            ],
            [
                'should return v1: missing indexes + put indexes',
                [
                    [],
                    [],
                    null,
                    true,
                    'mongodb',
                ],
                [
                    [
                        { bucket: 'testbucket', indexes: indexesForFeature.lifecycle.v2 },
                    ],
                    indexesForFeature.lifecycle.v2,
                    lifecycleTaskVersions.v1,
                ],
            ],
            [
                'should return v1: missing indexes + skip put indexes when bucket is in job list',
                [
                    [
                        { bucket: 'testbucket', indexes: indexesForFeature.lifecycle.v2 },
                    ],
                    [],
                    null,
                    true,
                    'mongodb',
                ],
                [
                    [
                        { bucket: 'testbucket', indexes: indexesForFeature.lifecycle.v2 },
                    ],
                    null,
                    lifecycleTaskVersions.v1,
                ],
            ],
            [
                'should return v1: missing indexes + skip put indexes when job list is at limit',
                [
                    [
                        { bucket: 'testbucket2', indexes: indexesForFeature.lifecycle.v2 },
                        { bucket: 'testbucket3', indexes: indexesForFeature.lifecycle.v2 },
                    ],
                    [],
                    null,
                    true,
                    'mongodb',
                ],
                [
                    [
                        { bucket: 'testbucket2', indexes: indexesForFeature.lifecycle.v2 },
                        { bucket: 'testbucket3', indexes: indexesForFeature.lifecycle.v2 },
                    ],
                    null,
                    lifecycleTaskVersions.v1,
                ],
            ],
            [
                'should return v1: missing indexes + skip put indexes when in progress index request fails',
                [
                    [],
                    [],
                    null,
                    false,
                    'mongodb',
                ],
                [
                    [],
                    null,
                    lifecycleTaskVersions.v1,
                ],
            ],
            [
                'should return v1: index request fails',
                [
                    [],
                    [],
                    new Error('test error'),
                    true,
                    'mongodb',
                ],
                [
                    [],
                    null,
                    lifecycleTaskVersions.v1,
                ],
            ],
        ];

        tests.forEach(([msg, input, expected]) =>
            it(msg, done => {
                const [inJobs, getIndexes, mockError, getInProgressSucceeded, bucektSource] = input;
                const [expectedJobs, putIndexes, expectedVersion] = expected;

                const client = new BackbeatMetadataProxyMock();
                conductor.clientManager.getBackbeatMetadataProxy = () => client;
                conductor.activeIndexingJobsRetrieved = getInProgressSucceeded;
                conductor.activeIndexingJobs = inJobs;
                conductor._bucketSource = bucektSource;
                conductor._mongodbClient = {
                    getDiskUsage: cb => cb(null, { available: 10000000000, free: 10000000000, total: 20000000000 }),
                    getCollectionStats: (bucketName, _log, cb) => cb(null, {
                        indexSizes: { _id_: 80000 },
                    }),
                };
                client.indexesObj = getIndexes;
                client.error = mockError;

                conductor._indexesGetOrCreate(getTask(true), log, (err, taskVersion) => {
                    assert.ifError(err);
                    assert.deepStrictEqual(client.receivedIdxObj, putIndexes);
                    assert.deepStrictEqual(conductor.activeIndexingJobs, expectedJobs);
                    assert.deepStrictEqual(taskVersion, expectedVersion);
                    done();
                });
            }));

        it('should return v1: SosAPI bucket skips index creation', done => {
            const client = new BackbeatMetadataProxyMock();
            conductor.clientManager.getBackbeatMetadataProxy = () => client;
            conductor.activeIndexingJobsRetrieved = true;
            conductor.activeIndexingJobs = [];
            conductor._bucketSource = 'mongodb';
            client.indexesObj = [];
            client.error = null;

            const sosTask = { ...getTask(true), hasSosApi: true };
            conductor._indexesGetOrCreate(sosTask, log, (err, taskVersion) => {
                assert.ifError(err);
                assert.deepStrictEqual(client.receivedIdxObj, null);
                assert.deepStrictEqual(conductor.activeIndexingJobs, []);
                assert.deepStrictEqual(taskVersion, lifecycleTaskVersions.v1);
                done();
            });
        });

        it('should still return v2 when SosAPI bucket already has indexes built', done => {
            const client = new BackbeatMetadataProxyMock();
            conductor.clientManager.getBackbeatMetadataProxy = () => client;
            conductor.activeIndexingJobsRetrieved = true;
            conductor.activeIndexingJobs = [];
            conductor._bucketSource = 'mongodb';
            client.indexesObj = indexesForFeature.lifecycle.v2;
            client.error = null;

            const sosTask = { ...getTask(true), hasSosApi: true };
            conductor._indexesGetOrCreate(sosTask, log, (err, taskVersion) => {
                assert.ifError(err);
                assert.deepStrictEqual(taskVersion, lifecycleTaskVersions.v2);
                done();
            });
        });

        it('should return v1: missing indexes + disk space check fails', done => {
            const client = new BackbeatMetadataProxyMock();
            conductor.clientManager.getBackbeatMetadataProxy = () => client;
            conductor.activeIndexingJobsRetrieved = true;
            conductor.activeIndexingJobs = [];
            conductor._bucketSource = 'mongodb';
            conductor._mongodbClient = {
                getDiskUsage: cb => cb(new Error('dbStats failed')),
                getCollectionStats: (bucketName, _log, cb) => cb(null, {
                    indexSizes: { _id_: 80000 },
                }),
            };
            client.indexesObj = [];
            client.error = null;

            conductor._indexesGetOrCreate(getTask(true), log, (err, taskVersion) => {
                assert.ifError(err);
                assert.deepStrictEqual(client.receivedIdxObj, null);
                assert.deepStrictEqual(conductor.activeIndexingJobs, []);
                assert.deepStrictEqual(taskVersion, lifecycleTaskVersions.v1);
                done();
            });
        });

        it('should return v1: missing indexes + insufficient disk space', done => {
            const client = new BackbeatMetadataProxyMock();
            conductor.clientManager.getBackbeatMetadataProxy = () => client;
            conductor.activeIndexingJobsRetrieved = true;
            conductor.activeIndexingJobs = [];
            conductor._bucketSource = 'mongodb';
            conductor._mongodbClient = {
                getDiskUsage: cb => cb(null, { available: 100000, free: 100000, total: 20000000000 }),
                getCollectionStats: (bucketName, _log, cb) => cb(null, {
                    indexSizes: { _id_: 80000000 },
                }),
            };
            client.indexesObj = [];
            client.error = null;

            conductor._indexesGetOrCreate(getTask(true), log, (err, taskVersion) => {
                assert.ifError(err);
                assert.deepStrictEqual(client.receivedIdxObj, null);
                assert.deepStrictEqual(conductor.activeIndexingJobs, []);
                assert.deepStrictEqual(taskVersion, lifecycleTaskVersions.v1);
                done();
            });
        });
    });

    describe('listBuckets', () => {
        const queue = new Queue();

        beforeEach(() => {
            queue.clean();
        });

        it('should list buckets from zookeeper', done => {
            const lcConductor = makeLifecycleConductorWithFilters({
                bucketSource: 'zookeeper',
            });
            lcConductor.listBuckets(queue, fakeLogger, (err, length) => {
                assert.strictEqual(length, 2);
                assert.strictEqual(queue.length(), 2);
                const expectedQueue = [
                    {
                        canonicalId: account1,
                        bucketName: bucket1,
                    },
                    {
                        canonicalId: account2,
                        bucketName: bucket2,
                    },
                ];
                assert.deepStrictEqual(queue.list(), expectedQueue);
                done();
            });
        });

        it('should list buckets from bucketd', done => {
            const markers = [];
            const lcConductor = makeLifecycleConductorWithFilters({
                bucketSource: 'bucketd',
            }, markers);
            lcConductor.listBuckets(queue, fakeLogger, (err, length) => {
                assert.strictEqual(length, 2);
                assert.strictEqual(queue.length(), 2);

                const expectedQueue = [
                    {
                        canonicalId: account1,
                        bucketName: bucket1,
                    },
                    {
                        canonicalId: account2,
                        bucketName: bucket2,
                    },
                ];
                assert.deepStrictEqual(queue.list(), expectedQueue);
                // test the listing optimization was not applied.
                assert.deepStrictEqual(markers, [`${account1}${splitter}${bucket1}`]);
                done();
            });
        });

        it('should filter out invalid buckets when listing from zookeeper', done => {
            const lcConductor = makeLifecycleConductorWithFilters({
                bucketSource: 'zookeeper',
            });
            sinon.stub(lcConductor._zkClient, 'getChildren').yields(null, [
                `${account1}:bucketuid123:${bucket1}`,
                `${account2}:bucketuid456:${bucket2}`,
                'invalid:bucketuid789',
                'invalid',
            ]);
            lcConductor.listBuckets(queue, fakeLogger, (err, length) => {
                assert.strictEqual(length, 2);
                assert.strictEqual(queue.length(), 2);
                const expectedQueue = [
                    {
                        canonicalId: account1,
                        bucketName: bucket1,
                    },
                    {
                        canonicalId: account2,
                        bucketName: bucket2,
                    },
                ];
                assert.deepStrictEqual(queue.list(), expectedQueue);
                done();
            });
        });

        it('should filter by bucket when listing from zookeeper', done => {
            const lcConductor = makeLifecycleConductorWithFilters({
                bucketsDenied: [bucket1],
                bucketSource: 'zookeeper',
            });
            lcConductor.listBuckets(queue, fakeLogger, (err, length) => {
                assert.strictEqual(length, 1);
                assert.strictEqual(queue.length(), 1);
                const expectedQueue = [
                    {
                        canonicalId: account2,
                        bucketName: bucket2,
                    },
                ];
                assert.deepStrictEqual(queue.list(), expectedQueue);
                done();
            });
        });

        it('should filter by bucket when listing from bucketd', done => {
            const markers = [];
            const lcConductor = makeLifecycleConductorWithFilters({
                bucketsDenied: [bucket1],
                bucketSource: 'bucketd',
            }, markers);
            lcConductor.listBuckets(queue, fakeLogger, (err, length) => {
                assert.strictEqual(length, 1);
                assert.strictEqual(queue.length(), 1);
                const expectedQueue = [
                    {
                        canonicalId: account2,
                        bucketName: bucket2,
                    },
                ];
                assert.deepStrictEqual(queue.list(), expectedQueue);
                // test the listing optimization was not applied.
                assert.deepStrictEqual(markers, [`${account1}${splitter}${bucket1}`]);
                done();
            });
        });

        it('should filter by account when listing from zookeeper', done => {
            const lcConductor = makeLifecycleConductorWithFilters({
                accountsDenied: [`${accountName1}:${account1}`],
                bucketSource: 'zookeeper',
            });
            lcConductor.listBuckets(queue, fakeLogger, (err, length) => {
                assert.strictEqual(length, 1);
                assert.strictEqual(queue.length(), 1);
                const expectedQueue = [
                    {
                        canonicalId: account2,
                        bucketName: bucket2,
                    },
                ];
                assert.deepStrictEqual(queue.list(), expectedQueue);
                done();
            });
        });

        it('should filter by account when listing from bucketd', done => {
            const markers = [];
            const lcConductor = makeLifecycleConductorWithFilters({
                accountsDenied: [`${accountName1}:${account1}`],
                bucketSource: 'bucketd',
            }, markers);
            lcConductor.listBuckets(queue, fakeLogger, (err, length) => {
                assert.strictEqual(length, 1);
                assert.strictEqual(queue.length(), 1);
                const expectedQueue = [
                    {
                        canonicalId: account2,
                        bucketName: bucket2,
                    },
                ];
                assert.deepStrictEqual(queue.list(), expectedQueue);
                // test the listing optimization was applied
                assert.deepStrictEqual(markers, [`${account1};`]);
                done();
            });
        });

        it('should filter by account and bucket when listing from zookeeper', done => {
            const lcConductor = makeLifecycleConductorWithFilters({
                accountsDenied: [`${accountName1}:${account1}`],
                bucketsDenied: [bucket2],
                bucketSource: 'zookeeper',
            });
            lcConductor.listBuckets(queue, fakeLogger, (err, length) => {
                assert.strictEqual(length, 0);
                assert.strictEqual(queue.length(), 0);
                const expectedQueue = [];
                assert.deepStrictEqual(queue.list(), expectedQueue);
                done();
            });
        });

        it('should filter by account and bucket when listing from bucketd', done => {
            const markers = [];
            const lcConductor = makeLifecycleConductorWithFilters({
                accountsDenied: [`${accountName1}:${account1}`],
                bucketsDenied: [bucket2],
                bucketSource: 'bucketd',
            }, markers);
            lcConductor.listBuckets(queue, fakeLogger, (err, length) => {
                assert.strictEqual(length, 0);
                assert.strictEqual(queue.length(), 0);
                const expectedQueue = [];
                assert.deepStrictEqual(queue.list(), expectedQueue);
                // test the listing optimization was not applied.
                assert.deepStrictEqual(markers, [`${account1}${splitter}${bucket1}`]);
                done();
            });
        });

        it('should not use any filter when listing from from mongodb', done => {
            const lcConductor = makeLifecycleConductorWithFilters({
                bucketSource: 'mongodb',
            }, []);
            const findStub = sinon.stub().returns({
                project: () => ({
                    hasNext: sinon.stub().resolves(false),
                })
            });
            lcConductor._mongodbClient = {
                getIndexingJobs: (_, cb) => cb(null, ['job1', 'job2']),
                getCollection: () => ({ find: findStub })
            };
            sinon.stub(lcConductor._zkClient, 'getData').yields(null, null, null);
            lcConductor.listBuckets(queue, fakeLogger, err => {
                assert.ifError(err);
                assert.deepEqual(findStub.getCall(0).args[0], {});
                done();
            });
        });

        it('should filter by account when listing from from mongodb', done => {
            const lcConductor = makeLifecycleConductorWithFilters({
                accountsDenied: [`${accountName1}:${account1}`],
                bucketSource: 'mongodb',
            }, []);
            const findStub = sinon.stub().returns({
                project: () => ({
                    hasNext: sinon.stub().resolves(false),
                })
            });
            lcConductor._mongodbClient = {
                getIndexingJobs: (_, cb) => cb(null, ['job1', 'job2']),
                getCollection: () => ({ find: findStub })
            };
            sinon.stub(lcConductor._zkClient, 'getData').yields(null, null, null);
            lcConductor.listBuckets(queue, fakeLogger, err => {
                assert.ifError(err);
                assert.deepEqual(findStub.getCall(0).args[0], {
                    'value.owner': {
                        $nin: [
                            'ab288756448dc58f61482903131e7ae533553d20b52b0e2ef80235599a1b9143',
                        ],
                    },
                });
                done();
            });
        });

        it('should filter by bucket when listing from from mongodb', done => {
            const lcConductor = makeLifecycleConductorWithFilters({
                bucketsDenied: [bucket2],
                bucketSource: 'mongodb',
            }, []);
            const findStub = sinon.stub().returns({
                project: () => ({
                    hasNext: sinon.stub().resolves(false),
                })
            });
            lcConductor._mongodbClient = {
                getIndexingJobs: (_, cb) => cb(null, ['job1', 'job2']),
                getCollection: () => ({ find: findStub })
            };
            sinon.stub(lcConductor._zkClient, 'getData').yields(null, null, null);
            lcConductor.listBuckets(queue, fakeLogger, err => {
                assert.ifError(err);
                assert.deepEqual(findStub.getCall(0).args[0], {
                    _id: {
                        $nin: ['bucket2'],
                    },
                });
                done();
            });
        });

        it('should use the resume marker when listing from from mongodb', done => {
            const lcConductor = makeLifecycleConductorWithFilters({
                bucketSource: 'mongodb',
            }, []);
            const findStub = sinon.stub().returns({
                project: () => ({
                    hasNext: sinon.stub().resolves(false),
                })
            });
            lcConductor._mongodbClient = {
                getIndexingJobs: (_, cb) => cb(null, ['job1', 'job2']),
                getCollection: () => ({ find: findStub })
            };
            sinon.stub(lcConductor._zkClient, 'getData').yields(null, Buffer.from('bucket1'), null);
            lcConductor.listBuckets(queue, fakeLogger, err => {
                assert.ifError(err);
                assert.deepEqual(findStub.getCall(0).args[0], {
                    _id: {
                        $gt: 'bucket1',
                    },
                });
                done();
            });
        });

        it('should filter by account and bucket when listing from from mongodb', done => {
            const lcConductor = makeLifecycleConductorWithFilters({
                accountsDenied: [`${accountName1}:${account1}`],
                bucketsDenied: [bucket2],
                bucketSource: 'mongodb',
            }, []);
            const findStub = sinon.stub().returns({
                project: () => ({
                    hasNext: sinon.stub().resolves(false),
                })
            });
            lcConductor._mongodbClient = {
                getIndexingJobs: (_, cb) => cb(null, ['job1', 'job2']),
                getCollection: () => ({ find: findStub })
            };
            sinon.stub(lcConductor._zkClient, 'getData').yields(null, Buffer.from('bucket1'), null);
            lcConductor.listBuckets(queue, fakeLogger, err => {
                assert.ifError(err);
                assert.deepEqual(findStub.getCall(0).args[0], {
                    '_id': {
                        $gt: 'bucket1',
                        $nin: ['bucket2'],
                    },
                    'value.owner': {
                        $nin: [
                            'ab288756448dc58f61482903131e7ae533553d20b52b0e2ef80235599a1b9143',
                        ],
                    },
                });
                done();
            });
        });

        it('should populate hasSosApi from bucket capabilities when listing from mongodb', done => {
            const lcConductor = makeLifecycleConductorWithFilters({
                bucketSource: 'mongodb',
            }, []);

            const docs = [
                { _id: 'veeam-bucket', value: { owner: 'acc', capabilities: { VeeamSOSApi: { foo: 'bar' } } } },
                { _id: 'normal-bucket', value: { owner: 'acc' } },
            ];
            let idx = 0;
            const cursor = {
                hasNext: () => Promise.resolve(idx < docs.length),
                next: () => Promise.resolve(docs[idx++]),
            };
            const projectStub = sinon.stub().returns(cursor);
            const findStub = sinon.stub().returns({ project: projectStub });

            lcConductor._mongodbClient = {
                getIndexingJobs: (_, cb) => cb(null, []),
                getCollection: name => {
                    if (name === '__metastore') {return { find: findStub };}
                    return { collectionName: name };
                },
                _isSpecialCollection: () => false,
            };
            sinon.stub(lcConductor._zkClient, 'getData').yields(null, null, null);

            lcConductor.listBuckets(queue, fakeLogger, err => {
                assert.ifError(err);
                // VeeamSOSApi field must be in the projection so we can read it
                assert.strictEqual(projectStub.getCall(0).args[0]['value.capabilities.VeeamSOSApi'], 1);
                const tasks = queue.list();
                assert.strictEqual(tasks.length, 2);
                assert.strictEqual(tasks.find(t => t.bucketName === 'veeam-bucket').hasSosApi, true);
                assert.strictEqual(tasks.find(t => t.bucketName === 'normal-bucket').hasSosApi, false);
                done();
            });
        });
    });
});
