'use strict'; // eslint-disable-line

const assert = require('assert');
const sinon = require('sinon');

const LifecycleConductor = require(
    '../../../extensions/lifecycle/conductor/LifecycleConductor');
const {
    lifecycleTaskVersions,
    indexesForFeature
} = require('../../../lib/constants');

const {
    zkConfig,
    kafkaConfig,
    lcConfig,
    repConfig,
    s3Config,
} = require('../../functional/lifecycle/configObjects');
const { BackbeatMetadataProxyMock } = require('../mocks');

const testTask = {
    bucketName: 'testbucket',
    canonicalId: 'testid',
    isLifecycled: true,
};

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
        // tests that `activeIndexingJobRetrieved` is not reset until the e
        it('should not reset `activeIndexingJobsRetrieved` while async operations are in progress', done => {
            let order = [];

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
        it('should return v1 for bucketd bucket sources', () => {
            conductor._bucketSource = 'bucketd';
            conductor._indexesGetOrCreate(testTask, log, (err, taskVersion) => {
                assert.ifError(err);
                assert.deepStrictEqual(taskVersion, lifecycleTaskVersions.v1);
            });
        });

        it('should return v1 for zookeeper bucket sources', () => {
            conductor._bucketSource = 'zookeeper';
            conductor._indexesGetOrCreate(testTask, log, (err, taskVersion) => {
                assert.ifError(err);
                assert.deepStrictEqual(taskVersion, lifecycleTaskVersions.v1);
            });
        });

        it('should return v1 for non-lifecyled buckets', () => {
            conductor._bucketSource = 'mongodb';
            const task = Object.assign({}, testTask, { isLifecyled: false });
            conductor._indexesGetOrCreate(task, log, (err, taskVersion) => {
                assert.ifError(err);
                assert.deepStrictEqual(taskVersion, lifecycleTaskVersions.v1);
            });
        });

        it('should return v1 if backbeat client cannot be created', () => {
            conductor.clientManager.getBackbeatMetadataProxy = () => null;
            conductor._indexesGetOrCreate(testTask, log, (err, taskVersion) => {
                assert.ifError(err);
                assert.deepStrictEqual(taskVersion, lifecycleTaskVersions.v1);
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
                ],
                [
                    [], // updated job state
                    null, // expected putIndex object
                    lifecycleTaskVersions.v2, // expected version
                ],
            ],
            [
                'should return v1: missing indexes + put indexes',
                [
                    [],
                    [],
                    null,
                    true,
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
                const [inJobs, getIndexes, mockError, getInProgressSucceeded] = input;
                const [expectedJobs, putIndexes, expectedVersion] = expected;

                const client = new BackbeatMetadataProxyMock();
                conductor.clientManager.getBackbeatMetadataProxy = () => client;
                conductor.activeIndexingJobsRetrieved = getInProgressSucceeded;
                conductor.activeIndexingJobs = inJobs;
                client.indexesObj = getIndexes;
                client.error = mockError;

                conductor._indexesGetOrCreate(testTask, log, (err, taskVersion) => {
                    assert.ifError(err);
                    assert.deepStrictEqual(client.receivedIdxObj, putIndexes);
                    assert.deepStrictEqual(conductor.activeIndexingJobs, expectedJobs);
                    assert.deepStrictEqual(taskVersion, expectedVersion);
                    done();
                });
            }));
    });
});
