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

        conductor = new LifecycleConductor(
            zkConfig, kafkaConfig, lcc, repConfig, s3Config);
        log = conductor.logger.newRequestLogger();
        conductor._accountIdCache.set('testid', '123456789012');
    });

    afterEach(() => {
        sinon.restore();
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
            conductor._bucketSource = 'zookeeper';
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
                ],
                [
                    [], // upated job state
                    null, // expected putIndex object
                    lifecycleTaskVersions.v2, // expcted version
                ],
            ],
            [
                'should return v1: missing indexes + put indexes',
                [
                    [],
                    [],
                    null,
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
                'should return v1: index request fails',
                [
                    [],
                    [],
                    new Error('test error'),
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
                const [inJobs, getIndexes, mockError] = input;
                const [expectedJobs, putIndexes, expectedVersion] = expected;

                const client = new BackbeatMetadataProxyMock();
                conductor.clientManager.getBackbeatMetadataProxy = () => client;
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
