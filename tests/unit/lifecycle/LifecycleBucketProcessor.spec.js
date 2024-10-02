'use strict'; // eslint-disable-line

const assert = require('assert');
const sinon = require('sinon');
const async = require('async');

const LifecycleBucketProcessor = require(
    '../../../extensions/lifecycle/bucketProcessor/LifecycleBucketProcessor');

const {
    zkConfig,
    kafkaConfig,
    lcConfig,
    repConfig,
    s3Config,
    mongoConfig,
    timeOptions,
} = require('../../functional/lifecycle/configObjects');

const {
    bucketProcessorEntry,
    bucketProcessorV1Entry,
    bucketProcessorV2Entry
} = require('../../utils/kafkaEntries');
const LifecycleTask = require('../../../extensions/lifecycle/tasks/LifecycleTask');
const LifecycleTaskV2 = require('../../../extensions/lifecycle/tasks/LifecycleTaskV2');


describe('Lifecycle Bucket Processor', () => {
    let lbp;
    beforeEach(() => {
        lbp = new LifecycleBucketProcessor(
            zkConfig, kafkaConfig, lcConfig, repConfig, s3Config, mongoConfig, 'http', timeOptions);
    });

    afterEach(() => {
        sinon.restore();
    });

    it('_pauseServiceForLocation:: should add location to paused location list', () => {
        lbp._pauseServiceForLocation('new-location');
        assert(lbp._pausedLocations.has('new-location'));
    });

    it('_resumeService:: should add location to paused location list', () => {
        lbp._pauseServiceForLocation('new-location');
        lbp._resumeServiceForLocation('new-location');
        assert(!lbp._pausedLocations.has('new-location'));
    });

    describe('_shouldProcessConfig', () => {
        [
            {
                title: 'should return false if no lifecycle rules were set',
                lcConfig: {
                    Rules: [],
                },
                supportedRules: [
                    'Expiration',
                    'NoncurrentVersionExpiration',
                    'AbortIncompleteMultipartUpload',
                    'Transitions',
                    'NoncurrentVersionTransitions',
                ],
                pauseLocations: [],
                expected: false,
            }, {
                title: 'should return false if all lifecyle rules are disabled',
                lcConfig: {
                    Rules: [
                        {
                            Status: 'Disabled',
                            AbortIncompleteMultipartUpload: {
                                DaysAfterInitiation: 7
                            },
                            NoncurrentVersionExpiration: {
                                NoncurrentDays: 7
                            },
                            Expiration: {
                                Days: 1
                            },
                            ID: '64bf6794-55fd-43f4-8d44-54a54cd2bf09'
                        },
                        {
                            Status: 'Disabled',
                            Transitions: [{
                                Days: 10,
                                StorageClass: 'azure'
                            }],
                            ID: 'dac36d89-0005-4c78-8e00-7e9ace06a9c4'
                        }
                    ],
                },
                supportedRules: [
                    'Expiration',
                    'NoncurrentVersionExpiration',
                    'AbortIncompleteMultipartUpload',
                    'Transitions',
                    'NoncurrentVersionTransitions',
                ],
                pauseLocations: [],
                expected: false,
            }, {
                title: 'should return true if at least one lifecycle rule is enabled',
                lcConfig: {
                    Rules: [
                        {
                            Status: 'Disabled',
                            Expiration: {
                                Days: 1
                            },
                            ID: '64bf6794-55fd-43f4-8d44-54a54cd2bf09'
                        },
                        {
                            Status: 'Enabled',
                            Transitions: [{
                                Days: 10,
                                StorageClass: 'azure'
                            }],
                            ID: 'dac36d89-0005-4c78-8e00-7e9ace06a9c4'
                        }
                    ],
                },
                supportedRules: [
                    'Expiration',
                    'NoncurrentVersionExpiration',
                    'AbortIncompleteMultipartUpload',
                    'Transitions',
                    'NoncurrentVersionTransitions',
                ],
                pauseLocations: [],
                expected: true,
            }, {
                title: 'should return false if transition rule\'s location is paused',
                lcConfig: {
                    Rules: [
                        {
                            Status: 'Enabled',
                            Transitions: [{
                                Days: 10,
                                StorageClass: 'azure'
                            }],
                            ID: 'dac36d89-0005-4c78-8e00-7e9ace06a9c4'
                        }
                    ],
                },
                supportedRules: [
                    'Expiration',
                    'NoncurrentVersionExpiration',
                    'AbortIncompleteMultipartUpload',
                    'Transitions',
                    'NoncurrentVersionTransitions',
                ],
                pauseLocations: ['azure'],
                expected: false,
            }, {
                title: 'should return false if non current transition rule\'s location is paused',
                lcConfig: {
                    Rules: [
                        {
                            Status: 'Enabled',
                            NoncurrentVersionTransitions: [{
                                NoncurrentDays: 10,
                                StorageClass: 'azure'
                            }],
                            ID: 'dac36d89-0005-4c78-8e00-7e9ace06a9c4'
                        }
                    ],
                },
                supportedRules: [
                    'Expiration',
                    'NoncurrentVersionExpiration',
                    'AbortIncompleteMultipartUpload',
                    'Transitions',
                    'NoncurrentVersionTransitions',
                ],
                pauseLocations: ['azure'],
                expected: false,
            }, {
                title: 'should return false if no supported rule is enabled',
                lcConfig: {
                    Rules: [
                        {
                            Status: 'Enabled',
                            Transitions: [{
                                Days: 10,
                                StorageClass: 'azure'
                            }],
                            ID: 'dac36d89-0005-4c78-8e00-7e9ace06a9c4'
                        }
                    ],
                },
                supportedRules: [
                    'Expiration',
                    'NoncurrentVersionExpiration',
                    'AbortIncompleteMultipartUpload',
                ],
                pauseLocations: [],
                expected: false,
            },
        ].forEach(params => {
            it(params.title, () => {
                lbp._supportedRules = params.supportedRules;
                lbp._pausedLocations = new Set(params.pauseLocations);
                assert.strictEqual(lbp._shouldProcessConfig(params.lcConfig), params.expected);
            });
        });
    });

    describe('_processBucketEntry', () => {

        [
            {
                it: 'should use the forceLegacyListing when listing version is not ' +
                    'specified in the Kafka messages (v1 case)',
                forceLegacyListing: true,
                kafkaMessage: bucketProcessorEntry,
                validateTask: task => task instanceof LifecycleTask && !(task instanceof LifecycleTaskV2),
            },
            {
                it: 'should use the forceLegacyListing when listing version is not ' +
                    'specified in the Kafka messages (v2 case)',
                forceLegacyListing: false,
                kafkaMessage: bucketProcessorEntry,
                validateTask: task => task instanceof LifecycleTaskV2,
            },
            {
                it: 'should use the listing type specified in the kafka message (v1 case)',
                forceLegacyListing: false,
                kafkaMessage: bucketProcessorV1Entry,
                validateTask: task => task instanceof LifecycleTask && !(task instanceof LifecycleTaskV2),
            },
            {
                it: 'should use the listing type specified in the kafka message (v2 case)',
                forceLegacyListing: true,
                kafkaMessage: bucketProcessorV2Entry,
                validateTask: task => task instanceof LifecycleTaskV2,
            },
        ].forEach(opts => {
            it(opts.it, done => {
                if (opts.forceLegacyListing !== null) {
                    lbp._lcConfig.forceLegacyListing = opts.forceLegacyListing;
                }

                lbp.clientManager = {
                    getS3Client: () => ({}),
                    getBackbeatMetadataProxy: () => ({}),
                };

                sinon.stub(lbp, '_getBucketLifecycleConfiguration').yields(null, {
                    Rules: [{
                        Status: 'Enabled',
                        Transitions: [{
                            Days: 10,
                            StorageClass: 'azure'
                        }],
                        ID: 'dac36d89-0005-4c78-8e00-7e9ace06a9c4'
                    }]
                });

                const tasks = [];
                lbp._internalTaskScheduler = async.queue((ctx, cb) => {
                    tasks.push(ctx.task);
                    cb();
                }, 1);

                lbp._processBucketEntry(opts.kafkaMessage, err => {
                    assert.ifError(err);
                    assert.strictEqual(tasks.length, 1);
                    assert(opts.validateTask(tasks[0]));
                    done();
                });
            });
        });
    });
});
