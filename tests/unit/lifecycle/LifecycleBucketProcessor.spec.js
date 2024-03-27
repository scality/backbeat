'use strict'; // eslint-disable-line

const assert = require('assert');
const sinon = require('sinon');

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


describe('Lifecycle Bucket Processor', () => {
    let lbp;
    beforeEach(() => {
        lbp = new LifecycleBucketProcessor(
            zkConfig, kafkaConfig, lcConfig, repConfig, s3Config, mongoConfig, timeOptions);
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
                    'Noncurrentversionexpiration',
                    'AbortincompletemultipartUpload',
                    'Transition',
                    'Noncurrentversiontransition',
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
                            Transition: {
                                Days: 10,
                                StorageClass: 'azure'
                            },
                            ID: 'dac36d89-0005-4c78-8e00-7e9ace06a9c4'
                        }
                    ],
                },
                supportedRules: [
                    'Expiration',
                    'Noncurrentversionexpiration',
                    'AbortincompletemultipartUpload',
                    'Transition',
                    'Noncurrentversiontransition',
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
                            Transition: {
                                Days: 10,
                                StorageClass: 'azure'
                            },
                            ID: 'dac36d89-0005-4c78-8e00-7e9ace06a9c4'
                        }
                    ],
                },
                supportedRules: [
                    'Expiration',
                    'Noncurrentversionexpiration',
                    'AbortincompletemultipartUpload',
                    'Transition',
                    'Noncurrentversiontransition',
                ],
                pauseLocations: [],
                expected: true,
            }, {
                title: 'should return false if transition rule\'s location is paused',
                lcConfig: {
                    Rules: [
                        {
                            Status: 'Enabled',
                            Transition: {
                                Days: 10,
                                StorageClass: 'azure'
                            },
                            ID: 'dac36d89-0005-4c78-8e00-7e9ace06a9c4'
                        }
                    ],
                },
                supportedRules: [
                    'Expiration',
                    'Noncurrentversionexpiration',
                    'AbortincompletemultipartUpload',
                    'Transition',
                    'Noncurrentversiontransition',
                ],
                pauseLocations: ['azure'],
                expected: false,
            }, {
                title: 'should return false if non current transition rule\'s location is paused',
                lcConfig: {
                    Rules: [
                        {
                            Status: 'Enabled',
                            NoncurrentVersionTransition: {
                                NoncurrentDays: 10,
                                StorageClass: 'azure'
                            },
                            ID: 'dac36d89-0005-4c78-8e00-7e9ace06a9c4'
                        }
                    ],
                },
                supportedRules: [
                    'Expiration',
                    'Noncurrentversionexpiration',
                    'AbortincompletemultipartUpload',
                    'Transition',
                    'Noncurrentversiontransition',
                ],
                pauseLocations: ['azure'],
                expected: false,
            }, {
                title: 'should return false if no supported rule is enabled',
                lcConfig: {
                    Rules: [
                        {
                            Status: 'Enabled',
                            Transition: {
                                Days: 10,
                                StorageClass: 'azure'
                            },
                            ID: 'dac36d89-0005-4c78-8e00-7e9ace06a9c4'
                        }
                    ],
                },
                supportedRules: [
                    'Expiration',
                    'Noncurrentversionexpiration',
                    'AbortincompletemultipartUpload',
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
});
