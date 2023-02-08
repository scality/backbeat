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
} = require('../../functional/lifecycle/configObjects');


describe('Lifecycle Bucket Processor', () => {
    let lbp;
    beforeEach(() => {
        lbp = new LifecycleBucketProcessor(
            zkConfig, kafkaConfig, lcConfig, repConfig, s3Config, mongoConfig);
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
});
