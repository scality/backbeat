const assert = require('assert');

const LocationStatus = require('../../../../lib/models/LocationStatus');

const locationStatusObj = {
    crr: {
        paused: true,
        scheduledResume: null,
    },
    ingestion: {
        paused: false,
        scheduledResume: null,
    },
    lifecycle: {
        paused: true,
        scheduledResume: 'Fri Jan 27 2023 13:56:12 GMT+0100',
    },
};

describe('LocationStatus', () => {
    let locationStatus = null;

    beforeEach(() => {
        locationStatus = new LocationStatus(['crr', 'ingestion', 'lifecycle'], locationStatusObj);
    });

    it('should init location service status as unpaused', () => {
        const ls = new LocationStatus(['crr', 'ingestion', 'lifecycle']);
        Object.keys(ls._data).forEach(service => {
            assert.strictEqual(ls._data[service].paused, false);
            assert.strictEqual(ls._data[service].scheduledResume, null);
        });
    });

    it('should copy data from LocationStatus', () => {
        const lsToCopy = new LocationStatus(['crr', 'ingestion', 'lifecycle'], locationStatusObj);
        const ls = new LocationStatus(['crr', 'ingestion', 'lifecycle'], lsToCopy);
        Object.keys(locationStatusObj).forEach(service => {
            assert.strictEqual(ls._data[service].paused, lsToCopy._data[service].paused);
            assert.strictEqual(ls._data[service].scheduledResume, lsToCopy._data[service].scheduledResume);
        });
    });

    it('should copy data from object', () => {
        const ls = new LocationStatus(['crr', 'ingestion', 'lifecycle'], locationStatusObj);
        Object.keys(locationStatusObj).forEach(service => {
            assert.strictEqual(ls._data[service].paused, locationStatusObj[service].paused);
            assert.strictEqual(ls._data[service].scheduledResume, locationStatusObj[service].scheduledResume);
        });
    });

    it('should initialize non specified services', () => {
        const tmpLocStatus = Object.assign({}, locationStatusObj);
        delete tmpLocStatus.ingestion;
        const ls = new LocationStatus(['crr', 'ingestion', 'lifecycle'], tmpLocStatus);
        assert.strictEqual(ls._data.ingestion.paused, false);
        assert.strictEqual(ls._data.ingestion.scheduledResume, null);
    });

    it('should set service status', () => {
        locationStatus.setServicePauseStatus('ingestion', true);
        assert.strictEqual(locationStatus._data.ingestion.paused, true);
    });

    it('should get service status', () => {
        const isPaused = locationStatus.getServicePauseStatus('lifecycle');
        assert.strictEqual(isPaused, locationStatusObj.lifecycle.paused);
    });

    it('should set service resume schedule', () => {
        const date = new Date();
        locationStatus.setServiceResumeSchedule('crr', date);
        assert.strictEqual(locationStatus._data.crr.scheduledResume, date.toString());
    });

    it('should get service resume schedule', () => {
        const schedule = locationStatus.getServiceResumeSchedule('lifecycle');
        assert.deepEqual(schedule, new Date(locationStatusObj.lifecycle.scheduledResume));
    });

    it('should get null service resume schedule', () => {
        const schedule = locationStatus.getServiceResumeSchedule('crr');
        assert.strictEqual(schedule, null);
    });

    it('should pause services', () => {
        locationStatus.pauseLocation('ingestion');
        assert.strictEqual(locationStatus._data.ingestion.paused, true);
    });

    it('should resume services', () => {
        locationStatus.resumeLocation('lifecycle');
        assert.strictEqual(locationStatus._data.lifecycle.paused, false);
        assert.strictEqual(locationStatus._data.lifecycle.scheduledResume, null);
    });

    it('should set resume shedule', () => {
        const date = new Date();
        locationStatus.resumeLocation('crr', date);
        assert.strictEqual(locationStatus._data.crr.paused, true);
        assert.strictEqual(locationStatus._data.crr.scheduledResume, date.toString());
    });

    it('should return serialized location status data', () => {
        const data = locationStatus.getValue();
        assert.deepEqual(data, locationStatusObj);
    });
});
