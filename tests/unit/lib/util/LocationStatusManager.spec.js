'use strict'; // eslint-disable-line

const sinon = require('sinon');
const async = require('async');
const assert = require('assert');

const fakeLogger = require('../../../utils/fakeLogger');
const locationConfig = require('../../../../conf/locationConfig.json') || {};
const LocationStatus = require('../../../../lib/models/LocationStatus');
const LocationStatusManager = require('../../../../lib/util/LocationStatusManager');

const zkNodes = {
    '/backbeat/crr/state/us-east-1': {
        paused: false,
    },
    '/backbeat/crr/state/us-east-2': {
        paused: false,
    },
};

const fakeMongoClient = {};
const fakeZkClient = {
    getData: (path, cb) => cb(null, Buffer.from(JSON.stringify(zkNodes[path]))),
};
const fakeRedisClient = {};

describe('LocationStatusManager', () => {
    let lsm;

    beforeEach(() => {
        lsm = new LocationStatusManager(
            fakeMongoClient,
            fakeZkClient,
            fakeRedisClient,
            {
                crr: {
                    namespace: '/backbeat/crr',
                    statePath: '/state',
                    topic: 'crr-topic',
                    isMongo: false,
                },
                ingestion: {
                    namespace: '/backbeat/ingestion',
                    statePath: '/state',
                    topic: 'ingestion-topic',
                    isMongo: false,
                },
                lifecycle: {
                    isMongo: true,
                },
            },
            fakeLogger,
        );
    });

    afterEach(() => {
        sinon.restore();
    });

    describe('pauseService', () => {
        it('should pause selected service', done => {
            sinon.stub(lsm, '_updateServiceStatusForLocation').yields();
            lsm._locationStatusStore['us-east-1'] = new LocationStatus(['lifecycle']);
            lsm._locationStatusStore['us-east-2'] = new LocationStatus(['lifecycle']);
            lsm.pauseService('lifecycle', ['us-east-1', 'us-east-2'], err => {
                assert.ifError(err);
                assert.strictEqual(lsm._locationStatusStore['us-east-1']
                    .getServicePauseStatus('lifecycle'), true);
                assert.strictEqual(lsm._locationStatusStore['us-east-2']
                    .getServicePauseStatus('lifecycle'), true);
                return done();
            });
        });
    });

    describe('_deleteResumeJob', () => {
        it('should clear previously scheduled resume job', () => {
            const cancelFn = sinon.stub();
            lsm._scheduledResumeJobs.lifecycle['us-east-1'] = {
                cancel: cancelFn,
            };
            lsm._deleteResumeJob('us-east-1', 'lifecycle');
            assert(cancelFn.calledOnce);
            assert.strictEqual(lsm._scheduledResumeJobs.lifecycle['us-east-1'], undefined);
        });
    });

    describe('_scheduleResumeJob', () => {
        it('should schedule and execute resume job', () => {
            const clock = sinon.useFakeTimers();
            sinon.stub(lsm, '_updateServiceStatusForLocation').yields();
            lsm._locationStatusStore['us-east-1'] = new LocationStatus(['lifecycle']);
            lsm._locationStatusStore['us-east-1'].pauseLocation('lifecycle');
            const date = new Date();
            date.setMinutes(date.getMinutes() + 1);
            lsm._scheduleResumeJob('us-east-1', 'lifecycle', date);
            assert.strictEqual(lsm._locationStatusStore['us-east-1']
                .getServicePauseStatus('lifecycle'), true);
            clock.tick(90000); // 1.5min in ms
            assert.strictEqual(lsm._locationStatusStore['us-east-1']
                .getServicePauseStatus('lifecycle'), false);
            clock.restore();
        });
    });

    describe('resumeService', () => {
        it('should resume paused locations', done => {
            sinon.stub(lsm, '_updateServiceStatusForLocation').yields();
            lsm._locationStatusStore['us-east-1'] = new LocationStatus(['lifecycle']);
            lsm._locationStatusStore['us-east-1'].pauseLocation('lifecycle');
            lsm._locationStatusStore['us-east-2'] = new LocationStatus(['lifecycle']);
            lsm._locationStatusStore['us-east-2'].pauseLocation('lifecycle');
            lsm.resumeService('lifecycle', ['us-east-1', 'us-east-2'], null, null, err => {
                assert.ifError(err);
                assert.strictEqual(lsm._locationStatusStore['us-east-1']
                    .getServicePauseStatus('lifecycle'), false);
                assert.strictEqual(lsm._locationStatusStore['us-east-2']
                    .getServicePauseStatus('lifecycle'), false);
                return done();
            });
        });

        it('should schedule a resume of paused locations', done => {
            const clock = sinon.useFakeTimers();
            sinon.stub(lsm, '_updateServiceStatusForLocation').yields();
            lsm._locationStatusStore['us-east-1'] = new LocationStatus(['lifecycle']);
            lsm._locationStatusStore['us-east-1'].pauseLocation('lifecycle');
            lsm._locationStatusStore['us-east-2'] = new LocationStatus(['lifecycle']);
            lsm._locationStatusStore['us-east-2'].pauseLocation('lifecycle');
            lsm.resumeService('lifecycle', ['us-east-1', 'us-east-2'], true, null, err => {
                assert.ifError(err);
                assert.strictEqual(lsm._locationStatusStore['us-east-1']
                    .getServicePauseStatus('lifecycle'), true);
                assert.strictEqual(lsm._locationStatusStore['us-east-2']
                    .getServicePauseStatus('lifecycle'), true);
                // default schedule is 6hrs
                clock.tick(2.34e+7); // 6.5hrs in ms
                assert.strictEqual(lsm._locationStatusStore['us-east-1']
                    .getServicePauseStatus('lifecycle'), false);
                assert.strictEqual(lsm._locationStatusStore['us-east-2']
                    .getServicePauseStatus('lifecycle'), false);
                clock.restore();
                return done();
            });
        });
    });

    describe('deleteScheduledResumeService', () => {
        it('should delete scheduled resume job', done => {
            const clock = sinon.useFakeTimers();
            sinon.stub(lsm, '_updateServiceStatusForLocation').yields();
            lsm._locationStatusStore['us-east-1'] = new LocationStatus(['lifecycle']);
            lsm._locationStatusStore['us-east-1'].pauseLocation('lifecycle');
            lsm._locationStatusStore['us-east-2'] = new LocationStatus(['lifecycle']);
            lsm._locationStatusStore['us-east-2'].pauseLocation('lifecycle');
            async.series([
                next => lsm.resumeService('lifecycle', ['us-east-1', 'us-east-2'], true, null, next),
                next => lsm.deleteScheduledResumeService('lifecycle', ['us-east-1', 'us-east-2'], next),
                next => {
                    // default schedule is 6hrs
                    clock.tick(2.52e+7); // 7hrs in ms
                    assert.strictEqual(lsm._locationStatusStore['us-east-1']
                        .getServicePauseStatus('lifecycle'), true);
                    assert.strictEqual(lsm._locationStatusStore['us-east-2']
                        .getServicePauseStatus('lifecycle'), true);
                    clock.restore();
                    return next();
                },
            ], done);
        });
    });


    describe('getResumeSchedule', () => {
        it('should return the scheduled time for a resume job', done => {
            const date = new Date();
            const expectedSchedules = {
                'us-east-1': date.toString(),
                'us-east-2': 'none',
            };
            lsm._locationStatusStore['us-east-1'] = new LocationStatus(['lifecycle']);
            lsm._locationStatusStore['us-east-1'].pauseLocation('lifecycle');
            lsm._locationStatusStore['us-east-1'].resumeLocation('lifecycle', date);
            lsm._locationStatusStore['us-east-2'] = new LocationStatus(['lifecycle']);
            lsm.getResumeSchedule('lifecycle', ['us-east-1', 'us-east-2'], (err, schedules) => {
                assert.ifError(err);
                assert.deepStrictEqual(schedules, expectedSchedules);
                return done();
            });
        });
    });

    describe('getServiceStatus', () => {
        it('should return the pause/resume status of a service', done => {
            const expectedStatuses = {
                'us-east-1': 'disabled',
                'us-east-2': 'enabled',
            };
            lsm._locationStatusStore['us-east-1'] = new LocationStatus(['lifecycle']);
            lsm._locationStatusStore['us-east-1'].pauseLocation('lifecycle');
            lsm._locationStatusStore['us-east-2'] = new LocationStatus(['lifecycle']);
            lsm.getServiceStatus('lifecycle', ['us-east-1', 'us-east-2'], (err, statuses) => {
                assert.ifError(err);
                assert.deepStrictEqual(statuses, expectedStatuses);
                return done();
            });
        });
    });

    describe('_setupLocationStatusStore', () => {
        [
            {
                case: 'should add missing locations',
                mongoLocations: [
                    {
                        _id: 'us-east-1',
                        value: {
                            lifecycle: {
                                paused: true,
                                scheduledResume: null,
                            }
                        }
                    },
                    {
                        _id: 'us-east-2',
                        value: {
                            lifecycle: {
                                paused: true,
                                scheduledResume: null,
                            }
                        }
                    },
                ],
            },
            {
                case: 'should remove invalid locations',
                mongoLocations: [
                    {
                        _id: 'us-east-3',
                        value: {
                            lifecycle: {
                                paused: true,
                                scheduledResume: null,
                            }
                        }
                    },
                ],
            }
        ].forEach(params => {
            it(params.case, done => {
                lsm._mongoClient = {
                    createCollection: sinon.stub().yields(),
                    collection: () => ({
                        find: sinon.stub().yields(null, {
                            toArray: sinon.stub().yields(null, params.mongoLocations),
                        }),
                        insert: sinon.stub().yields(),
                        deleteMany: sinon.stub().yields(),
                    }),
                };
                lsm._setupLocationStatusStore(err => {
                    assert.ifError(err);
                    const finalLocations = Object.keys(lsm._locationStatusStore);
                    const validLocations = Object.keys(locationConfig);
                    assert.deepStrictEqual(finalLocations, validLocations);
                    return done();
                });
            });
        });
    });

    describe('_getStateDetails', () => {
        it('should return the correct state (mongo)', done => {
            const zkStateStub = sinon.stub(lsm, '_getZkStateDetails').yields();
            lsm._locationStatusStore = {
                'us-east-1': new LocationStatus(['lifecycle']),
                'us-east-2': new LocationStatus(['lifecycle']),
            };
            lsm._getStateDetails('lifecycle', ['us-east-1', 'us-east-2'], err => {
                assert.ifError(err);
                assert(zkStateStub.notCalled);
                return done();
            });
        });

        it('should return the correct state (zookeeper)', done => {
            const zkStateStub = sinon.stub(lsm, '_getZkStateDetails').yields();
            lsm._getStateDetails('ingestion', ['us-east-1', 'us-east-2'], err => {
                assert.ifError(err);
                assert(zkStateStub.calledOnce);
                return done();
            });
        });
    });

    describe('_getZkStateDetails', () => {
        it('should correctly read zookeeper state', done => {
            lsm._getZkStateDetails('crr', ['us-east-1', 'us-east-2'], (err, data) => {
                assert.ifError(err);
                assert.deepStrictEqual(data, {
                    'us-east-1': zkNodes['/backbeat/crr/state/us-east-1'],
                    'us-east-2': zkNodes['/backbeat/crr/state/us-east-2'],
                });
                return done();
            });
        });
    });


    describe('_parseScheduleResumeBody', () => {
        it('should correctly extract scheduled time from body', () => {
            const body = JSON.stringify({ hours: 1 });
            const hours = lsm._parseScheduleResumeBody(body);
            assert.deepStrictEqual(hours, { hours: 1 });
        });

        it('should set default value', () => {
            const body = JSON.stringify({});
            const hours = lsm._parseScheduleResumeBody(body);
            assert.deepStrictEqual(hours, { hours: 6 });
        });
    });

    describe('_pushActionToRedis', () => {
        it('should pick correct channel', () => {
            lsm._redis.publish = sinon.stub();
            const date = new Date();
            lsm._pushActionToRedis('crr', 'us-east-1', 'resumeService', date);
            assert(lsm._redis.publish.firstCall
                .calledWith('crr-topic-us-east-1', JSON.stringify({ action: 'resumeService', date })));
            lsm._pushActionToRedis('ingestion', 'us-east-2', 'pauseService');
            assert(lsm._redis.publish.lastCall
                .calledWith('ingestion-topic-us-east-2', JSON.stringify({ action: 'pauseService' })));
        });
    });

    describe('_addNewLocations', () => {
        it('should add new locations', done => {
            lsm._locationStatusColl = {
                insert: sinon.stub().yields(),
            };
            lsm._addNewLocations([], err => {
                assert.ifError(err);
                const validLocations = Object.keys(locationConfig);
                assert(Object.keys(lsm._locationStatusStore).every(loc => validLocations.includes(loc)));
                return done();
            });
        });
    });

    describe('_deleteInvalidLocations', () => {
        it('should remove invalid locations', done => {
            const mongoLocations = [
                {
                    _id: 'us-east-4',
                    value: {
                        lifecycle: {
                            paused: true,
                            scheduledResume: null,
                        }
                    }
                },
            ];
            const deleteManyStub = sinon.stub().yields();
            lsm._locationStatusColl = {
                deleteMany: deleteManyStub,
            };
            lsm._deleteInvalidLocations(mongoLocations, err => {
                assert.ifError(err);
                assert.deepStrictEqual(deleteManyStub.args[0][0], {
                    _id: {
                        $in: ['us-east-4'],
                    }
                });
                return done();
            });
        });
    });

    describe('_getPreviousLocationStates', () => {
        it('should retreive location state', done => {
            sinon.stub(lsm, '_scheduleResumeJob');
            const date = new Date();
            const mongoLocations = [
                {
                    _id: 'us-east-1',
                    value: {
                        lifecycle: {
                            paused: true,
                            scheduledResume: date.toString(),
                        }
                    }
                },
            ];
            lsm._getPreviousLocationStates(mongoLocations, err => {
                assert.ifError(err);
                assert.strictEqual(lsm._locationStatusStore['us-east-1'].getServicePauseStatus('lifecycle'), true);
                assert.strictEqual(lsm._locationStatusStore['us-east-1']
                    .getServiceResumeSchedule('lifecycle').toString(), date.toString());
                return done();
            });
        });
    });
});
