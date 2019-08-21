const assert = require('assert');
const async = require('async');
const schedule = require('node-schedule');

const IngestionPopulator =
    require('../../../lib/queuePopulator/IngestionPopulator');
const TimeMachine = require('../utils/timeMachine');
const ZKStateHelper = require('../utils/pauseResumeUtils/zkStateHelper');
const MockAPI = require('../utils/pauseResumeUtils/mockAPI');

// Configs
const Config = require('../../../lib/Config');
const { zookeeperNamespace } =
    require('../../../extensions/ingestion/constants');

const redisConfig = {
    host: Config.redis.host,
    port: Config.redis.port,
};
const kafkaConfig = Config.kafka;
const zkConfig = Config.zookeeper;
const ingestionConfig = Config.extensions.ingestion;
const s3Config = Config.s3;

// Constants
const ZK_TEST_STATE_PATH = `${zookeeperNamespace}/state`;

// Future Date to be used in tests
const futureDate = new Date();
futureDate.setHours(futureDate.getHours() + 5);

// test ingestion buckets we should fetch from mongo
const firstBucket = {
    // zenko bucket name
    name: 'zenko-bucket',
    // zenko storage location
    locationConstraint: 'ring-location',
    // s3c bucket name
    bucketName: 'ring-bucket',
};
const secondBucket = {
    name: 'other-zenko-bucket',
    locationConstraint: 'other-ring-location',
    bucketName: 'other-ring-bucket',
};

// This is passed from initManagement to Config and only represents the data
// we need in IngestionPopulator
const existingLocationConstraints = {
    [firstBucket.locationConstraint]: {
        details: {
            accessKey: 'accessKey1',
            secretKey: 'verySecretKey1',
            endpoint: 'http://localhost:8000',
            bucketName: firstBucket.bucketName,
        },
        locationType: 'location-scality-ring-s3-v1',
    },
    [secondBucket.locationConstraint]: {
        details: {
            accessKey: 'accessKey1',
            secretKey: 'verySecretKey1',
            endpoint: 'http://localhost:8000',
            bucketName: secondBucket.bucketName,
        },
        locationType: 'location-scality-ring-s3-v1',
    },
};

class IngestionPopulatorMock extends IngestionPopulator {

    /* Overwrite setup process */

    open(cb) {
        super.open(err => {
            assert.ifError(err);

            const buckets = [firstBucket, secondBucket];
            Config.setIngestionBuckets(existingLocationConstraints, buckets);

            return cb(err);
        });
    }

    _setupMetricsClients(cb) {
        return cb();
    }

    /* Getters for testing purposes */

    getIngestionSourceNames() {
        return Object.keys(this._ingestionSources);
    }

    getPausedLocations() {
        return this._pausedLocations;
    }

    getPausedLocationNames() {
        return Object.keys(this._pausedLocations);
    }

    getZkClient() {
        return this.zkClient;
    }

    getRedisPubSubClient() {
        return this._redis;
    }
}

/* eslint-disable no-param-reassign */
function locationConditionalCheck(iPopulator, location, zkPauseState, cFxn,
done) {
    const zkClient = iPopulator.getZkClient();
    const path = `${ZK_TEST_STATE_PATH}/${location}`;
    async.doWhilst(cb => setTimeout(() => {
        zkClient.getData(path, (err, data) => {
            if (err) {
                return cb(err);
            }
            const parsedData = JSON.parse(data);
            Object.keys(parsedData).forEach(key => {
                zkPauseState[key] = parsedData[key];
            });
            return cb();
        });
    }, 1000), cFxn, error => done(error, zkPauseState));
}
/* eslint-enable no-param-reassign */

function pauseLocation(iPopulator, location, done) {
    const state = {};
    const conditionFxn = () => {
        const pausedLocations = iPopulator.getPausedLocationNames();
        return !pausedLocations.includes(location);
    };
    locationConditionalCheck(iPopulator, location, state, conditionFxn,
    (err, zkPauseState) => {
        if (err) {
            return done(err);
        }
        assert.strictEqual(zkPauseState.paused, true);
        return done(null, zkPauseState);
    });
}

function resumeLocation(iPopulator, location, done) {
    const state = {};
    const conditionFxn = () => {
        const pausedLocations = iPopulator.getPausedLocationNames();
        return pausedLocations.includes(location);
    };
    locationConditionalCheck(iPopulator, location, state, conditionFxn,
    (err, zkPauseState) => {
        if (err) {
            return done(err);
        }
        assert.strictEqual(zkPauseState.paused, false);
        return done(null, zkPauseState);
    });
}

function resumeScheduleLocation(iPopulator, location, done) {
    const state = {};
    const conditionFxn = () => (state && !state.scheduledResume);
    locationConditionalCheck(iPopulator, location, state, conditionFxn,
    (err, zkPauseState) => {
        if (err) {
            return done(err);
        }
        assert.strictEqual(zkPauseState.paused, true);
        assert(zkPauseState.scheduledResume);
        return done(null, zkPauseState);
    });
}

function deleteScheduledResumeLocation(iPopulator, location, done) {
    const state = {};
    const conditionFxn = () => (state && state.scheduledResume);
    locationConditionalCheck(iPopulator, location, state, conditionFxn,
    (err, zkPauseState) => {
        if (err) {
            return done(err);
        }
        assert.strictEqual(zkPauseState.scheduledResume, null);
        return done(null, zkPauseState);
    });
}

/*
    Create an initial state where the `firstBucket` location is not paused and
    `secondBucket` is paused and has a scheduled resume for `futureDate`.

    The IngestionPopulator holds state for all ingestion buckets as:
        `{ zenko-bucket-name: IngestionReader() }`
    This object keeps track of all ingestion buckets regardless of state.

    The IngestionPopulator holds state for all paused locations as:
        `{ location-constraint: null || new schedule }`
    This object keeps track of all paused ingestion locations.
*/

describe('Ingestion Pause/Resume', function d() {
    this.timeout(10000);

    this.mockAPI = new MockAPI(ingestionConfig);

    before(done => {
        this.zkHelper = new ZKStateHelper(zkConfig, ZK_TEST_STATE_PATH,
            firstBucket.locationConstraint, secondBucket.locationConstraint,
            futureDate);
        this.zkHelper.init(err => {
            if (err) {
                return done(err);
            }
            const zkClient = this.zkHelper.getClient();
            this.iPopulator = new IngestionPopulatorMock(zkClient, zkConfig,
                kafkaConfig, {}, {}, redisConfig, ingestionConfig, s3Config);
            return done();
        });
    });

    beforeEach(done => {
        const firstLocation = firstBucket.locationConstraint;
        const secondLocation = secondBucket.locationConstraint;
        async.series([
            next => this.iPopulator.open(next),
            next => {
                this.iPopulator._resumeService(firstLocation);
                this.iPopulator._pauseService(secondLocation);
                return this.iPopulator.applyUpdates(next);
            },
        ], err => {
            if (err) {
                return done(err);
            }
            let pausedList = this.iPopulator.getPausedLocations();
            return async.whilst(() =>
                Object.keys(pausedList).includes(firstLocation) ||
                pausedList[secondLocation] === undefined ||
                (pausedList[secondLocation] &&
                 pausedList[secondLocation].constructor.name !== 'Job'),
            cb => setTimeout(() => {
                this.iPopulator._resumeService(firstLocation);
                this.iPopulator._resumeService(secondLocation, futureDate);
                pausedList = this.iPopulator.getPausedLocations();
                cb();
            }, 1000), err => {
                if (err) {
                    return done(err);
                }
                return setTimeout(done, 2000);
            });
        });
    });

    afterEach(() => {
        const redisClient = this.iPopulator.getRedisPubSubClient();
        if (redisClient) {
            redisClient.quit();
        }
    });

    after(() => {
        this.zkHelper.close();
    });

    it('should setup initial state', done => {
        const locations = Object.keys(existingLocationConstraints);
        const zkClient = this.iPopulator.getZkClient();

        async.each(locations, (l, cb) => {
            const path = `${ZK_TEST_STATE_PATH}/${l}`;
            // assert bucket paths are created
            zkClient.getData(path, (err, data) => {
                assert.ifError(err);

                const parsedData = JSON.parse(data);
                if (firstBucket.locationConstraint === l) {
                    assert.strictEqual(parsedData.paused, false);
                }
                if (secondBucket.locationConstraint === l) {
                    // assert default state
                    assert.strictEqual(parsedData.paused, true);
                    // assert default state
                    assert(parsedData.scheduledResume);
                }
                return cb();
            });
        }, err => {
            assert.ifError(err);

            const sourceListNames = this.iPopulator.getIngestionSourceNames();
            const pausedListNames = this.iPopulator.getPausedLocationNames();

            // ingestion sources holds all IngestionReader instances
            assert.strictEqual(sourceListNames.length, 2);
            assert(sourceListNames.includes(firstBucket.name));
            assert(sourceListNames.includes(secondBucket.name));
            // paused locations holds all current paused locations and their
            // schedules (schedule resume), if any
            assert.strictEqual(pausedListNames.length, 1);
            assert(pausedListNames.includes(secondBucket.locationConstraint));

            done();
        });
    });

    it('should pause an active location', done => {
        // using first location
        const zenkoBucket = firstBucket.name;
        const location = firstBucket.locationConstraint;
        // send fake api request
        this.mockAPI.pauseService(location);
        pauseLocation(this.iPopulator, location, (err, zkPauseState) => {
            assert.ifError(err);

            const sourceListNames = this.iPopulator.getIngestionSourceNames();
            const pausedList = this.iPopulator.getPausedLocations();
            const pausedListNames = this.iPopulator.getPausedLocationNames();

            // should have paused first location with no scheduled resume
            assert.strictEqual(zkPauseState.paused, true);
            assert(!zkPauseState.scheduledResume);
            assert(sourceListNames.includes(zenkoBucket));
            assert(pausedListNames.includes(location));
            assert.strictEqual(pausedList[location], null);

            // should not have affected the other location
            const otherLocation = secondBucket.locationConstraint;
            assert(sourceListNames.includes(secondBucket.name));
            assert.strictEqual(
                pausedList[otherLocation].constructor.name, 'Job');
            done();
        });
    });

    it('should not change state if pausing an already paused location',
    done => {
        // using first location
        const zenkoBucket = firstBucket.name;
        const location = firstBucket.locationConstraint;

        async.series([
            // pause
            next => {
                this.mockAPI.pauseService(location);
                pauseLocation(this.iPopulator, location, next);
            },
            // then send another pause request and confirm state has not changed
            next => {
                this.mockAPI.pauseService(location);
                // just in case, give some time because we already expect state
                // to be set as paused. If for some reason, state will change,
                // we want to give the request time to make that unexpected
                // change
                setTimeout(() => {
                    pauseLocation(this.iPopulator, location, next);
                }, 2000);
            },
        ], (error, zkPauseStates) => {
            assert.ifError(error);

            const sourceListNames = this.iPopulator.getIngestionSourceNames();
            const pausedListNames = this.iPopulator.getPausedLocationNames();

            // first location should still be paused
            assert(sourceListNames.includes(zenkoBucket));
            assert(pausedListNames.includes(location));
            // zookeeper state should be the same
            assert.deepStrictEqual(zkPauseStates[0], zkPauseStates[1]);
            done();
        });
    });

    it('should resume a paused location', done => {
        // using second location
        const zenkoBucket = secondBucket.name;
        const location = secondBucket.locationConstraint;

        // send fake api request
        this.mockAPI.resumeService(location);
        resumeLocation(this.iPopulator, location, (err, zkPauseState) => {
            assert.ifError(err);

            const sourceListNames = this.iPopulator.getIngestionSourceNames();
            const pausedListNames = this.iPopulator.getPausedLocationNames();

            // should have resumed location
            assert.strictEqual(zkPauseState.paused, false);
            assert(sourceListNames.includes(zenkoBucket));
            assert(!pausedListNames.includes(location));

            // should not have affected other location
            assert(sourceListNames.includes(secondBucket.name));
            assert(!pausedListNames.includes(
                secondBucket.locationConstraint));
            done();
        });
    });

    it('should not change state if resuming an already active location',
    done => {
        // using first location
        const zenkoBucket = firstBucket.name;
        const location = firstBucket.locationConstraint;

        const sourceListNames = this.iPopulator.getIngestionSourceNames();
        const pausedListNames = this.iPopulator.getPausedLocationNames();

        // by default, should already be active
        assert(sourceListNames.includes(zenkoBucket));
        assert(!pausedListNames.includes(location));

        // send fake api request
        this.mockAPI.resumeService(location);
        // just in case, give some time because we already expect state
        // to be set as not paused. If for some reason, state will change,
        // we want to give the request time to make that unexpected change
        setTimeout(() => {
            resumeLocation(this.iPopulator, location,
            (err, zkPauseState) => {
                assert.ifError(err);

                const sourceListNames =
                    this.iPopulator.getIngestionSourceNames();
                const pausedListNames =
                    this.iPopulator.getPausedLocationNames();

                // first location should still be active
                assert(sourceListNames.includes(zenkoBucket));
                assert(!pausedListNames.includes(location));
                // zookeeper state should be the same
                assert.strictEqual(zkPauseState.paused, false);

                done();
            });
        }, 2000);
    });

    it('should schedule a resume', done => {
        // using first location
        const location = firstBucket.locationConstraint;

        // send fake api request
        this.mockAPI.pauseService(location);

        // first pause and confirm
        pauseLocation(this.iPopulator, location, error => {
            assert.ifError(error);

            // send fake api request
            this.mockAPI.resumeService(location, futureDate);
            resumeScheduleLocation(this.iPopulator, location,
            (err, zkPauseState) => {
                assert.ifError(err);

                const sourceListNames =
                    this.iPopulator.getIngestionSourceNames();
                const pausedList = this.iPopulator.getPausedLocations();
                const pausedListNames =
                    this.iPopulator.getPausedLocationNames();

                // should still be paused
                assert.strictEqual(zkPauseState.paused, true);
                assert(pausedListNames.includes(location));
                // should have a scheduled date matching futureDate
                assert(zkPauseState.scheduledResume);
                assert.strictEqual(futureDate.getTime(),
                    (new Date(zkPauseState.scheduledResume).getTime()));
                // should have saved the schedule in memory
                assert(pausedList[location]);
                assert.strictEqual(
                    pausedList[location].constructor.name, 'Job');

                // should not have affected other location
                const otherLocation = secondBucket.locationConstraint;
                assert(sourceListNames.includes(secondBucket.name));
                assert.strictEqual(
                    pausedList[otherLocation].constructor.name, 'Job');

                done();
            });
        });
    });

    it('should cancel a scheduled resume for a given location', done => {
        // using second location
        const location = secondBucket.locationConstraint;

        this.mockAPI.deleteScheduledResumeService(location);
        deleteScheduledResumeLocation(this.iPopulator, location,
        (err, zkPauseState) => {
            assert.ifError(err);

            const pausedList = this.iPopulator.getPausedLocations();
            const pausedListNames = this.iPopulator.getPausedLocationNames();

            // should still be paused
            assert.strictEqual(zkPauseState.paused, true);
            assert(pausedListNames.includes(location));
            // should no longer have a scheduled date
            assert.strictEqual(zkPauseState.scheduledResume, null);
            // should have removed the in-memory scheduled Job
            assert.strictEqual(pausedList[location], null);
            done();
        });
    });

    it('should not schedule a resume when the location is already active',
    done => {
        // using first location
        const location = firstBucket.locationConstraint;

        // schedule resume
        this.mockAPI.resumeService(location, futureDate);
        resumeLocation(this.iPopulator, location, (err, zkPauseState) => {
            assert.ifError(err);

            const pausedListNames = this.iPopulator.getPausedLocationNames();

            // state should be the same
            assert.strictEqual(zkPauseState.paused, false);
            assert(!pausedListNames.includes(location));
            assert(!zkPauseState.scheduledResume);
            done();
        });
    });

    [
        // new schedule time is less than current scheduled time
        {
            testCase: 'less than',
            timeChange: -1,
        },
        // new schedule time is greater than current scheduled time
        {
            testCase: 'greater than',
            timeChange: 1,
        },
    ].forEach(item => {
        it('should overwrite an existing scheduled resume on new valid ' +
        `request where new time is ${item.testCase} current scheduled time`,
        done => {
            // using first location
            const location = firstBucket.locationConstraint;
            const zkClient = this.iPopulator.getZkClient();

            let zkPauseState;
            async.series([
                next => {
                    // pause
                    this.mockAPI.pauseService(location);
                    pauseLocation(this.iPopulator, location, next);
                },
                next => {
                    // schedule resume
                    this.mockAPI.resumeService(location, futureDate);
                    resumeScheduleLocation(this.iPopulator, location,
                        next);
                },
            ], error => {
                assert.ifError(error);

                const path = `${ZK_TEST_STATE_PATH}/${location}`;
                const newScheduledDate = new Date(futureDate);
                newScheduledDate.setHours(newScheduledDate.getHours() +
                    item.timeChange);

                // send fake api request
                this.mockAPI.resumeService(location, newScheduledDate);

                async.doWhilst(cb => setTimeout(() => {
                    zkClient.getData(path, (err, data) => {
                        if (err) {
                            return cb(err);
                        }
                        zkPauseState = JSON.parse(data);
                        return cb();
                    });
                }, 1000), () => {
                    if (zkPauseState && zkPauseState.scheduledResume) {
                        const zkStateDate =
                            new Date(zkPauseState.scheduledResume);
                        return zkStateDate.getTime() !==
                            newScheduledDate.getTime();
                    }
                    return true;
                }, error => {
                    assert.ifError(error);

                    const pausedList = this.iPopulator.getPausedLocations();
                    const pausedListNames =
                        this.iPopulator.getPausedLocationNames();

                    // should still be paused
                    assert.strictEqual(zkPauseState.paused, true);
                    assert(pausedListNames.includes(location));
                    // should have a different date
                    assert.strictEqual(
                        new Date(zkPauseState.scheduledResume).toString(),
                        newScheduledDate.toString());
                    // scheduled job should still be saved in-memory
                    const schedule = pausedList[location];
                    assert(schedule);
                    assert.strictEqual(schedule.constructor.name, 'Job');
                    done();
                });
            });
        });
    });

    it('should cancel a scheduled resume when the location is manually resumed',
    done => {
        // using first location
        const location = firstBucket.locationConstraint;
        const zkClient = this.iPopulator.getZkClient();
        const path = `${ZK_TEST_STATE_PATH}/${location}`;

        return async.series([
            next => {
                // pause
                this.mockAPI.pauseService(location);
                pauseLocation(this.iPopulator, location, next);
            },
            next => {
                // schedule resume
                this.mockAPI.resumeService(location, futureDate);
                resumeScheduleLocation(this.iPopulator, location, next);
            },
            next => {
                // resume
                this.mockAPI.resumeService(location);
                resumeLocation(this.iPopulator, location, next);
            }
        ], error => {
            assert.ifError(error);

            // check zookeeper
            return zkClient.getData(path, (err, data) => {
                assert.ifError(err);

                const zkPauseState = JSON.parse(data);
                const pausedListNames =
                    this.iPopulator.getPausedLocationNames();

                // should not be paused
                assert.strictEqual(zkPauseState.paused, false);
                assert(!pausedListNames.includes(location));
                // scheduled resume job should be cancelled
                assert.strictEqual(zkPauseState.scheduledResume, null);
                return done();
            });
        });
    });

    it('should resume a location when a scheduled resume triggers', done => {
        // install time machine to eventually move time forward
        const timeMachine = new TimeMachine();
        timeMachine.install();
        // hack - for some reason, the scheduled job in QueueProcessor does not
        // trigger unless I schedule a job here.
        schedule.scheduleJob(new Date(), () => {});

        // using first location
        const location = firstBucket.locationConstraint;

        async.series([
            next => {
                // pause
                this.mockAPI.pauseService(location);
                pauseLocation(this.iPopulator, location, next);
            },
            next => {
                // schedule resume
                this.mockAPI.resumeService(location, futureDate);
                resumeScheduleLocation(this.iPopulator, location, next);
            },
        ], error => {
            assert.ifError(error);

            let zkPauseState;
            const zkClient = this.iPopulator.getZkClient();
            const path = `${ZK_TEST_STATE_PATH}/${location}`;

            // move time forward past `futureDate`
            timeMachine.moveTimeForward(6);

            async.doWhilst(cb => setTimeout(() => {
                zkClient.getData(path, (err, data) => {
                    if (err) {
                        return cb(err);
                    }
                    zkPauseState = JSON.parse(data);
                    return cb();
                });
            }, 1000), () => zkPauseState.paused !== false,
            error => {
                assert.ifError(error);

                const pausedListNames =
                    this.iPopulator.getPausedLocationNames();

                // location should be active
                assert.strictEqual(zkPauseState.paused, false);
                assert(!pausedListNames.includes(location));
                // location should no longer have scheduled resume
                assert.strictEqual(zkPauseState.scheduledResume, null);

                timeMachine.uninstall();

                done();
            });
        });
    });

    it('should not trigger a scheduled job that was previously cancelled',
    done => {
        // install time machine to eventually move time forward
        const timeMachine = new TimeMachine();
        timeMachine.install();
        // hack - for some reason, the scheduled job in QueueProcessor does not
        // trigger unless I schedule a job here.
        schedule.scheduleJob(new Date(), () => {});

        // using first location
        const location = firstBucket.locationConstraint;

        async.series([
            next => {
                // pause
                this.mockAPI.pauseService(location);
                pauseLocation(this.iPopulator, location, next);
            },
            next => {
                // schedule resume
                this.mockAPI.resumeService(location, futureDate);
                resumeScheduleLocation(this.iPopulator, location, next);
            },
            next => {
                // cancel scheduled resume
                this.mockAPI.deleteScheduledResumeService(location);
                deleteScheduledResumeLocation(this.iPopulator, location, next);
            },
        ], error => {
            assert.ifError(error);

            const zkClient = this.iPopulator.getZkClient();
            const path = `${ZK_TEST_STATE_PATH}/${location}`;

            // move time forward past `futureDate`
            timeMachine.moveTimeForward(6);

            // just in case, give some time
            setTimeout(() => {
                zkClient.getData(path, (err, data) => {
                    assert.ifError(err);

                    const zkPauseState = JSON.parse(data);
                    const pausedList = this.iPopulator.getPausedLocations();
                    const pausedListNames =
                        this.iPopulator.getPausedLocationNames();

                    // location should not have a scheduled resume
                    assert(pausedListNames.includes(location));
                    assert.strictEqual(pausedList[location], null);
                    // location should still be paused
                    assert.strictEqual(zkPauseState.paused, true);

                    timeMachine.uninstall();

                    done();
                });
            }, 2000);
        });
    });
});
