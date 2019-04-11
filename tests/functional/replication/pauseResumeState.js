const assert = require('assert');
const async = require('async');
const Redis = require('ioredis');
const schedule = require('node-schedule');
const zookeeper = require('node-zookeeper-client');

const QueueProcessor = require('../../../extensions/replication' +
                               '/queueProcessor/QueueProcessor');
const TimeMachine = require('../utils/timeMachine');

// Configs
const config = require('../../config.json');
const constants = require('../../../extensions/replication/constants');
const redisConfig = {
    host: config.redis.host,
    port: config.redis.port,
};
const kafkaConfig = config.kafka;
const repConfig = config.extensions.replication;
const sourceConfig = {
    auth: { type: 'skip' },
};
const destConfig = {
    auth: { type: 'skip', vault: 'skip' },
    bootstrapList: repConfig.destination.bootstrapList,
};
const mConfig = config.metrics;

// Constants
const ZK_TEST_CRR_STATE_PATH =
    `${constants.zookeeperReplicationNamespace}/state`;
const EPHEMERAL_NODE = 1;

// Future Date to be used in tests
const futureDate = new Date();
futureDate.setHours(futureDate.getHours() + 5);

class ZKStateHelper {
    constructor(zkConfig, firstSite, secondSite) {
        this.zkConfig = zkConfig;
        this.zkClient = null;

        this.firstPath = `${ZK_TEST_CRR_STATE_PATH}/${firstSite}`;
        this.secondPath = `${ZK_TEST_CRR_STATE_PATH}/${secondSite}`;
    }

    getClient() {
        return this.zkClient;
    }

    get(site, cb) {
        const path = `${ZK_TEST_CRR_STATE_PATH}/${site}`;
        this.zkClient.getData(path, (err, data) => {
            if (err) {
                process.stdout.write('Zookeeper test helper error in ' +
                'ZKStateHelper.get at zkClient.getData');
                return cb(err);
            }
            let state;
            try {
                state = JSON.parse(data.toString());
            } catch (parseErr) {
                process.stdout.write('Zookeeper test helper error in ' +
                'ZKStateHelper.get at JSON.parse');
                return cb(parseErr);
            }
            return cb(null, state);
        });
    }

    set(site, state, cb) {
        const data = Buffer.from(state);
        const path = `${ZK_TEST_CRR_STATE_PATH}/${site}`;
        this.zkClient.setData(path, data, cb);
    }

    /**
     * Setup initial zookeeper state for pause/resume tests. After each test,
     * state should be reset to this initial state.
     * State is setup as such:
     *   - firstSite: { paused: false }
     *   - secondSite: { paused: true, scheduledResume: futureDate }
     * Where futureDate is defined at the top of this test file.
     * @param {function} cb - callback(err)
     * @return {undefined}
     */
    init(cb) {
        const { connectionString } = config.zookeeper;
        this.zkClient = zookeeper.createClient(connectionString);
        this.zkClient.connect();
        this.zkClient.once('connected', () => {
            async.series([
                next => this.zkClient.mkdirp(ZK_TEST_CRR_STATE_PATH, err => {
                    if (err && err.name !== 'NODE_EXISTS') {
                        return next(err);
                    }
                    return next();
                }),
                next => {
                    // emulate first site to be active (not paused)
                    const data =
                        Buffer.from(JSON.stringify({ paused: false }));
                    this.zkClient.create(this.firstPath, data, EPHEMERAL_NODE,
                        next);
                },
                next => {
                    // emulate second site to be paused
                    const data = Buffer.from(JSON.stringify({
                        paused: true,
                        scheduledResume: futureDate.toString(),
                    }));
                    this.zkClient.create(this.secondPath, data, EPHEMERAL_NODE,
                        next);
                },
            ], err => {
                if (err) {
                    process.stdout.write('Zookeeper test helper error in ' +
                    'ZKStateHelper.init');
                    return cb(err);
                }
                return cb();
            });
        });
    }

    reset(cb) {
        // reset state, just overwrite regardless of current state
        async.parallel([
            next => {
                const data = Buffer.from(JSON.stringify({
                    paused: false,
                    scheduledResume: null,
                }));
                this.zkClient.setData(this.firstPath, data, next);
            },
            next => {
                const data = Buffer.from(JSON.stringify({
                    paused: true,
                    scheduledResume: futureDate.toString(),
                }));
                this.zkClient.setData(this.secondPath, data, next);
            },
        ], err => {
            if (err) {
                process.stdout.write('Zookeeper test helper error in ' +
                'ZKStateHelper.reset');
                return cb(err);
            }
            return cb();
        });
    }

    close() {
        if (this.zkClient) {
            this.zkClient.close();
            this.zkClient = null;
        }
    }
}

class MockAPI {
    constructor() {
        this.publisher = new Redis();
    }

    _sendRequest(site, msg) {
        const channel = `${repConfig.topic}-${site}`;
        this.publisher.publish(channel, msg);
    }

    /**
     * mock a delete schedule resume call
     * @param {string} site - site name
     * @return {undefined}
     */
    deleteScheduledResumeService(site) {
        const message = JSON.stringify({
            action: 'deleteScheduledResumeService',
        });
        this._sendRequest(site, message);
    }

    /**
     * mock a resume api call
     * @param {string} site - site name
     * @param {Date} [date] - optional date object
     * @return {undefined}
     */
    resumeCRRService(site, date) {
        const message = {
            action: 'resumeService',
        };
        if (date) {
            message.date = date;
        }
        this._sendRequest(site, JSON.stringify(message));
    }

    /**
     * mock a pause api call
     * @param {string} site - site name
     * @return {undefined}
     */
    pauseCRRService(site) {
        const message = JSON.stringify({
            action: 'pauseService',
        });
        this._sendRequest(site, message);
    }
}

function isConsumerActive(consumer) {
    return consumer.getServiceStatus();
}

describe('CRR Pause/Resume status updates', function d() {
    this.timeout(10000);
    let zkHelper;
    let mockAPI;
    const firstSite = destConfig.bootstrapList[0].site;
    const secondSite = destConfig.bootstrapList[1].site;
    let qpSite1;
    let qpSite2;
    let consumer1;
    let consumer2;

    before(done => {
        mockAPI = new MockAPI();
        zkHelper = new ZKStateHelper(config.zookeeper, firstSite,
            secondSite);
        zkHelper.init(err => {
            if (err) {
                return done(err);
            }
            const zkClient = zkHelper.getClient();

            // qpSite1 (first site) is not paused
            qpSite1 = new QueueProcessor(zkClient, kafkaConfig, sourceConfig,
                destConfig, repConfig, redisConfig, mConfig, {}, {}, firstSite);
            qpSite1.start();

            // qpSite2 (second site) is paused and has a scheduled resume
            qpSite2 = new QueueProcessor(zkClient, kafkaConfig, sourceConfig,
                destConfig, repConfig, redisConfig, mConfig, {}, {},
                secondSite);
            qpSite2.start({ paused: true });
            qpSite2.scheduleResume({ paused: true }, futureDate);

            // wait for clients/jobs to set
            return async.whilst(() => (
                !consumer1 && !consumer2 && !qpSite2.scheduledResume
            ), cb => setTimeout(() => {
                consumer1 = qpSite1._consumer;
                consumer2 = qpSite2._consumer;
                return cb();
            }, 1000), done);
        });
    });

    afterEach(done => {
        consumer1.resume();
        consumer2.pause();
        async.whilst(() => qpSite1.scheduledResume !== null ||
            !qpSite2.scheduledResume,
        cb => setTimeout(() => {
            qpSite1._deleteScheduledResumeService();
            qpSite2.scheduleResume({ paused: true }, futureDate);
            cb();
        }, 1000), err => {
            assert.ifError(err);
            zkHelper.reset(done);
        });
    });

    after(() => {
        zkHelper.close();
    });

    it('should pause an active location', done => {
        let zkPauseState;
        // send fake api request
        mockAPI.pauseCRRService(firstSite);

        return async.doWhilst(cb => setTimeout(() => {
            zkHelper.get(firstSite, (err, data) => {
                if (err) {
                    return cb(err);
                }
                zkPauseState = data.paused;
                return cb();
            });
        }, 1000), () => isConsumerActive(consumer1),
        err => {
            assert.ifError(err);
            // is zookeeper state shown as paused?
            assert.strictEqual(zkPauseState, true);
            // is the consumer currently subscribed to any topics?
            assert.strictEqual(isConsumerActive(consumer1), false);
            return done();
        });
    });

    it('should not change state if pausing an already paused location',
    done => {
        let zkPauseState;
        let zkScheduleState;
        // double-check initial state
        assert.strictEqual(isConsumerActive(consumer2), false);
        // send fake api request
        mockAPI.pauseCRRService(secondSite);

        return async.doWhilst(cb => setTimeout(() => {
            zkHelper.get(secondSite, (err, data) => {
                if (err) {
                    return cb(err);
                }
                zkPauseState = data.paused;
                zkScheduleState = data.scheduledResume;
                return cb();
            });
        }, 1000), () => isConsumerActive(consumer2),
        err => {
            assert.ifError(err);
            assert.strictEqual(zkPauseState, true);
            assert.strictEqual(isConsumerActive(consumer2), false);
            assert.strictEqual(new Date(zkScheduleState).toString(),
                futureDate.toString());
            return done();
        });
    });

    it('should resume a paused location', done => {
        let zkPauseState;
        // send fake api request
        mockAPI.resumeCRRService(secondSite);

        return async.doWhilst(cb => setTimeout(() => {
            zkHelper.get(secondSite, (err, data) => {
                if (err) {
                    return cb(err);
                }
                zkPauseState = data.paused;
                return cb();
            });
        }, 1000), () => !isConsumerActive(consumer2),
        err => {
            assert.ifError(err);
            assert.strictEqual(zkPauseState, false);
            assert.strictEqual(isConsumerActive(consumer2), true);
            return done();
        });
    });

    it('should not change state if resuming an already active location',
    done => {
        let zkPauseState;
        // double-check initial state
        assert.strictEqual(isConsumerActive(consumer1), true);
        // send fake api request
        mockAPI.resumeCRRService(firstSite);
        return async.doWhilst(cb => setTimeout(() => {
            zkHelper.get(firstSite, (err, data) => {
                if (err) {
                    return cb(err);
                }
                zkPauseState = data.paused;
                return cb();
            });
        }, 1000), () => isConsumerActive(consumer1) !== true,
        err => {
            assert.ifError(err);
            assert.strictEqual(zkPauseState, false);
            assert.strictEqual(isConsumerActive(consumer1), true);
            return done();
        });
    });

    it('should cancel a scheduled resume for a given location', done => {
        let zkScheduleState;
        // send fake api request
        mockAPI.deleteScheduledResumeService(secondSite);

        return async.doWhilst(cb => setTimeout(() => {
            zkHelper.get(secondSite, (err, data) => {
                if (err) {
                    return cb(err);
                }
                zkScheduleState = data.scheduledResume;
                return cb();
            });
        }, 1000), () => zkScheduleState !== null,
        err => {
            assert.ifError(err);
            assert.strictEqual(zkScheduleState, null);
            return done();
        });
    });

    it('should schedule a resume', done => {
        let zkPauseState;
        let zkScheduleState;
        // since re-using firstSite, pause it first for this test
        const pauseState = JSON.stringify({ paused: true });
        zkHelper.set(firstSite, pauseState, err => {
            assert.ifError(err);
            consumer1.pause();
            // send fake api request
            mockAPI.resumeCRRService(firstSite, futureDate);

            return async.doWhilst(cb => setTimeout(() => {
                zkHelper.get(firstSite, (err, data) => {
                    if (err) {
                        return cb(err);
                    }
                    zkPauseState = data.paused;
                    zkScheduleState = data.scheduledResume;
                    return cb();
                });
            }, 1000), () => {
                if (zkScheduleState) {
                    const zkStateDate = new Date(zkScheduleState);
                    return zkStateDate.getTime() !== futureDate.getTime();
                }
                return true;
            }, err => {
                assert.ifError(err);
                assert.strictEqual(zkPauseState, true);
                assert.strictEqual(new Date(zkScheduleState).toString(),
                    futureDate.toString());
                done();
            });
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
            let zkScheduleState;
            const newScheduledDate = new Date(futureDate);
            newScheduledDate.setHours(newScheduledDate.getHours() +
                item.timeChange);
            // send fake api request
            mockAPI.resumeCRRService(secondSite, newScheduledDate);

            return async.doWhilst(cb => setTimeout(() => {
                zkHelper.get(secondSite, (err, data) => {
                    if (err) {
                        return cb(err);
                    }
                    zkScheduleState = data.scheduledResume;
                    return cb();
                });
            }, 1000), () => {
                if (zkScheduleState) {
                    const zkStateDate = new Date(zkScheduleState);
                    return zkStateDate.getTime() !== newScheduledDate.getTime();
                }
                return true;
            }, err => {
                assert.ifError(err);
                assert.strictEqual(new Date(zkScheduleState).toString(),
                    newScheduledDate.toString());
                done();
            });
        });
    });

    it('should cancel a scheduled resume when the location is manually ' +
    'resumed', done => {
        let zkScheduleState;
        let zkPauseState;
        // send fake api request
        mockAPI.resumeCRRService(secondSite);

        return async.doWhilst(cb => setTimeout(() => {
            zkHelper.get(secondSite, (err, data) => {
                if (err) {
                    return cb(err);
                }
                zkPauseState = data.paused;
                zkScheduleState = data.scheduledResume;
                return cb();
            });
        }, 1000), () => isConsumerActive(consumer2) !== true,
        err => {
            assert.ifError(err);
            assert.strictEqual(zkPauseState, false);
            assert.strictEqual(zkScheduleState, null);
            done();
        });
    });

    it('should not schedule a resume when the location is already active',
    done => {
        // send fake api request
        mockAPI.resumeCRRService(firstSite, futureDate);

        setTimeout(() => {
            zkHelper.get(firstSite, (err, data) => {
                assert.ifError(err);

                assert(!data.scheduledResume);
                assert.strictEqual(data.paused, false);
                assert.strictEqual(isConsumerActive(consumer1), true);
                done();
            });
        }, 1000);
    });

    it('should resume a location when a scheduled resume triggers',
    done => {
        let zkPauseState;
        let zkScheduleState;
        // move time forward to trigger scheduled resume
        const timeMachine = new TimeMachine();
        timeMachine.install();
        // hack - for some reason, the scheduled job in QueueProcessor does not
        // trigger unless I schedule a job here.
        schedule.scheduleJob(new Date(), () => {});
        timeMachine.moveTimeForward(6);

        return async.doWhilst(cb => setTimeout(() => {
            zkHelper.get(secondSite, (err, data) => {
                if (err) {
                    return cb(err);
                }
                zkPauseState = data.paused;
                zkScheduleState = data.scheduledResume;
                return cb();
            });
        }, 1000), () => isConsumerActive(consumer2) !== true,
        err => {
            timeMachine.uninstall();
            assert.ifError(err);
            assert.strictEqual(zkPauseState, false);
            assert.strictEqual(isConsumerActive(consumer2), true);
            assert.strictEqual(zkScheduleState, null);
            done();
        });
    });
});
