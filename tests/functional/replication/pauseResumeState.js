const assert = require('assert');
const async = require('async');
const schedule = require('node-schedule');

const QueueProcessor = require('../../../extensions/replication' +
                               '/queueProcessor/QueueProcessor');
const TimeMachine = require('../utils/timeMachine');
const ZKStateHelper = require('../utils/pauseResumeUtils/zkStateHelper');
const MockAPI = require('../utils/pauseResumeUtils/mockAPI');

// Configs
const config = require('../../config.json');
const {
    zookeeperNamespace,
    zkStatePath,
    zkReplayStatePath,
} = require('../../../extensions/replication/constants');
const redisConfig = {
    host: config.redis.host,
    port: config.redis.port,
};
const zkConfig = config.zookeeper;
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
const ZK_TEST_CRR_STATE_PATH = `${zookeeperNamespace}${zkStatePath}`;
const ZK_TEST_CRR_REPLAY_TOPIC = 'backbeat-func-replay-topic';
const ZK_TEST_CRR_REPLAY_STATE_PATH = `${zookeeperNamespace}${zkReplayStatePath}/${ZK_TEST_CRR_REPLAY_TOPIC}`;

// Future Date to be used in tests
const futureDate = new Date();
futureDate.setHours(futureDate.getHours() + 5);

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

    let replayConsumer1;
    let replayConsumer2;
    let replayProcessor1;
    let replayProcessor2;
    let zkReplayHelper;

    before(done => {
        mockAPI = new MockAPI(repConfig);
        zkHelper = new ZKStateHelper(config.zookeeper, ZK_TEST_CRR_STATE_PATH,
            firstSite, secondSite, futureDate);
        zkHelper.init(err => {
            if (err) {
                return done(err);
            }
            const zkClient = zkHelper.getClient();

            // qpSite1 (first site) is not paused
            qpSite1 = new QueueProcessor(
                'backbeat-func-test-dummy-topic', zkConfig, zkClient,
                kafkaConfig, sourceConfig, destConfig, repConfig,
                redisConfig, mConfig, {}, {}, firstSite);
            qpSite1.start();

            // qpSite2 (second site) is paused and has a scheduled resume
            qpSite2 = new QueueProcessor(
                'backbeat-func-test-dummy-topic', zkConfig, zkClient,
               kafkaConfig, sourceConfig, destConfig, repConfig,
               redisConfig, mConfig, {}, {}, secondSite);
            qpSite2.start({ paused: true });
            qpSite2.scheduleResume(futureDate);

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

    before(done => {
        zkReplayHelper = new ZKStateHelper(config.zookeeper, ZK_TEST_CRR_REPLAY_STATE_PATH,
            firstSite, secondSite, futureDate);
        zkReplayHelper.init(err => {
            if (err) {
                return done(err);
            }
            const zkClient = zkReplayHelper.getClient();
            const replayConfig = { ...repConfig };
            replayConfig.replayTopics = [
                { topicName: ZK_TEST_CRR_REPLAY_TOPIC, retries: 5 }
            ];
            // replay processor
            replayProcessor1 = new QueueProcessor(
                ZK_TEST_CRR_REPLAY_TOPIC, zkConfig, zkClient,
                kafkaConfig, sourceConfig, destConfig, replayConfig,
                redisConfig, mConfig, {}, {}, firstSite);
            replayProcessor1.start();

            // replay processor 2 (second site) is paused
            replayProcessor2 = new QueueProcessor(
                ZK_TEST_CRR_REPLAY_TOPIC, zkConfig, zkClient,
                kafkaConfig, sourceConfig, destConfig, replayConfig,
                redisConfig, mConfig, {}, {}, secondSite);
            replayProcessor2.start({ paused: true });
            replayProcessor2.scheduleResume(futureDate);

            // wait for clients/jobs to set
            return async.whilst(() => (
                !replayConsumer1 && !replayConsumer2 && !replayProcessor2.scheduledResume
            ), cb => setTimeout(() => {
                replayConsumer1 = replayProcessor1._consumer;
                replayConsumer2 = replayProcessor2._consumer;
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
            qpSite2.scheduleResume(futureDate);
            cb();
        }, 1000), err => {
            assert.ifError(err);
            zkHelper.reset(done);
        });
    });

    afterEach(done => {
        replayConsumer1.resume();
        replayConsumer2.pause();
        async.whilst(() => replayProcessor1.scheduledResume !== null ||
            !replayProcessor2.scheduledResume,
        cb => setTimeout(() => {
            replayProcessor1._deleteScheduledResumeService();
            replayProcessor2.scheduleResume(futureDate);
            cb();
        }, 1000), err => {
            assert.ifError(err);
            zkReplayHelper.reset(done);
        });
    });

    after(() => {
        zkHelper.close();
        zkReplayHelper.close();
    });

    it('should pause an active location', done => {
        let zkPauseState;
        // send fake api request
        mockAPI.pauseService(firstSite);

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

    it('should use a different path for replay processor', done => {
        assert.notStrictEqual(replayProcessor1._getZkSiteNode(), qpSite1._getZkSiteNode());
        return done();
    });

    it('should pause replay processor as well', done => {
        let zkPauseState;
        // send fake api request
        mockAPI.pauseService(firstSite);

        return async.doWhilst(cb => setTimeout(() => {
            zkReplayHelper.get(firstSite, (err, data) => {
                if (err) {
                    return cb(err);
                }
                zkPauseState = data.paused;
                return cb();
            });
        }, 1000), () => isConsumerActive(replayConsumer1),
        err => {
            assert.ifError(err);
            // is zookeeper state shown as paused?
            assert.strictEqual(zkPauseState, true);
            // is the consumer currently subscribed to any topics?
            assert.strictEqual(isConsumerActive(replayConsumer1), false);
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
        mockAPI.pauseService(secondSite);

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
        mockAPI.resumeService(secondSite);

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

    it('should resume replay processor as well', done => {
        let zkPauseState;
        // send fake api request
        mockAPI.resumeService(secondSite);

        return async.doWhilst(cb => setTimeout(() => {
            zkReplayHelper.get(secondSite, (err, data) => {
                if (err) {
                    return cb(err);
                }
                zkPauseState = data.paused;
                return cb();
            });
        }, 1000), () => !isConsumerActive(replayConsumer2),
        err => {
            assert.ifError(err);
            assert.strictEqual(zkPauseState, false);
            assert.strictEqual(isConsumerActive(replayConsumer2), true);
            return done();
        });
    });

    it('should not change state if resuming an already active location',
    done => {
        let zkPauseState;
        // double-check initial state
        assert.strictEqual(isConsumerActive(consumer1), true);
        // send fake api request
        mockAPI.resumeService(firstSite);
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
            mockAPI.resumeService(firstSite, futureDate);

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
            mockAPI.resumeService(secondSite, newScheduledDate);

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
        mockAPI.resumeService(secondSite);

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
        mockAPI.resumeService(firstSite, futureDate);

        setTimeout(() => {
            zkHelper.get(firstSite, (err, data) => {
                assert.ifError(err);

                assert.strictEqual(data.scheduledResume, null);
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
