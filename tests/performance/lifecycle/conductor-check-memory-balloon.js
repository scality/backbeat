const assert = require('assert');
const async = require('async');

const baseConfig = require('../../../lib/Config');
baseConfig.zookeeper.autoCreateNamespace = true;
const mongoConfig = baseConfig.queuePopulator.mongo;
const LifecycleConductor = require('../../../extensions/lifecycle/conductor/LifecycleConductor');
const { BackbeatMetadataProxyMock } = require('../../functional/lifecycle/utils');

const nBuckets = 200000;

describe('Lifecycle Conductor', function testBackpressure() {
    this.timeout(10 * 60 * 1000);

    describe('with bucketd', () => {
        const lcConfig = {
            ...baseConfig.extensions.lifecycle,
            auth: {
                type: '',
            },
            conductor: {
                cronRule: '12 12 12 12 12',
                concurrency: 10000,
                bucketSource: 'bucketd',
                bucketd: {
                    host: '127.0.0.1',
                    port: 9001,
                },
                backlogControl: {
                    enabled: false,
                },
                concurrentIndexesBuildLimit: 1,
            },
        };

        const lc = new LifecycleConductor(
            baseConfig.zookeeper,
            baseConfig.kafka,
            lcConfig,
            baseConfig.extensions.replication,
            baseConfig.s3,
        );

        it('should apply backpressure on bucket queue instead of ballooning', done => {
            lc.init(err => {
                if (err) {
                    return done(err);
                }

                return lc.processBuckets((err, nEnqueued) => {
                    if (err) {
                        return done(err);
                    }
                    return lc.stop(err => {
                        if (err) {
                            return done(err);
                        }

                        assert.deepStrictEqual(nEnqueued, nBuckets);
                        return done();
                    });
                });
            });
        });
    });

    describe('with mongodb', () => {
        const lcConfig = {
            ...baseConfig.extensions.lifecycle,
            auth: {
                type: '',
            },
            conductor: {
                cronRule: '12 12 12 12 12',
                concurrency: 10000,
                bucketSource: 'mongodb',
                mongodb: mongoConfig,
                backlogControl: {
                    enabled: false,
                },
                concurrentIndexesBuildLimit: 1,
            },
        };

        const zookeeperConfig = {
            ...baseConfig.zookeeper,
            autoCreateNamespace: true,
        };

        const lc = new LifecycleConductor(
            zookeeperConfig,
            baseConfig.kafka,
            lcConfig,
            baseConfig.extensions.replication,
            baseConfig.s3,
        );

        const injectAccounts = client => {
            const { database } = mongoConfig;

            const db = client.db(database);
            const c = db.collection('__metastore');
            const batchSize = 500;
            const bucketMD = {
                owner: 'a'.repeat(64),
            };

            return c.drop()
                .then(() => new Promise((resolve, reject) => {
                    async.timesLimit(
                        nBuckets / batchSize,
                        4,
                        (n, next) => {
                            const batch = [];
                            for (let i = 0; i < batchSize; i++) {
                                const id = n * batchSize + i;
                                const bucketName = ('0'.repeat(24) + id).slice(-24);
                                batch[i] = {
                                    _id: bucketName,
                                    value: bucketMD,
                                };
                            }
                            c.insertMany(batch)
                                .then(() => next())
                                .catch(next);
                        },
                        err => {
                            if (err) {
                                return reject(err);
                            }
                            return resolve();
                        }
                    );
                }));
        };

        it('should apply backpressure on bucket queue instead of ballooning', done => {
            lc.clientManager.getBackbeatMetadataProxy = () => new BackbeatMetadataProxyMock();
            lc.init(err => {
                if (err) {
                    return done(err);
                }

                return injectAccounts(lc._mongodbClient.client)
                    .then(() => {
                        lc.processBuckets((err, nEnqueued) => {
                            if (err) {
                                return done(err);
                            }
                            return lc.stop(err => {
                                if (err) {
                                    return done(err);
                                }

                                assert.deepStrictEqual(nEnqueued, nBuckets);
                                return done();
                            });

                        });
                    })
                    .catch(err => {
                        done(err);
                    });
            });
        });
    });
});
