'use strict'; // eslint-disable-line

const assert = require('assert');
const async = require('async');
const http = require('http');
const url = require('url');
const werelogs = require('werelogs');

const zookeeper = require('../../../lib/clients/zookeeper');
const BackbeatTestConsumer = require('../../utils/BackbeatTestConsumer');
const LifecycleConductor = require(
    '../../../extensions/lifecycle/conductor/LifecycleConductor');
const configValidator = require(
    '../../../extensions/lifecycle/LifecycleConfigValidator');

const zkConfig = {
    zookeeper: {
        connectionString: '127.0.0.1:2181',
        autoCreateNamespace: true,
    },
};
const kafkaConfig = {
    hosts: '127.0.0.1:9092',
    backlogMetrics: {
        zkPath: '/test/lifecycle/kafka-backlog-metrics',
        intervalS: 1,
    },
};
const repConfig = {
    dataMoverTopic: 'backbeat-data-mover-spec',
};

const s3Config = {
    host: '127.0.0.1',
    port: 8000,
};

const bucketTasksTopic = 'backbeat-lifecycle-bucket-tasks-spec';

const expected2Messages = [
    {
        value: {
            action: 'processObjects',
            contextInfo: { reqId: 'test-request-id' },
            target: { bucket: 'bucket1', owner: 'owner1', taskVersion: 'v1' },
            details: {},
        },
    },
    {
        value: {
            action: 'processObjects',
            contextInfo: { reqId: 'test-request-id' },
            target: { bucket: 'bucket1-2', owner: 'owner1', taskVersion: 'v1' },
            details: {},
        },
    },
];

const expected4Messages = [
    {
        value: {
            action: 'processObjects',
            contextInfo: { reqId: 'test-request-id' },
            target: { bucket: 'bucket1', owner: 'owner1', taskVersion: 'v1' },
            details: {},
        },
    },
    {
        value: {
            action: 'processObjects',
            contextInfo: { reqId: 'test-request-id' },
            target: { bucket: 'bucket1-2', owner: 'owner1', taskVersion: 'v1' },
            details: {},
        },
    },
    {
        value: {
            action: 'processObjects',
            contextInfo: { reqId: 'test-request-id' },
            target: { bucket: 'bucket3', owner: 'owner3', taskVersion: 'v1' },
            details: {},
        },
    },
    {
        value: {
            action: 'processObjects',
            contextInfo: { reqId: 'test-request-id' },
            target: { bucket: 'bucket4', owner: 'owner4', taskVersion: 'v1' },
            details: {},
        },
    },
];

const baseLCConfig = {
    zookeeperPath: '/test/lifecycle',
    bucketTasksTopic,
    objectTasksTopic: 'backbeat-lifecycle-object-tasks-spec',
    transitionTasksTopic: 'backbeat-lifecycle-transition-tasks-spec',
    conductor: {
        cronRule: '*/5 * * * * *',
        backlogControl: {
            enabled: false,
        },
        probeServer: {
            port: 8552,
        },
        concurrentIndexesBuildLimit: 10,
    },
    auth: {
        type: 'account',
        account: 'lifecycle',
    },
    bucketProcessor: {
        groupId: 'a',
        probeServer: {
            port: 8553,
        }
    },
    coldStorageArchiveTopicPrefix: 'cold-archive-req-',
};

function withAccountIds(messages) {
    return messages.map(m => ({
        value: {
            ...m.value,
            target: {
                ...m.value.target,
                accountId: m.value.target.owner.replace('owner', 'account'),
            },
        },
    }));
}

const identity = _ => _;

const TIMEOUT = 120000;
const CONSUMER_TIMEOUT = 60000;

werelogs.configure({ level: 'info', dump: 'error' });

describe('lifecycle conductor', function lifecycleConductor() {
    this.timeout(TIMEOUT);

    describe('backlog control', () => {
        const bucketdPort = 14344;
        let bucketd;

        const bucketdHandler = (_, res) => {
            setTimeout(
                () => {
                    res.end(JSON.stringify({
                        Contents: [],
                        IsTruncated: false,
                    }));
                },
                2000);
        };

        beforeEach(done => {
            bucketd = http.createServer(bucketdHandler);
            bucketd.listen(bucketdPort, done);
        });

        afterEach(done => {
            bucketd.close(done);
        });

        it('should detect ongoing batches', done => {
            const lcConfig = {
                ...baseLCConfig,
                conductor: {
                    ...baseLCConfig.conductor,
                    cronRule: '*/5 */5 */5 */5 */5 */5',
                    bucketSource: 'bucketd',
                    bucketd: {
                        host: 'localhost',
                        port: bucketdPort,
                    },
                    backlogControl: {
                        enabled: true,
                    },
                },
                bucketProcessor: {
                    groupId: 'a',
                },
                objectProcessor: {
                    groupId: 'b',
                },
                transitionProcessor: {
                    groupId: 'c',
                },
            };

            // make topic unique so that different tests' bootstrap messages don't interfere
            lcConfig.bucketTasksTopic += Math.random();

            const localKafkaConfig = {
                ...kafkaConfig,
                backlogMetrics: {
                    zkPath: '/backbeat/run/kafka-backlog-metrics',
                    intervalS: 60,
                },
            };

            const lc = new LifecycleConductor(zkConfig.zookeeper,
                localKafkaConfig, lcConfig, repConfig, s3Config);

            async.series([
                next => lc.start(next),
                next => async.parallel([
                    nextp => lc.processBuckets(nextp),
                    nextp => setTimeout(() => {
                        lc.processBuckets(err => {
                            // test explicitly for the non-backlog-metrics related error
                            if (err && err.Throttling && err.description === 'Batch in progress') {
                                return nextp();
                            }

                            const e = new Error('should have returned a `Throttling` error');
                            return nextp(e);
                        });
                    }, 500),
                    nextp => setTimeout(() => {
                        lc.processBuckets(err => {
                            // test explicitly for the non-backlog-metrics related error
                            if (err && err.Throttling && err.description === 'Batch in progress') {
                                return nextp();
                            }

                            const e = new Error('should have returned a `Throttling` error');
                            return nextp(e);
                        });
                    }, 1000),
                ], next),
                next => lc.stop(next),
            ],
            done);
        });
    });

    describe('bucketd listing', () => {
        const bucketdPort = 14344;
        let bucketd;
        let called = false;

        const bucketdHandler = (_, res) => {
            setTimeout(
                () => {
                    if (!called) {
                        called = true;
                        res.statusCode = 500; // eslint-disable-line no-param-reassign
                        return res.end();
                    }
                    return res.end(JSON.stringify({
                        Contents: [],
                        IsTruncated: false,
                    }));
                },
                2000);
        };

        beforeEach(done => {
            bucketd = http.createServer(bucketdHandler);
            bucketd.listen(bucketdPort, done);
        });

        afterEach(done => {
            bucketd.close(done);
        });

        it('should retry on bucketd errors', done => {
            const lcConfig = {
                ...baseLCConfig,
                conductor: {
                    ...baseLCConfig.conductor,
                    cronRule: '*/5 */5 */5 */5 */5 */5',
                    bucketSource: 'bucketd',
                    bucketd: {
                        host: 'localhost',
                        port: bucketdPort,
                    },
                    backlogControl: {
                        enabled: true,
                    },
                },
                bucketProcessor: {
                    groupId: 'a',
                },
                objectProcessor: {
                    groupId: 'b',
                },
                transitionProcessor: {
                    groupId: 'c',
                },
            };

            // make topic unique so that different tests' bootstrap messages don't interfere
            lcConfig.bucketTasksTopic += Math.random();

            const localKafkaConfig = {
                ...kafkaConfig,
                backlogMetrics: {
                    zkPath: '/backbeat/run/kafka-backlog-metrics',
                    intervalS: 60,
                },
            };

            const lc = new LifecycleConductor(zkConfig.zookeeper,
                localKafkaConfig, lcConfig, repConfig, s3Config);

            async.series([
                next => lc.start(next),
                next => lc.processBuckets(next),
                next => lc.stop(next),
            ],
            done);
        });
    });

    function describeConductorSpec(opts) {
        const {
            description,
            lifecycleConfig,
            transformExpectedMessages,
            mockBucketd,
            mockVault,
            setupZookeeper,
            skip,
        } = opts;

        if (skip) {
            return describe.skip(`skipped: ${description} ${skip}`, () => {
            });
        }

        const bucketdPort = 14345;
        const vaultPort = 14346;
        const maxKeys = 2;

        let vault;
        let bucketd;
        let bucketdListing;
        let zkClient;
        let bucketPopulatorStep1;
        let bucketPopulatorStep2;
        let consumer;
        let lcConductor;

        const vaultHandler = (req, res) => {
            const { pathname, query } = url.parse(req.url, true);

            assert.strictEqual(pathname, '/');
            assert.strictEqual(req.method, 'GET');
            assert.strictEqual(query.Action, 'GetAccounts');

            const canonicalIds = Array.isArray(query.canonicalIds) ?
                query.canonicalIds :
                [query.canonicalIds];
            const accountIds = canonicalIds.map(v => ({
                id: v.replace('owner', 'account'),
                canId: v,
            }));

            res.end(JSON.stringify(accountIds));
        };

        const bucketdHandler = (req, res) => {
            const { pathname, query } = url.parse(req.url, true);

            assert.strictEqual(pathname, '/default/bucket/users..bucket');
            assert.strictEqual(req.method, 'GET');
            assert.strictEqual(query.prefix, '');
            assert.strictEqual(query.maxKeys, `${maxKeys}`);

            const thisListing = [...bucketdListing].splice(0, query.maxKeys);
            bucketdListing = [...bucketdListing].splice(query.maxKeys);

            res.end(JSON.stringify({
                Contents: thisListing.map(key => ({
                    key,
                    value: {},
                })),
                IsTruncated: !!bucketdListing.length,
            }));
        };

        if (mockBucketd) {
            lifecycleConfig.conductor.bucketd.port = bucketdPort;

            bucketPopulatorStep1 = next => {
                bucketdListing.push('owner1..|..bucket1', 'owner1..|..bucket1-2');
                process.nextTick(next);
            };

            bucketPopulatorStep2 = next => {
                bucketdListing.push('owner1..|..bucket1', 'owner1..|..bucket1-2');
                bucketdListing.push('owner3..|..bucket3', 'owner4..|..bucket4');
                process.nextTick(next);
            };
        }

        if (mockVault) {
            lifecycleConfig.auth.vault.port = vaultPort;
        }

        lifecycleConfig.conductor.concurrency = maxKeys;
        // make topic unique so that different tests' bootstrap messages don't interfere
        lifecycleConfig.bucketTasksTopic += Math.random();

        const validatedLifecycleConfig = configValidator(null, lifecycleConfig);

        if (setupZookeeper) {
            assert.ok(!mockBucketd);

            bucketPopulatorStep1 = next => {
                async.each(
                    ['owner1:uid1:bucket1', 'owner1:uid1-2:bucket1-2'],
                    (bucket, done) => zkClient.create(
                        `${validatedLifecycleConfig.zookeeperPath}/data/buckets/${bucket}`, done),
                    next);
            };

            bucketPopulatorStep2 = next => {
                async.each(
                    ['owner3:uid3:bucket3', 'owner4:uid4:bucket4'],
                    (bucket, done) => zkClient.create(
                        `${validatedLifecycleConfig.zookeeperPath}/data/buckets/${bucket}`, done),
                    next);
            };
        }

        return describe(description, () => {
            beforeEach(done => {
                bucketdListing = [];

                lcConductor = new LifecycleConductor(zkConfig.zookeeper,
                    kafkaConfig, validatedLifecycleConfig, repConfig, s3Config);

                async.series([
                    next => lcConductor.init(next),
                    next => {
                        consumer = new BackbeatTestConsumer({
                            kafka: { hosts: kafkaConfig.hosts },
                            topic: validatedLifecycleConfig.bucketTasksTopic,
                            groupId: 'test-consumer-group',
                        });
                        consumer.on('ready', next);
                    },
                    next => {
                        consumer.subscribe();
                        // it seems the consumer needs some extra time to
                        // start consuming the first messages
                        setTimeout(next, 2000);
                    },
                    next => {
                        if (mockBucketd) {
                            bucketd = http.createServer(bucketdHandler);
                            bucketd.listen(bucketdPort, next);
                        } else {
                            process.nextTick(next);
                        }
                    },
                    next => {
                        if (mockVault) {
                            vault = http.createServer(vaultHandler);
                            vault.listen(vaultPort, next);
                        } else {
                            process.nextTick(next);
                        }
                    },
                    next => {
                        if (setupZookeeper) {
                            zkClient = zookeeper.createClient(
                                zkConfig.zookeeper.connectionString,
                                zkConfig.zookeeper);
                            zkClient.connect();
                            zkClient.once('ready', () => {
                                lcConductor.initZkPaths(next);
                            });
                        } else {
                            process.nextTick(next);
                        }
                    },
                ], done);
            });

            afterEach(done => {
                async.series([
                    next => {
                        if (mockBucketd) {
                            bucketd.close(next);
                        } else {
                            process.nextTick(next);
                        }
                    },
                    next => {
                        if (mockVault) {
                            vault.close(next);
                        } else {
                            process.nextTick(next);
                        }
                    },
                    next => {
                        if (setupZookeeper) {
                            zkClient.removeRecur(validatedLifecycleConfig.zookeeperPath, next);
                        } else {
                            process.nextTick(next);
                        }
                    },
                    next => consumer.close(next),
                    next => lcConductor.stop(next),
                ], done);
            });

            it('should populate queue', done => {
                async.waterfall([
                    bucketPopulatorStep1,
                    next => {
                        lcConductor.processBuckets();
                        consumer.expectUnorderedMessages(transformExpectedMessages(expected2Messages),
                            CONSUMER_TIMEOUT, next);
                    },
                    bucketPopulatorStep2,
                    next => {
                        lcConductor.processBuckets();
                        consumer.expectUnorderedMessages(transformExpectedMessages(expected4Messages),
                            CONSUMER_TIMEOUT, next);
                    },
                ], err => {
                    assert.ifError(err);
                    done();
                });
            });
        });
    }

    describeConductorSpec({
        description: 'with auth `account` and buckets from bucketd',
        lifecycleConfig: {
            ...baseLCConfig,
            conductor: {
                ...baseLCConfig.conductor,
                bucketSource: 'bucketd',
                bucketd: {
                    host: '127.0.0.1',
                },
            },
        },
        mockBucketd: true,
        transformExpectedMessages: identity,
    });

    describeConductorSpec({
        description: 'with auth `account` and buckets from zookeeper (compat mode)',
        lifecycleConfig: baseLCConfig,
        setupZookeeper: true,
        transformExpectedMessages: identity,
    });

    describeConductorSpec({
        description: 'with auth `assumeRole` and buckets from bucketd',
        lifecycleConfig: {
            ...baseLCConfig,
            conductor: {
                ...baseLCConfig.conductor,
                bucketSource: 'bucketd',
                bucketd: {
                    host: '127.0.0.1',
                },
            },
            auth: {
                type: 'assumeRole',
                roleName: 'lc',
                sts: {
                    host: '127.0.0.1',
                    port: 8650,
                    accessKey: 'ak',
                    secretKey: 'sk',
                },
                vault: {
                    host: '127.0.0.1',
                },
            },
        },
        mockBucketd: true,
        mockVault: true,
        transformExpectedMessages: withAccountIds,
        skip: 'to be reintroduced with https://scality.atlassian.net/browse/BB-126',
    });
});
