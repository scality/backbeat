'use strict';

const assert = require('assert');
const async = require('async');
const http = require('http');
const { MongoClient } = require('mongodb');
const querystring = require('querystring');
const url = require('url');
const { promisify } = require('util');
const werelogs = require('werelogs');

const { indexesForFeature } = require('../../../lib/constants');
const { mongoConfig } = require('./configObjects');
const ZookeeperManager = require('../../../lib/clients/ZookeeperManager');
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

const mongoUrl =
    `mongodb://${mongoConfig.replicaSetHosts}` +
    `/db?replicaSet=${mongoConfig.replicaSet}`;

const bucketTasksTopic = 'backbeat-lifecycle-bucket-tasks-spec';

const expected2Messages = (version='v2') => ([
    {
        value: {
            action: 'processObjects',
            contextInfo: {
                reqId: 'test-request-id',
                conductorScanId: 'test-scan-id',
                conductorScanStartTimestamp: 0,
            },
            target: { bucket: 'bucket1', owner: 'owner1', taskVersion: version },
            details: {},
        },
    },
    {
        value: {
            action: 'processObjects',
            contextInfo: {
                reqId: 'test-request-id',
                conductorScanId: 'test-scan-id',
                conductorScanStartTimestamp: 0,
            },
            target: { bucket: 'bucket1-2', owner: 'owner1', taskVersion: version },
            details: {},
        },
    },
]);

const expected4Messages = (version='v2') => ([
    {
        value: {
            action: 'processObjects',
            contextInfo: {
                reqId: 'test-request-id',
                conductorScanId: 'test-scan-id',
                conductorScanStartTimestamp: 0,
            },
            target: { bucket: 'bucket1', owner: 'owner1', taskVersion: version },
            details: {},
        },
    },
    {
        value: {
            action: 'processObjects',
            contextInfo: {
                reqId: 'test-request-id',
                conductorScanId: 'test-scan-id',
                conductorScanStartTimestamp: 0,
            },
            target: { bucket: 'bucket1-2', owner: 'owner1', taskVersion: version },
            details: {},
        },
    },
    {
        value: {
            action: 'processObjects',
            contextInfo: {
                reqId: 'test-request-id',
                conductorScanId: 'test-scan-id',
                conductorScanStartTimestamp: 0,
            },
            target: { bucket: 'bucket3', owner: 'owner3', taskVersion: version },
            details: {},
        },
    },
    {
        value: {
            action: 'processObjects',
            contextInfo: {
                reqId: 'test-request-id',
                conductorScanId: 'test-scan-id',
                conductorScanStartTimestamp: 0,
            },
            target: { bucket: 'bucket4', owner: 'owner4', taskVersion: version },
            details: {},
        },
    },
]);

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
const log = new werelogs.Logger('LifecycleConductor:test');

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
            useMongodb,
        } = opts;

        const bucketdPort = 14345;
        const vaultPort = 14346;
        const stsPort = 14347;
        const backbeatPort = 14348;
        const maxKeys = 2;
        // assuming a role goes through STS before vault can be reached
        const mockSts = lifecycleConfig.auth.type === 'assumeRole';
        // v2 listing is only granted once the bucket carries the lifecycle
        // indexes, which the conductor looks up over the backbeat API
        const mockBackbeat = useMongodb && !lifecycleConfig.forceLegacyListing;
        // mongodb keeps its listing checkpoints in zookeeper
        const needsZkPaths = setupZookeeper || useMongodb;

        let sts;
        let vault;
        let backbeat;
        let mongoClient;
        let bucketd;
        let bucketdListing;
        let zkClient;
        let bucketPopulatorStep1;
        let bucketPopulatorStep2;
        let consumer;
        let lcConductor;

        const stsHandler = (req, res) => {
            req.resume();
            const expiration = new Date(Date.now() + 3600000).toISOString();

            res.writeHead(200, { 'Content-Type': 'text/xml' });
            res.end(`<?xml version="1.0" encoding="UTF-8"?>
<AssumeRoleResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
  <AssumeRoleResult>
    <Credentials>
      <AccessKeyId>accessKey</AccessKeyId>
      <SecretAccessKey>secretKey</SecretAccessKey>
      <SessionToken>sessionToken</SessionToken>
      <Expiration>${expiration}</Expiration>
    </Credentials>
    <AssumedRoleUser>
      <Arn>arn:aws:sts::000000000000:assumed-role/lc/lc</Arn>
      <AssumedRoleId>ASSUMEDROLEID:lc</AssumedRoleId>
    </AssumedRoleUser>
  </AssumeRoleResult>
</AssumeRoleResponse>`);
        };

        const vaultHandler = (req, res) => {
            const { pathname, query } = url.parse(req.url, true);

            assert.strictEqual(pathname, '/');

            const respond = params => {
                assert.strictEqual(params.Action, 'GetAccounts');

                const canonicalIds = Array.isArray(params.canonicalIds) ?
                    params.canonicalIds :
                    [params.canonicalIds];
                const accountIds = canonicalIds.map(v => ({
                    id: v.replace('owner', 'account'),
                    canId: v,
                }));

                res.end(JSON.stringify(accountIds));
            };

            // admin routes are queried over POST once IAM authentication is on
            if (req.method === 'POST') {
                const chunks = [];
                req.on('data', chunk => chunks.push(chunk));
                req.on('end', () => respond(
                    querystring.parse(Buffer.concat(chunks).toString())));
                return;
            }

            assert.strictEqual(req.method, 'GET');
            respond(query);
        };

        // buckets are reported as already indexed, so listing stays on v2
        const backbeatHandler = (req, res) => {
            const { pathname } = url.parse(req.url, true);

            assert.strictEqual(req.method, 'GET');
            assert.ok(pathname.startsWith('/_/backbeat/index/'), pathname);

            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ Indexes: indexesForFeature.lifecycle.v2 }));
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

        if (useMongodb) {
            lifecycleConfig.conductor.mongodb = mongoConfig;

            const insertBuckets = buckets => mongoClient
                .db(mongoConfig.database)
                .collection('__metastore')
                .insertMany(buckets.map(([bucketName, owner]) => ({
                    _id: bucketName,
                    value: {
                        owner,
                        lifecycleConfiguration: { rules: [] },
                    },
                })));

            bucketPopulatorStep1 = async () => insertBuckets(
                [['bucket1', 'owner1'], ['bucket1-2', 'owner1']]);

            bucketPopulatorStep2 = async () => insertBuckets(
                [['bucket3', 'owner3'], ['bucket4', 'owner4']]);
        }

        if (mockVault) {
            lifecycleConfig.auth.vault.port = vaultPort;
        }

        if (mockSts) {
            lifecycleConfig.auth.sts.port = stsPort;
        }

        // the conductor reaches the backbeat index routes through the S3 endpoint
        const conductorS3Config = mockBackbeat ?
            { ...s3Config, port: backbeatPort } :
            s3Config;

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

        const expectedTaskVersion = validatedLifecycleConfig.forceLegacyListing ? 'v1' : 'v2';

        return describe(description, () => {
            beforeEach(done => {
                bucketdListing = [];

                lcConductor = new LifecycleConductor(zkConfig.zookeeper,
                    kafkaConfig, validatedLifecycleConfig, repConfig, conductorS3Config);

                async.series([
                    async () => {
                        if (mockSts) {
                            sts = http.createServer(stsHandler);
                            await promisify(cb => sts.listen(stsPort, cb))();
                        }
                    },
                    async () => {
                        if (mockBackbeat) {
                            backbeat = http.createServer(backbeatHandler);
                            await promisify(cb => backbeat.listen(backbeatPort, cb))();
                        }
                    },
                    async () => {
                        if (useMongodb) {
                            mongoClient = new MongoClient(mongoUrl);
                            await mongoClient.connect();
                        }
                    },
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
                        if (needsZkPaths) {
                            zkClient = new ZookeeperManager(
                                zkConfig.zookeeper.connectionString,
                                zkConfig.zookeeper,
                                log
                            );
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
                    async () => {
                        if (mockSts) {
                            await promisify(cb => sts.close(cb))();
                        }
                    },
                    async () => {
                        if (mockBackbeat) {
                            await promisify(cb => backbeat.close(cb))();
                        }
                    },
                    async () => {
                        if (useMongodb) {
                            await mongoClient.db(mongoConfig.database)
                                .collection('__metastore')
                                .deleteMany({});
                            await mongoClient.close();
                        }
                    },
                    next => {
                        if (needsZkPaths) {
                            zkClient.removeRecur(validatedLifecycleConfig.zookeeperPath, next);
                        } else {
                            process.nextTick(next);
                        }
                    },
                    next => consumer.close(next),
                    next => lcConductor.stop(next),
                ], done);
            });

            // scans share the conductor's scan id, so letting one start before
            // the previous has completed makes them clobber each other
            const runBatch = (expectedMessages, next) => async.parallel([
                cb => consumer.expectUnorderedMessages(
                    transformExpectedMessages(expectedMessages),
                    CONSUMER_TIMEOUT, cb),
                cb => lcConductor.processBuckets(cb),
            ], err => next(err));

            it('should populate queue', done => {
                // series, not waterfall: no step feeds the next one, and
                // waterfall would pass an async step's result on as the
                // following step's callback
                async.series([
                    bucketPopulatorStep1,
                    next => runBatch(expected2Messages(expectedTaskVersion), next),
                    bucketPopulatorStep2,
                    next => runBatch(expected4Messages(expectedTaskVersion), next),
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
        description: 'with auth `account` and buckets from bucketd (legacy listing mode)',
        lifecycleConfig: {
            ...baseLCConfig,
            forceLegacyListing: true,
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
        description: 'with auth `account` and buckets from zookeeper (legacy listing mode)',
        lifecycleConfig: {
            ...baseLCConfig,
            forceLegacyListing: true,
        },
        setupZookeeper: true,
        transformExpectedMessages: identity,
    });

    // `assumeRole` is only used on mongodb deployments, S3C does not support STS
    const assumeRoleConfig = {
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
    };

    describeConductorSpec({
        description: 'with auth `assumeRole` and buckets from mongodb',
        lifecycleConfig: {
            ...baseLCConfig,
            conductor: {
                ...baseLCConfig.conductor,
                bucketSource: 'mongodb',
            },
            auth: assumeRoleConfig,
        },
        useMongodb: true,
        mockVault: true,
        transformExpectedMessages: withAccountIds,
    });

    describeConductorSpec({
        description: 'with auth `assumeRole` and buckets from mongodb (legacy listing mode)',
        lifecycleConfig: {
            ...baseLCConfig,
            forceLegacyListing: true,
            conductor: {
                ...baseLCConfig.conductor,
                bucketSource: 'mongodb',
            },
            auth: assumeRoleConfig,
        },
        useMongodb: true,
        mockVault: true,
        transformExpectedMessages: withAccountIds,
    });
});
