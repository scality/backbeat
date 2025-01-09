const assert = require('assert');
const async = require('async');
const http = require('http');
const kafka = require('node-rdkafka');
const { MetadataMock, mockLogs } = require('../utils/MetadataMock');
const MongoClient = require('mongodb').MongoClient;
const { promisify } = require('util');
const timers  = require('timers/promises');

const dummyLogger = require('../../utils/DummyLogger');
const dummyPensieveCredentials = require('./DummyPensieveCredentials.json');
const dummySSHKey = require('./DummySSHKey.json');
const { expectedNewIngestionEntry, expectedZeroByteObj, expectedUTF8Obj,
    expectedVersionIdObj, expectedTagsObj } = require('./expectedEntries');
const IngestionQueuePopulator =
    require('../../../extensions/ingestion/IngestionQueuePopulator');
const IngestionReader = require('../../../lib/queuePopulator/IngestionReader');
const { initManagement } = require('../../../lib/management/index');
const testConfig = require('../../config.json');
const { setupS3Mock, emptyAndDeleteVersionedBucket } = require('./S3Mock');
const ZookeeperManager = require('../../../lib/clients/ZookeeperManager');
const BackbeatProducer = require('../../../lib/BackbeatProducer');

const testPort = testConfig.extensions.ingestion.sources[0].port;
const mockLogOffset = 1;
const CONSUMER_TIMEOUT = 35000;

const expectedLogs = JSON.parse(JSON.stringify(mockLogs));
const expectedOOBEntries = [];
const oobEntries = expectedLogs.log.filter(entry =>
    entry.db === testConfig.extensions.ingestion.sources[0].bucket &&
    entry.method !== 0 && entry.method !== 7);
oobEntries.forEach(bucketEntry => {
    bucketEntry.entries.forEach(entry => {
        expectedOOBEntries.push(JSON.stringify(entry.value));
    });
});

const Kafka = require('node-rdkafka');

const kafkaAdminClient = Kafka.AdminClient.create({
    'client.id': 'kafka-admin',
    'metadata.broker.list': [testConfig.kafka.hosts]
});


const ingestionQP = new IngestionQueuePopulator({
    config: testConfig.extensions.ingestion,
    logger: dummyLogger,
});
const consumerParams = {
    'metadata.broker.list': [testConfig.kafka.hosts],
    'group.id': 'test-consumer-group-ingestion',
    // we manage stored offsets based on the highest
    // contiguous offset fully processed by a worker, so
    // disabling automatic offset store is needed
    'enable.auto.offset.store': false,
};
const consumer = new kafka.KafkaConsumer(consumerParams, {});

function setZookeeperInitState(ingestionReader, zkClient, cb) {
    const path = `${ingestionReader.bucketInitPath}/isStatusComplete`;
    async.series([
        next => zkClient.mkdirp(path, next),
        next => zkClient.setData(path, Buffer.from('true'),
            -1, next),
    ], cb);
}

function checkEntryInQueue(kafkaEntries, expectedEntries, done) {
    // 2 entries per object, but the master key is filtered
    assert.strictEqual(kafkaEntries.length, expectedEntries.length);

    const retrievedEntries = kafkaEntries.map(entry => JSON.parse(entry.value));

    expectedEntries.forEach(entry => {
        const entryValue = JSON.parse(entry.value);

        // for tests, one as master, one w/ version
        const matchedKafkaEntries = retrievedEntries.filter(e =>
            e.key.startsWith(entry.key));

        matchedKafkaEntries.forEach(kafkaEntry => {
            const kafkaValue = JSON.parse(kafkaEntry.value);
            assert.strictEqual(entry.type, kafkaEntry.type);
            assert.strictEqual(entry.bucket, kafkaEntry.bucket);

            Object.keys(entryValue).forEach(key => {
                if (typeof entryValue[key] === 'object') {
                    assert.strictEqual(JSON.stringify(entryValue[key]),
                        JSON.stringify(kafkaValue[key]));
                } else if (key !== 'md-model-version') {
                    // ignore model version, but compare all other fields
                    assert.strictEqual(entryValue[key], kafkaValue[key]);
                }
            });
        });
    });
    return done();
}

describe('ingestion reader tests with mock', function fD() {
    this.timeout(40000);
    let httpServer;
    let producer;
    let zkClient;
    const mongoUrl =
    `mongodb://${testConfig.queuePopulator.mongo.replicaSetHosts}` +
    '/db?replicaSet=rs0';
    const client = new MongoClient(mongoUrl, {});
    const db = client.db('metadata', { ignoreUndefined: true });
    before(async () => {
        testConfig.s3.port = testPort;
        const topic = testConfig.extensions.ingestion.topic;
        await client.connect();
        try {
            const createTopic = promisify(kafkaAdminClient.createTopic).bind(kafkaAdminClient);
            await createTopic({
                topic,
                num_partitions: 1, // eslint-disable-line camelcase
                replication_factor: 1, // eslint-disable-line camelcase
            });
        } catch (err) {
            if (err.code !== 36) { // if topic does not already exist
                throw err;
            }
        }
        producer = new BackbeatProducer({
            kafka: testConfig.kafka,
            topic: testConfig.extensions.ingestion.topic,
        });
        await new Promise(resolve => producer.once('ready', resolve));
        await new Promise(resolve => {
            consumer.connect({ timeout: 1000 }, () => {});
            consumer.once('ready', resolve);
        });

        consumer.subscribe([testConfig.extensions.ingestion.topic]);
        await timers.setTimeout(2000);
        await db.createCollection('PENSIEVE');
        const collection = db.collection('PENSIEVE');
        await collection.insertOne(dummyPensieveCredentials);
        await collection.insertOne({
            _id: 'configuration/overlay-version',
            value: 6,
        });
        await collection.insertOne(dummySSHKey);
        zkClient = new ZookeeperManager('localhost:2181', { autoCreateNamespace: true }, dummyLogger);
        await new Promise((resolve, reject) => {
            zkClient.once('error', reject);
            zkClient.once('ready', resolve);
        });
        await promisify(initManagement)(testConfig);
        const metadataMock = new MetadataMock();
        httpServer = http.createServer((req, res) => metadataMock.onRequest(req, res))
            .listen(testPort);
    });

    after(async () =>  {
        await promisify(httpServer.close.bind(httpServer))();
        consumer.unsubscribe();
        await db.collection('PENSIEVE').drop();
        await client.close();
    });

    describe('testing with `bucket1` configuration', () => {
        let batchState;
        const sourceConfig = testConfig.extensions.ingestion.sources[0];

        beforeEach(done => {
            batchState = {
                logRes: null,
                logStats: {
                    nbLogRecordsRead: 0,
                    nbLogEntriesRead: 0,
                    hasMoreLog: false,
                },
                entriesToPublish: {},
                publishedEntries: {},
                maxRead: 10000,
                startTime: Date.now(),
                timeoutMs: 1000,
                logger: dummyLogger,
            };
            this.ingestionReader = new IngestionReader({
                zkClient,
                ingestionConfig: testConfig.extensions.ingestion,
                kafkaConfig: testConfig.kafka,
                bucketdConfig: testConfig.extensions.ingestion.sources[0],
                qpConfig: testConfig.queuePopulator,
                logger: dummyLogger,
                extensions: [ingestionQP],
                metricsProducer: { publishMetrics: () => { } },
                s3Config: testConfig.s3,
                producer,
            });
            this.ingestionReader.setup(() => {
                async.series([
                    next => setZookeeperInitState(this.ingestionReader, zkClient, next),
                    next => zkClient.setData(
                        this.ingestionReader.pathToLogOffset,
                        Buffer.from(mockLogOffset.toString()), -1, err => {
                            assert.ifError(err);
                            return next(err);
                        }
                    ),
                    next => setupS3Mock(sourceConfig, next),
                ], err => {
                    assert.ifError(err);
                    return done();
                });
            });
        });

        afterEach(done => {
            async.series([
                next => emptyAndDeleteVersionedBucket(sourceConfig, next),
                next => zkClient.remove(this.ingestionReader.pathToLogOffset, -1, next),
            ], done);
        });

        it('_processReadRecords should retrieve logRes stream', done => {
            assert.strictEqual(batchState.logRes, null);
            return this.ingestionReader._processReadRecords({}, batchState,
                err => {
                    assert.ifError(err);
                    assert.deepStrictEqual(batchState.logRes.info,
                        { start: 1, cseq: 8, prune: 1 });
                    const receivedLogs = [];
                    batchState.logRes.log.on('data', data => {
                        receivedLogs.push(data);
                    });
                    batchState.logRes.log.on('end', () => {
                        assert.strictEqual(receivedLogs.length, 8);
                        return done();
                    });
                });
        });

        // Assertion on parsedlogs here is done in the extIngestionQP mock
        it('_processPrepareEntries should send entries in the correct format ' +
            'and update `nbLogEntriesRead` + `nbLogRecordsRead`', done => {
                async.waterfall([
                    next =>
                        this.ingestionReader._processReadRecords({}, batchState,
                            next),
                    next =>
                        this.ingestionReader._processPrepareEntries(batchState, next),
                ], () => {
                    // We have 8 records but one of these records has 2 entries, so
                    // we expect total log entries to be 9
                    assert.deepStrictEqual(batchState.logStats, {
                        nbLogRecordsRead: 8, nbLogEntriesRead: 9,
                        hasMoreLog: false,
                    });
                    return done();
                });
            });

        it('should successfully run setup()', done => {
            this.ingestionReader.setup(err => {
                assert.ifError(err);
                return done();
            });
        });

        it('should get logOffset', done => {
            const logOffset = this.ingestionReader.getLogOffset();
            // value initialized when creating MockZkClient
            assert.equal(logOffset, mockLogOffset);
            done();
        });

        // TODO: ZENKO-3420
        it.skip('should successfully ingest new bucket with existing object',
            done => {
                // update zookeeper status to indicate snapshot phase
                const path =
                    `${this.ingestionReader.bucketInitPath}/isStatusComplete`;
                async.waterfall([
                    next => zkClient.setData(path, Buffer.from('false'), -1,
                        err => {
                            assert.ifError(err);
                            return next();
                        }),
                    next => this.ingestionReader.processLogEntries({}, err => {
                        assert.ifError(err);
                        setTimeout(next, CONSUMER_TIMEOUT);
                    }),
                    next => {
                        consumer.consume(10, (err, entries) => {
                            assert.ifError(err);
                            checkEntryInQueue(entries, [expectedNewIngestionEntry],
                                next);
                        });
                    },
                ], done);
            });

        [
            {
                params: {},
                hasMoreLog: false,
            },
            {
                params: {
                    maxRead: 2,
                },
                hasMoreLog: true,
             },
        ].forEach(p => {
            it('should successfully generate entries from raft logs ' +
                `with processLogEntries params ${JSON.stringify(p.params)}`,
                done => {
                    async.waterfall([
                        next => this.ingestionReader.processLogEntries(p.params, (err, hasMoreLog) => {
                            assert.ifError(err);
                            setTimeout(() => {
                                next(null, hasMoreLog);
                            }, CONSUMER_TIMEOUT);
                        }),
                        (hasMoreLog, next) => {
                            consumer.consume(10, (err, entries) => {
                                assert.ifError(err);
                                // the mockLogs have 9 entries, but only 3 entries
                                // pertain to the test
                                if (!hasMoreLog) {
                                    assert.strictEqual(entries.length, 3);
                                } else {
                                    assert(entries.length <= p.params.maxRead);
                                }
                                assert.strictEqual(hasMoreLog, p.hasMoreLog);
                                entries.forEach(entry => {
                                    const receivedEntry =
                                        JSON.parse(entry.value.toString());
                                    assert(expectedOOBEntries.
                                        indexOf(receivedEntry.value) > -1);
                                });
                                return next();
                            });
                        },
                    ], done);
                });
        });
    });

    describe('testing with `bucket2` configuration', () => {
        const sourceConfig = testConfig.extensions.ingestion.sources[1];

        beforeEach(done => {
            this.ingestionReader = new IngestionReader({
                zkClient,
                ingestionConfig: testConfig.extensions.ingestion,
                kafkaConfig: testConfig.kafka,
                bucketdConfig: sourceConfig,
                qpConfig: testConfig.queuePopulator,
                logger: dummyLogger,
                extensions: [ingestionQP],
                metricsProducer: { publishMetrics: () => { } },
                s3Config: testConfig.s3,
                producer,
            });
            this.ingestionReader.setup(() => {
                async.series([
                    next => setZookeeperInitState(this.ingestionReader, zkClient, next),
                    next => setupS3Mock(sourceConfig, next),
                ], err => {
                    assert.ifError(err);
                    return done();
                });
            });
        });

        afterEach(done => {
            emptyAndDeleteVersionedBucket(sourceConfig, done);
        });

        it('should successfully ingest from new bucket: existing 0-byte ' +
            'object, existing object with versionId, existing object ' +
            'with utf-8 key, existing object with tags',
            done => {
                // update zookeeper status to indicate snapshot phase
                const path = `${this.ingestionReader.bucketInitPath}/isStatusComplete`;
                async.waterfall([
                    next => zkClient.setData(path, Buffer.from('false'), -1, err => {
                        assert.ifError(err);
                        return next();
                    }),
                    next => this.ingestionReader.processLogEntries({}, err => {
                        assert.ifError(err);
                        setTimeout(next, CONSUMER_TIMEOUT);
                    }),
                    next => {
                        consumer.consume(10, (err, entries) => {
                            assert.ifError(err);
                            checkEntryInQueue(entries, [
                                expectedZeroByteObj,
                                expectedUTF8Obj,
                                expectedTagsObj,
                                expectedVersionIdObj
                            ], next);
                        });
                    },
                ], done);
            });
    });
});
