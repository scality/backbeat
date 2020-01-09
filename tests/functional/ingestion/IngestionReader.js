const assert = require('assert');
const async = require('async');
const http = require('http');
const kafka = require('node-rdkafka');
const jsutil = require('arsenal').jsutil;
const { MetadataMock, mockLogs } = require('../utils/MetadataMock');
const MongoClient = require('mongodb').MongoClient;

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
const zookeeper = require('../../../lib/clients/zookeeper');
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
const zkClient = zookeeper.createClient('localhost:2181', {
    autoCreateNamespace: true,
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

function setZookeeperInitState(ingestionReader, cb) {
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

    before(done => {
        testConfig.s3.port = testPort;
        const mongoUrl =
            `mongodb://${testConfig.queuePopulator.mongo.replicaSetHosts}` +
            '/db?replicaSet=rs0';
        async.waterfall([
            next => {
                MongoClient.connect(mongoUrl, {}, (err, client) => {
                    if (err) {
                        next(err);
                    }
                    this.client = client;
                    this.db = this.client.db('metadata', {
                        ignoreUndefined: true,
                    });
                    next();
                });
            },
            next => {
                const topic = testConfig.extensions.ingestion.topic;
                producer = new BackbeatProducer({
                    kafka: testConfig.kafka,
                    topic,
                });
                producer.once('ready', () => {
                    next();
                });
            },
            next => {
                consumer.connect({ timeout: 1000 }, () => {});
                consumer.once('ready', () => {
                    next();
                });
            },
            next => {
                consumer.subscribe([testConfig.extensions.ingestion.topic]);
                setTimeout(next, 2000);
            },
            next => this.db.createCollection('PENSIEVE', err => {
                assert.ifError(err);
                return next();
            }),
            next => {
                this.m = this.db.collection('PENSIEVE');
                this.m.insertOne(dummyPensieveCredentials, {});
                return next();
            },
            next => {
                this.m.insertOne({
                    _id: 'configuration/overlay-version',
                    value: 6,
                }, {});
                return next();
            },
            next => {
                this.m.insertOne(dummySSHKey, {});
                return next();
            },
            next => {
                const cbOnce = jsutil.once(next);
                zkClient.connect();
                zkClient.once('error', cbOnce);
                zkClient.once('ready', () => {
                    zkClient.removeAllListeners('error');
                    return cbOnce();
                });
            },
            next => initManagement(testConfig, next),
            next => {
                const metadataMock = new MetadataMock();
                httpServer = http.createServer(
                    (req, res) => metadataMock.onRequest(req, res))
                    .listen(testPort);
                next();
            }
        ], done);
    });

    after(done => {
        async.waterfall([
            next => {
                httpServer.close();
                next();
            },
            next => {
                consumer.unsubscribe();
                next();
            },
            next => this.db.collection('PENSIEVE').drop(err => {
                assert.ifError(err);
                this.client.close();
                next();
            }),
        ], done);
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
                metricsProducer: { publishMetrics: () => {} },
                s3Config: testConfig.s3,
                producer,
            });
            this.ingestionReader.setup(() => {
                async.series([
                    next => setZookeeperInitState(this.ingestionReader, next),
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

        it('should successfully ingest new bucket with existing object',
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

        [{}, { maxRead: 2 }].forEach(params => {
             it('should successfully generate entries from raft logs ' +
             `with processLogEntries params ${JSON.stringify(params)}`,
             done => {
                 async.waterfall([
                     next => this.ingestionReader.processLogEntries({}, err => {
                         assert.ifError(err);
                         setTimeout(next, CONSUMER_TIMEOUT);
                     }),
                     next => {
                         consumer.consume(10, (err, entries) => {
                             // the mockLogs have 9 entries, but only 3 entries
                             // pertain to the test so the expected length is 3
                             assert.strictEqual(entries.length, 3);
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
                metricsProducer: { publishMetrics: () => {} },
                s3Config: testConfig.s3,
                producer,
            });
            this.ingestionReader.setup(() => {
                async.series([
                    next => setZookeeperInitState(this.ingestionReader, next),
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
