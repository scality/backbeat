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
const IngestionQueuePopulator =
    require('../../../extensions/ingestion/IngestionQueuePopulator');
const IngestionReader = require('../../../lib/queuePopulator/IngestionReader');
const { initManagement } = require('../../../lib/management/index');
const testConfig = require('../../config.json');
const { setupS3Mock, emptyAndDeleteVersionedBucket } = require('./S3Mock');
const zookeeper = require('../../../lib/clients/zookeeper');

const testPort = testConfig.extensions.ingestion.sources[0].port;
const mockLogOffset = 2;
const CONSUMER_TIMEOUT = 25000;

const expectedNewIngestionEntry = {
    type: 'put',
    bucket: 'zenkobucket',
    key: 'object1',
    value: '{"owner-display-name":"test_1518720219","owner-id":' +
    '"94224c921648ada653f584f3caf42654ccf3f1cbd2e569a24e88eb460f2f84d8",' +
    '"content-length":17303,"content-md5":"42070968aa8ae79704befe77afc6935b",' +
    '"x-amz-version-id":"null","x-amz-server-version-id":"",' +
    '"x-amz-storage-class":"STANDARD","x-amz-server-side-encryption":"",' +
    '"x-amz-server-side-encryption-aws-kms-key-id":"",' +
    '"x-amz-server-side-encryption-customer-algorithm":"",' +
    '"x-amz-website-redirect-location":"","acl":{"Canned":"private",' +
    '"FULL_CONTROL":[],"WRITE_ACP":[],"READ":[],"READ_ACP":[]},"key":"",' +
    '"location":[],"isDeleteMarker":false,"tags":{},"replicationInfo":' +
    '{"status":"","backends":[],"content":[],"destination":"","storageClass":' +
    '"","role":"","storageType":"","dataStoreVersionId":""},"dataStoreName":' +
    '"us-east-1","last-modified":"2018-02-16T22:43:37.174Z",' +
    '"md-model-version":3}',
};

const expectedZeroByteObj = {
    type: 'put',
    bucket: 'zenkobucket',
    key: 'zerobyteobject',
    value: '{"owner-display-name":"test_1518720219","owner-id":' +
    '"94224c921648ada653f584f3caf42654ccf3f1cbd2e569a24e88eb460f2f84d8",' +
    '"content-length":0,"content-md5":"d41d8cd98f00b204e9800998ecf8427e",' +
    '"x-amz-version-id":"null","x-amz-server-version-id":"",' +
    '"x-amz-storage-class":"STANDARD","x-amz-server-side-encryption":"",' +
    '"x-amz-server-side-encryption-aws-kms-key-id":"",' +
    '"x-amz-server-side-encryption-customer-algorithm":"",' +
    '"x-amz-website-redirect-location":"","acl":{"Canned":"private",' +
    '"FULL_CONTROL":[],"WRITE_ACP":[],"READ":[],"READ_ACP":[]},"key":"",' +
    '"location":[],"isDeleteMarker":false,"tags":{},"replicationInfo":' +
    '{"status":"","backends":[],"content":[],"destination":"","storageClass":' +
    '"","role":"","storageType":"","dataStoreVersionId":""},"dataStoreName":' +
    '"us-east-1","last-modified":"2018-02-16T22:43:37.174Z",' +
    '"md-model-version":3}',
};

const expectedUTF8Obj = {
    type: 'put',
    bucket: 'zenkobucket',
    key: '䆩鈁櫨㟔罳',
    value: '{"owner-display-name":"test_1518720219","owner-id":' +
    '"94224c921648ada653f584f3caf42654ccf3f1cbd2e569a24e88eb460f2f84d8",' +
    '"content-length":0,"content-md5":"d41d8cd98f00b204e9800998ecf8427e",' +
    '"x-amz-version-id":"null","x-amz-server-version-id":"",' +
    '"x-amz-storage-class":"STANDARD","x-amz-server-side-encryption":"",' +
    '"x-amz-server-side-encryption-aws-kms-key-id":"",' +
    '"x-amz-server-side-encryption-customer-algorithm":"",' +
    '"x-amz-website-redirect-location":"","acl":{"Canned":"private",' +
    '"FULL_CONTROL":[],"WRITE_ACP":[],"READ":[],"READ_ACP":[]},"key":"",' +
    '"location":[],"isDeleteMarker":false,"tags":{},"replicationInfo":' +
    '{"status":"","backends":[],"content":[],"destination":"","storageClass":' +
    '"","role":"","storageType":"","dataStoreVersionId":""},"dataStoreName":' +
    '"us-east-1","last-modified":"2018-02-16T22:43:37.174Z",' +
    '"md-model-version":3}',
};

const expectedLogs = JSON.parse(JSON.stringify(mockLogs));
const expectedOOBEntries = expectedLogs.log.filter(entry =>
    entry.db === testConfig.extensions.ingestion.sources[0].bucket);
expectedOOBEntries.forEach((bucketEntry, i) => {
    expectedOOBEntries[i].entries[0].value.attributes =
        JSON.stringify(bucketEntry.entries[0].value.attributes);
    expectedOOBEntries[i] =
        JSON.stringify(bucketEntry.entries[0].value);
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
    // this function is called periodically based on
    // auto-commit of stored offsets
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

function checkEntryInQueue(kafkaEntries, expectedEntry) {
    assert.strictEqual(kafkaEntries.length, 2);

    const retrievedEntries = kafkaEntries.map(entry =>
        entry.value.toString());
    const expectedEntryString =
        JSON.stringify(expectedEntry);
    assert(retrievedEntries.indexOf(expectedEntryString) > -1);
}

describe('ingestion reader tests with mock', function fD() {
    let httpServer;

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
                kafkaConfig: testConfig.kafka,
                bucketdConfig: testConfig.extensions.ingestion.sources[0],
                qpConfig: testConfig.queuePopulator,
                logger: dummyLogger,
                extensions: [ingestionQP],
                s3Config: testConfig.s3,
                bucket: testConfig.extensions.ingestion.sources[0].bucket,
            });
            this.ingestionReader.setup(() => {
                async.series([
                    next => setZookeeperInitState(this.ingestionReader, next),
                    next => zkClient.setData(
                        this.ingestionReader.pathToLogOffset,
                        Buffer.from('2'), -1, err => {
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
            emptyAndDeleteVersionedBucket(sourceConfig, done);
        });

        it('_processReadRecords should retrieve logRes stream', done => {
            assert.strictEqual(batchState.logRes, null);
            return this.ingestionReader._processReadRecords({}, batchState,
                err => {
                assert.ifError(err);
                assert.deepStrictEqual(batchState.logRes.info,
                    { start: 1, cseq: 7, prune: 1 });
                const receivedLogs = [];
                batchState.logRes.log.on('data', data => {
                    receivedLogs.push(data);
                });
                batchState.logRes.log.on('end', () => {
                    assert.strictEqual(receivedLogs.length, 7);
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
                assert.deepStrictEqual(batchState.logStats, {
                    nbLogRecordsRead: 7, nbLogEntriesRead: 7,
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
                        checkEntryInQueue(entries, expectedNewIngestionEntry);
                        return next();
                    });
                },
            ], done);
        });

        it('should successfully generate entries from raft logs', done => {
            async.waterfall([
                next => this.ingestionReader.processLogEntries({}, err => {
                    assert.ifError(err);
                    setTimeout(next, CONSUMER_TIMEOUT);
                }),
                next => {
                    consumer.consume(10, (err, entries) => {
                        // the mockLogs have 7 entries, but only 2 entries
                        // pertain to the test so the expected length is 2
                        assert.strictEqual(entries.length, 2);
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

    describe('testing with `bucket2` configuration', () => {
        const sourceConfig = testConfig.extensions.ingestion.sources[1];

        beforeEach(done => {
            this.ingestionReader = new IngestionReader({
                zkClient,
                kafkaConfig: testConfig.kafka,
                bucketdConfig: sourceConfig,
                qpConfig: testConfig.queuePopulator,
                logger: dummyLogger,
                extensions: [ingestionQP],
                s3Config: testConfig.s3,
                bucket: sourceConfig.bucket,
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

        it('should successfully ingest new bucket with existing 0-byte object',
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
                        checkEntryInQueue(entries, expectedZeroByteObj);
                        return next();
                    });
                },
            ], done);
        });
    });

    describe('testing with `bucket3` configuration', () => {
        const sourceConfig = testConfig.extensions.ingestion.sources[2];

        beforeEach(done => {
            this.ingestionReader = new IngestionReader({
                zkClient,
                kafkaConfig: testConfig.kafka,
                bucketdConfig: sourceConfig,
                qpConfig: testConfig.queuePopulator,
                logger: dummyLogger,
                extensions: [ingestionQP],
                s3Config: testConfig.s3,
                bucket: sourceConfig.bucket,
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

        it('should successfully ingest new bucket with utf-8 key object',
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
                        checkEntryInQueue(entries, expectedUTF8Obj);
                        return next();
                    });
                },
            ], done);
        });
    });
});
