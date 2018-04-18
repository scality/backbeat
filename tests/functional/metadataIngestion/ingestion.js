const async = require('async');
const http = require('http');
const kafka = require('node-rdkafka');

const QueuePopulator = require('../../../lib/queuePopulator/QueuePopulator');
const IngestionProducer =
    require('../../../lib/queuePopulator/IngestionProducer');
const MetadataMock = require('../../utils/MetadataMock');
const testConfig = require('./config.json');

const testKafkaConfig = {
    'metadata.broker.list': 'localhost:9092',
    'group.id': 'testid',
};

describe.only('Ingest metadata to kafka', () => {
    let metadataMock;
    let httpServer;
    let kafkaConsumer;
    let iProducer;
    let queuePopulator;

    before(function before(done) {
        async.waterfall([
            next => {
                kafkaConsumer = new kafka.KafkaConsumer(testKafkaConfig);
                return next();
            },
            next => {
                kafkaConsumer.connect({ allTopics: true }, (err, res) => {
                    console.log('attempting to connect to kafkaConsumer');
                    console.log(err, res);
                    return next(err);
                });
            },
            next => {
                metadataMock = new MetadataMock();
                httpServer = http.createServer((req, res) =>
                    metadataMock.onRequest(req, res)).listen(7779);
                return next();
            },
            next => {
                iProducer = new IngestionProducer({
                    host: 'localhost:7779',
                    port: 7779,
                });
                return next();
            },
            next => {
                queuePopulator = new QueuePopulator(testConfig.zookeeper,
                testConfig.kafka, testConfig.queuePopulator, testConfig.metrics,
                testConfig.redis, testConfig.extensions, testConfig.ingestion);
                return queuePopulator.open(next);
            },
        ], done);
        // this.kafkaConsumer = new kafka.KafkaConsumer(testKafkaConfig);
        // this.kafkaConsumer.connect();
        // metadataMock = new MetadataMock();
        // httpServer = http.createServer(
        //     (req, res) => metadataMock.onRequest(req, res)).listen(7779);
        // this.iProducer = new IngestionProducer({
        //     host: 'localhost:7779',
        //     port: 7779,
        // });
        // this.queuePopulator = new QueuePopulator(
        //     testConfig.zookeeper, testConfig.kafka, testConfig.queuePopulator,
        //     testConfig.metrics, testConfig.redis, testConfig.extensions,
        //     testConfig.ingestion);
        // return this.queuePopulator.open(done);
    });

    after(done => {
        httpServer.close();
        done();
    });

    it('should store metadata ingested from remote cloud backend', done => {
        return async.waterfall([
            next => {
                console.log('this.iProducer', iProducer);
                next();
            },
            next => iProducer.snapshot(1, (err, res) => {
                console.log('WE PRODUCED SNAPSHOT', res);
                return next();
            }),
            next => {
                console.log('WE WILL PROCESS ALL LOG ENTRIES NOW');
                queuePopulator.processAllLogEntries({ maxRead: 10 },
                (err, counters) => {
                    console.log('attempting to process all log entries');
                    console.log(err, counters);
                    return next();
                });
            },
            next => {
                return kafkaConsumer.getMetadata({}, (err, res) => {
                    console.log('Getting metadata from kafkaConsumer', err);
                    console.log('Getting metadata from kafkaConsumer', res);
                    return next();
                });
            }
        ], () => {
            console.log('finishing');
            return done();
        });
    });
});
