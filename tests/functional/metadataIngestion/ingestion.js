const async = require('async');
const http = require('http');
const kafka = require('node-rdkafka');

const QueuePopulator = require('../../../lib/queuePopulator/QueuePopulator');
const IngestionProducer =
    require('../../../lib/queuePopulator/IngestionProducer');
const MetadataMock = require('../../utils/MetadataMock');
const testConfig = require('../../config.json');

const testKafkaConfig = {
    'metadata.broker.list': 'localhost:9092',
};

describe('Ingest metadata to kafka', () => {
    let metadataMock;
    let httpServer;

    before(function before(done) {
        // async.waterfall([
        //     next => {
        //         this.kafkaConsumer = new kafka.KafkaConsumer(testKafkaConfig);
        //         return next();
        //     },
        //     next => {
        //         this.kafkaConsumer.connect();
        //         return next();
        //     },
        //     next => {
        //         metadataMock = new MetadataMock();
        //         httpServer = http.createServer((req, res) =>
        //             metadataMock.onRequest(req, res)).listen(7779);
        //         return
        //     },
        // ])
        this.kafkaConsumer = new kafka.KafkaConsumer(testKafkaConfig);
        this.kafkaConsumer.connect();
        metadataMock = new MetadataMock();
        httpServer = http.createServer(
            (req, res) => metadataMock.onRequest(req, res)).listen(7779);
        this.iProducer = new IngestionProducer({
            host: 'localhost:7779',
            port: 7779,
        });
        this.queuePopulator = new QueuePopulator(
            testConfig.zookeeper, testConfig.kafka, testConfig.queuePopulator,
            testConfig.extensions);
        return this.queuePopulator.open(done);
    });

    after(done => {
        httpServer.close();
        done();
    });

    it('should store metadata ingested from remote cloud backend', done => {
        async.waterfall([
            next => this.iProducer.snapshot(1, (err, res) => {
                console.log(res);
                return next();
            }),
            next => this.queuePopulator.processAllLogEntries({ maxRead: 10 },
            (err, counters) => {
                console.log(err, counters);
                return next();
            }),
            next => {
                return this.kafkaConsumer.getMetadata({}, (err, res) => {
                    console.log(err);
                    console.log(res);
                    return next();
                });
            }
        ], done);
    });
});
