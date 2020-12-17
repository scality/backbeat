const assert = require('assert');
const { errors } = require('arsenal');
const BackbeatProducer = require('../../../lib/BackbeatProducer');
const kafkaConf = { hosts: 'localhost:9092' };
const topic = 'backbeat-producer-spec';
const multipleMessages = [
    { key: 'foo', message: 'hello' },
    { key: 'bar', message: 'world' },
    { key: 'qux', message: 'hi' },
];
const oneMessage = [{ key: 'foo', message: 'hello world' }];
const oneBigMessage = [{ key: 'large-foo',
                         message: Buffer.alloc(3000000).fill('x').toString() }];


[
    {
        type: 'auto-partitioning (keyed-message) mechanism',
        config: { kafka: kafkaConf, topic, pollIntervalMs: 100 },
    },
].forEach(item => {
    describe(`BackbeatProducer - ${item.type}`, function backbeatProducer() {
        this.timeout(10000);

        let producer;
        before(done => {
            producer = new BackbeatProducer(item.config);
            producer.on('ready', () => done());
        });
        after(() => { producer = null; });

        it('should be able to send one message', done => {
            producer.send(oneMessage, err => {
                assert.ifError(err);
                done();
            });
        }).timeout(30000);

        it('should be able to send a batch of messages', done => {
            producer.send(multipleMessages, err => {
                assert.ifError(err);
                done();
            });
        }).timeout(30000);

        it('should be able to send a big ' +
        `${oneBigMessage[0].message.length / 1000000}MB message`, done => {
            producer.send(oneBigMessage, err => {
                assert.ifError(err);
                done();
            });
        }).timeout(30000);
    });
});


describe('BackbeatProducer - Error case', function backbeatProducerErrors() {
    this.timeout(10000);

    let producer;
    before(done => {
        producer = new BackbeatProducer({ kafka: kafkaConf, topic });
        producer
            .on('ready', () => producer.close(done))
            .on('error', done);
    });

    after(() => { producer = null; });

    it('should get an error if producer is not ready', done => {
        producer.send(oneMessage, err => {
            assert.deepStrictEqual(errors.InternalError, err);
            done();
        });
    }).timeout(30000);
});
