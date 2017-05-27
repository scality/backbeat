const assert = require('assert');
const { errors } = require('arsenal');
const BackbeatProducer = require('../../lib/BackbeatProducer');
const zookeeper = { host: 'localhost', port: 2181 };
const log = { logLevel: 'info', dumpLevel: 'error' };
const topic = 'backbeat-producer-spec';
const partition = 0;
const multipleMessages = [
    { key: 'foo', message: 'hello' },
    { key: 'bar', message: 'world' },
    { key: 'qux', message: 'hi' },
];
const oneMessage = [{ key: 'foo', message: 'hello world' }];


[
    {
        type: 'partition mechanism',
        config: { zookeeper, topic, partition, log },
    },
    {
        type: 'auto-partitioning (keyed-message) mechanism',
        config: { zookeeper, topic, log },
    },
].forEach(item => {
    describe(`BackbeatProducer - ${item.type}`, () => {
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
        });

        it('should be able to send a batch of messages', done => {
            producer.send(multipleMessages, err => {
                assert.ifError(err);
                done();
            });
        });
    });
});


describe('BackbeatProducer - Error case', () => {
    let producer;
    before(done => {
        producer = new BackbeatProducer({ zookeeper, topic, log });
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
    });
});
