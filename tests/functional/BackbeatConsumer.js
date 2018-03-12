const assert = require('assert');
const BackbeatProducer = require('../../lib/BackbeatProducer');
const BackbeatConsumer = require('../../lib/BackbeatConsumer');
const kafkaConf = { hosts: 'localhost:9092' };
const topic = 'backbeat-consumer-spec';
const groupId = `replication-group-${Math.random()}`;
const messages = [
    { key: 'foo', message: 'hello' },
    { key: 'bar', message: 'world' },
    { key: 'qux', message: 'hi' },
];
describe('BackbeatConsumer', () => {
    let producer;
    let consumer;
    const consumedMessages = [];
    function queueProcessor(message, cb) {
        consumedMessages.push(message.value);
        process.nextTick(cb);
    }
    before(function before(done) {
        this.timeout(60000);

        let producerReady = false;
        let consumerReady = false;
        function _doneIfReady() {
            if (producerReady && consumerReady) {
                done();
            }
        }
        producer = new BackbeatProducer({ kafka: kafkaConf, topic,
                                          pollIntervalMs: 100 });
        consumer = new BackbeatConsumer({ kafka: kafkaConf, groupId, topic,
                                          queueProcessor,
                                          bootstrap: true });
        producer.on('ready', () => {
            producerReady = true;
            _doneIfReady();
        });
        consumer.on('ready', () => {
            consumerReady = true;
            _doneIfReady();
        });
    });
    after(done => {
        producer.close(() => {
            consumer.close(() => {
                producer = null;
                consumer = null;
                done();
            });
        });
    });

    it('should be able to read messages sent to the topic', done => {
        let consumeCb = null;
        let totalConsumed = 0;

        consumer.subscribe();
        consumer.on('consumed', messagesConsumed => {
            totalConsumed += messagesConsumed;
            assert(totalConsumed <= messages.length);
            if (totalConsumed === messages.length) {
                assert.deepStrictEqual(
                    messages.map(e => e.message),
                    consumedMessages.map(buffer => buffer.toString()));
                consumeCb();
            }
        });
        consumeCb = done;
        producer.send(messages, err => {
            assert.ifError(err);
        });
    }).timeout(30000);
});
