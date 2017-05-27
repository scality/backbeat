const assert = require('assert');
const BackbeatProducer = require('../../lib/BackbeatProducer');
const BackbeatConsumer = require('../../lib/BackbeatConsumer');
const zookeeper = { host: 'localhost', port: 2181 };
const log = { logLevel: 'info', dumpLevel: 'error' };
const topic = 'backbeat-consumer-spec';
const groupId = `replication-group-${Math.random()}`;
const messages = [
    { key: 'foo', message: 'hello' },
    { key: 'bar', message: 'world' },
    { key: 'qux', message: 'hi' },
];
// skipping test suite for now, as tests are failing intermittently. Connect
// event is not reliable enough to check if consumer is ready
describe.skip('BackbeatConsumer', () => {
    let producer;
    let consumer;
    const consumedMessages = [];
    function queueProcessor(message, cb) {
        consumedMessages.push(message.value);
        process.nextTick(cb);
    }
    before(done => {
        let producerReady = false;
        let consumerReady = false;
        function _doneIfReady() {
            if (producerReady && consumerReady) {
                done();
            }
        }
        producer = new BackbeatProducer({ zookeeper, log, topic });
        consumer = new BackbeatConsumer({ zookeeper, groupId, topic,
            queueProcessor, log });
        producer.on('ready', () => {
            producerReady = true;
            _doneIfReady();
        });
        consumer.on('connect', () => {
            consumerReady = true;
            _doneIfReady();
        });
    });
    after(() => {
        producer = null;
        consumer = null;
    });

    it('should be able to read messages sent to the topic', done => {
        producer.send(messages, () => {
            consumer.subscribe().on('consumed', () => {
                assert.deepStrictEqual(messages.map(e => e.message),
                    consumedMessages);
                done();
            });
        });
    });
});
