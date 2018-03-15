const assert = require('assert');
const async = require('async');

const BackbeatProducer = require('../../../lib/BackbeatProducer');
const BackbeatTestConsumer = require('../../utils/BackbeatTestConsumer');

const TIMEOUT = 60000;
const TOPIC = 'backbeat-test-consumer-spec';
const GROUP_ID = 'test-consumer-group';

describe('BackbeatTestConsumer', function backbeatTestConsumer() {
    let consumer;
    let producer;
    const messages = [
        { key: 'k1', message: '{"value":"m1"}' },
        { key: 'k2', message: '{"value":"m2"}' },
        { message: '{"value":"m3"}' },
    ];

    this.timeout(TIMEOUT);

    before(done => {
        async.series([
            next => {
                producer = new BackbeatProducer({
                    kafka: { hosts: 'localhost:9092' },
                    topic: TOPIC,
                });
                producer.once('error', next);
                producer.once('ready', () => {
                    producer.removeAllListeners('error');
                    next();
                });
            },
            next => {
                consumer = new BackbeatTestConsumer({
                    kafka: { hosts: 'localhost:9092' },
                    topic: TOPIC,
                    groupId: GROUP_ID,
                });
                consumer.on('ready', next);
            },
            next => {
                consumer.subscribe();
                // it seems the consumer needs some extra time to
                // start consuming the first messages
                setTimeout(next, 2000);
            },
        ], done);
    });

    after(done => {
        async.waterfall([
            next => producer.close(next),
            next => consumer.close(next),
        ], done);
    });

    it('should succeed with expected ordered messages', done => {
        const expected = messages.map(obj => ({ key: obj.key,
                                                value: obj.message }));
        consumer.expectOrderedMessages(expected, TIMEOUT, err => {
            assert.ifError(err);
            done();
        });
        producer.send(messages, err => {
            assert.ifError(err);
        });
    });
    it('should fail with messages expected ordered ' +
    'received out of order', done => {
        const expected = messages.map(obj => ({ key: obj.key,
                                                value: obj.message }));
        const outOfOrder = [expected[1], expected[2], expected[0]];
        consumer.expectOrderedMessages(outOfOrder, TIMEOUT, err => {
            assert(err);
            done();
        });
        producer.send(messages, err => {
            assert.ifError(err);
        });
    });
    it('should succeed with messages expected unordered ' +
    'received out of order', done => {
        const expected = messages.map(obj => ({ key: obj.key,
                                                value: obj.message }));
        const outOfOrder = [expected[1], expected[2], expected[0]];
        consumer.expectUnorderedMessages(outOfOrder, TIMEOUT, err => {
            assert.ifError(err);
            done();
        });
        producer.send(messages, err => {
            assert.ifError(err);
        });
    });
    it('should fail with messages expected ordered if ' +
    'unexpected message received', done => {
        const expected = messages.map(
            obj => ({ key: obj.key,
                      value: JSON.parse(obj.message) }));
        consumer.expectOrderedMessages(expected, TIMEOUT, err => {
            assert(err);
            done();
        });
        const altered = [messages[0],
                         { key: messages[1].key,
                           message: '{"not":"expected"}' },
                         messages[2]];
        producer.send(altered, err => {
            assert.ifError(err);
        });
    });
    it('should fail with messages expected unordered if ' +
    'unexpected message received', done => {
        const expected = messages.map(
            obj => ({ key: obj.key,
                      value: JSON.parse(obj.message) }));
        consumer.expectUnorderedMessages(expected, TIMEOUT, err => {
            assert(err);
            done();
        });
        const altered = [messages[0],
                         { key: messages[1].key,
                           message: '{"not":"expected"}' },
                         messages[2]];
        producer.send(altered, err => {
            assert.ifError(err);
        });
    });
    it('should timeout if not all expected ordered messages ' +
    'are received', done => {
        const expected = messages.map(
            obj => ({ key: obj.key,
                      value: JSON.parse(obj.message) }));
        consumer.expectOrderedMessages(expected, 1000, err => {
            assert(err);
            done();
        });
        const truncated = [messages[0], messages[1]];
        producer.send(truncated, err => {
            assert.ifError(err);
        });
    });
    it('should timeout if not all expected unordered messages ' +
    'are received', done => {
        const expected = messages.map(
            obj => ({ key: obj.key,
                      value: JSON.parse(obj.message) }));
        consumer.expectUnorderedMessages(expected, 1000, err => {
            assert(err);
            done();
        });
        const truncated = [messages[0], messages[2]];
        producer.send(truncated, err => {
            assert.ifError(err);
        });
    });
});
