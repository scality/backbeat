const { EventEmitter } = require('events');
const { KafkaConsumer } = require('node-rdkafka');
const async = require('async');
const joi = require('joi');

const Logger = require('werelogs').Logger;

// controls the number of messages to process in parallel
const CONCURRENCY_DEFAULT = 1;
const CLIENT_ID = 'BackbeatConsumer';

class BackbeatConsumer extends EventEmitter {

    /**
    * constructor
    * @param {Object} config - config
    * @param {string} config.topic - Kafka topic to subscribe to
    * @param {function} config.queueProcessor - function to invoke to process
    * an item in a queue
    * @param {string} config.groupId - consumer group id. Messages are
    * distributed among multiple consumers belonging to the same group
    * @param {Object} config.kafka - kafka connection config
    * @param {string} config.kafka.hosts - kafka hosts list
    * as "host:port[,host:port...]"
    * @param {string} [config.fromOffset] - valid values latest/earliest
    * @param {number} [config.concurrency] - represents the number of entries
    * that can be processed in parallel
    * @param {number} [config.fetchMaxBytes] - max. bytes to fetch in a
    * fetch loop
    */
    constructor(config) {
        super();

        const configJoi = {
            kafka: joi.object({
                hosts: joi.string().required(),
            }).required(),
            topic: joi.string().required(),
            groupId: joi.string().required(),
            queueProcessor: joi.func(),
            fromOffset: joi.alternatives().try('latest', 'earliest'),
            concurrency: joi.number().greater(0).default(CONCURRENCY_DEFAULT),
            fetchMaxBytes: joi.number(),
        };
        const validConfig = joi.attempt(config, configJoi,
                                        'invalid config params');

        const { kafka, topic, groupId, queueProcessor,
                fromOffset, concurrency, fetchMaxBytes } = validConfig;

        this._kafkaHosts = kafka.hosts;
        this._log = new Logger(CLIENT_ID);
        this._topic = topic;
        this._groupId = groupId;
        this._queueProcessor = queueProcessor;
        this._concurrency = concurrency;
        this._messagesConsumed = 0;
        const consumerParams = {
            'metadata.broker.list': this._kafkaHosts,
            'group.id': this._groupId,
            'enable.auto.commit': false,
        };
        if (fromOffset !== undefined) {
            consumerParams['auto.offset.reset'] = fromOffset;
        }
        if (fetchMaxBytes !== undefined) {
            consumerParams['fetch.message.max.bytes'] = fetchMaxBytes;
        }
        this._consumer = new KafkaConsumer(consumerParams);
        this._consumer.connect();
        this._consumer.on('ready', () => {
            this.emit('ready');
        });
        return this;
    }

    /**
    * subscribe to messages from a topic
    * Once subscribed, the consumer does a fetch from the topic with new
    * messages. Each fetch loop can contain one or more messages, so the fetch
    * is paused until the current queue of tasks are processed. Once the task
    * queue is empty, the current offset is committed and the fetch is resumed
    * to get the next batch of messages
    * @return {this} current instance
    */
    subscribe() {
        const q = async.queue(this._queueProcessor, this._concurrency);

        this._consumer.subscribe([this._topic]);
        this._consumer.consume();
        // consume a message in the fetch loop
        this._consumer.on('data', entry => {
            this._messagesConsumed++;
            this._consumer.pause([this._topic]);
            q.push(entry, err => {
                if (err) {
                    const { topic, partition, offset, key, timestamp } = entry;
                    this._log.error('error processing an entry', {
                        error: err,
                        entry: { topic, partition, offset, key, timestamp },
                    });
                    this.emit('error', err, entry);
                }
            });
        });

        // when the task queue is empty, commit offset for all
        // consumed partitions and resume fetch loop
        q.drain = () => {
            this._consumer.commit();
            this.emit('consumed', this._messagesConsumed);
            this._messagesConsumed = 0;
            this._consumer.resume([this._topic]);
        };

        this._consumer.on('event.error', error => {
            this._log.error('consumer error', {
                error,
                topic: this._topic,
            });
            this.emit('error', error);
        });

        return this;
    }

    /**
    * force commit the current offset and close the client connection
    * @param {callback} cb - callback to invoke
    * @return {undefined}
    */
    close(cb) {
        this._consumer.commit();
        this._consumer.disconnect();
        this._consumer.on('disconnected', cb);
    }
}

module.exports = BackbeatConsumer;
