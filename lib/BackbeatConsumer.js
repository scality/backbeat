const assert = require('assert');
const { EventEmitter } = require('events');
const { ConsumerGroup } = require('kafka-node');
const kafkaLogging = require('kafka-node/logging');
const async = require('async');

const { errors } = require('arsenal');
const Logger = require('werelogs').Logger;
kafkaLogging.setLoggerProvider(new Logger('Consumer'));
// controls the number of messages to process in parallel
const CONCURRENCY_DEFAULT = 1;
const LOG_DEFAULT = { logLevel: 'info', dumpLevel: 'error' };
const ZOOKEEPER_DEFAULT = { host: 'localhost', port: 2181 };
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
    * @param {Object} [config.zookeeper] - zookeeper endpoint config
    * @param {string} config.zookeeper.host - zookeeper host
    * @param {number} config.zookeeper.port - zookeeper port
    * @param {boolean} [config.ssl] - ssl enabled if ssl === true
    * @param {string} [config.fromOffset] - valid values latest/earliest/none
    * @param {number} [config.concurrency] - represents the number of entries
    * that can be processed in parallel
    * @param {number} [config.fetchMaxBytes] - max. bytes to fetch in a
    * fetch loop
    * @param {object} [config.log] - logger config
    * @param {object} config.log.logLevel - default log level
    * @param {object} config.log.dumpLevel - dump level for logger
    */
    constructor(config) {
        super();
        const { zookeeper, ssl, groupId, fromOffset, topic, queueProcessor,
            concurrency, fetchMaxBytes, log } = config;
        assert(typeof topic === 'string', 'config: topic must be a string');
        assert(typeof groupId === 'string', 'config: groupId must be a string');
        if (zookeeper !== undefined) {
            assert(typeof zookeeper === 'object', 'config: zookeeper must be' +
                'an object');
            assert(typeof zookeeper.host === 'string', 'config: ' +
                'zookeeper.host must be a string');
            assert(typeof zookeeper.port === 'number', 'config: ' +
                'zookeeper.port must be a number');
        }
        if (ssl !== undefined) {
            assert(typeof ssl === 'boolean', 'config: ssl must be a boolean');
        }
        if (fromOffset !== undefined) {
            assert(typeof fromOffset === 'string', 'config: fromOffset ' +
                'must be a string');
            assert(fromOffset === 'latest' || fromOffset === 'earliest' ||
                fromOffset === 'none', 'config: fromOffset should be one of' +
                'latest, earliest or none');
        }
        if (concurrency !== undefined) {
            assert(typeof concurrency === 'number' && concurrency > 0,
                'config: concurrency must be a number and greater than 0');
        }
        if (fetchMaxBytes !== undefined) {
            assert(typeof fetchMaxBytes === 'number', 'config: fetchMaxBytes' +
                'must be a number');
        }
        if (log !== undefined) {
            assert(typeof log === 'object', 'config: log must be an object');
            assert(typeof log.logLevel === 'string', 'config: log.logLevel ' +
                'must be a string');
            assert(typeof log.dumpLevel === 'string', 'config: log.dumpLevel' +
                ' must be a string');
        }
        const { logLevel, dumpLevel } = log || LOG_DEFAULT;
        const { host, port } = zookeeper || ZOOKEEPER_DEFAULT;
        this._zookeeperEndpoint = `${host}:${port}`;
        this._log = new Logger(CLIENT_ID, {
            level: logLevel,
            dump: dumpLevel,
        });
        this._topic = topic;
        this._groupId = groupId;
        this._queueProcessor = queueProcessor;
        this._concurrency = concurrency || CONCURRENCY_DEFAULT;
        this._messagesConsumed = 0;
        this._consumer = new ConsumerGroup({
            host: this._zookeeperEndpoint,
            ssl,
            groupId: this._groupId,
            fromOffset,
            autoCommit: false,
            fetchMaxBytes,
        }, this._topic);
        this._consumer.on('connect', () => this.emit('connect'));
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
        let partition = null;
        let offset = null;
        // consume a message in the fetch loop
        this._consumer.on('message', entry => {
            partition = entry.partition;
            offset = entry.offset;
            this._messagesConsumed++;
            this._consumer.pause();
            q.push(entry, err => {
                if (err) {
                    this._log.error('error processing an entry', {
                        error: err,
                        method: 'BackbeatConsumer.subscribe',
                        partition,
                        offset,
                    });
                    const error = errors.InternalError
                        .customizeDescription('error processing entry');
                    this.emit('error', error, entry);
                }
            });
        });

        // commit offset and resume fetch loop when the task queue is empty
        q.drain = () => {
            const count = this._messagesConsumed;
            this._consumer.sendOffsetCommitRequest([{
                topic: this._topic,
                partition, // default 0
                offset,
                metadata: 'm', //default 'm'
            }], () => {
                this.emit('consumed', count);
                this._messagesConsumed = 0;
                this._consumer.resume();
            });
        };

        this._consumer.on('error', error => {
            this._consumer.pause();
            this._log.error('error subscribing to topic', {
                error,
                method: 'BackbeatConsumer.subscribe',
                topic: this._topic,
                partition: this._partition,
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
        this._consumer.close(true, cb);
    }
}

module.exports = BackbeatConsumer;
