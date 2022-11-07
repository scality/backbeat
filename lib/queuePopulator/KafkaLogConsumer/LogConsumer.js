const kafka = require('node-rdkafka');
const async = require('async');
const ListRecordStream = require('./ListRecordStream');

// maximum time to wait for consumer group to rebalance (ms)
const maxDelayRebalanceMs = 120000;

class LogConsumer {

    /**
     * @constructor
     * @param {Object} kafkaConfig queue populator kafka config
     * @param {string} kafkaConfig.hosts kafka hosts
     * @param {string} kafkaConfig.topic kafka oplog topic
     * @param {string} kafkaConfig.consumerGroupId consumer group id
     * @param {Logger} logger logger
     */
    constructor(kafkaConfig, logger) {
        const { hosts, topic, consumerGroupId } = kafkaConfig;
        this._kafkaHosts = hosts;
        this._topic = topic;
        this._consumerGroupId = consumerGroupId;
        this._log = logger;
        this._offsets = {};
    }

    /**
     * Get partition offsets
     * @returns {string} stored partition offsets
     */
    _getOffset() {
        // Format: [{ topic: 'oplog-topic', partition: 0, offset: 0 }]
        return JSON.stringify(this._offsets);
    }

    /**
     * Connects consumer to kafka and subscribes
     * to oplog topic
     * @param {Function} done callback
     * @returns {undefined}
     */
    setup(done) {
        // partition offsets will be managed by kafka
        const consumerParams = {
            'enable.auto.offset.store': true,
            'metadata.broker.list': this._kafkaHosts,
            'group.id': this._consumerGroupId,
        };
        const topicParams = {
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': true,
        };
        this._consumer = new kafka.KafkaConsumer(consumerParams, topicParams);
        this._consumer.connect();
        this._consumer.on('ready', () => {
            this._consumer.subscribe([this._topic]);
            return done();
        });
    }

    /**
     * Waits for consumer group to rebalance
     * @param {number} wait amount of time to wait already waited for, in milliseconds
     * @param {Function} cb callback
     * @returns {undefined}
     */
    _waitForAssignment(wait, cb) {
        setTimeout(() => {
            // assignements contain the partitions
            // assigned to this consumer, they are only
            // set once consumer group is balanced
            const assignments = this._consumer.assignments();
            if (assignments.length === 0) {
                if (wait > maxDelayRebalanceMs) {
                    this._log.error('Timeout waiting for consumer to be assigned to partitions', {
                        method: 'LogConsumer._waitForAssignment',
                        topic: this._topic,
                        consumerGroupId: this._consumerGroupId,
                    });
                    return cb();
                }
                return this._waitForAssignment(wait + 2000, cb);
            }
            return cb();
        }, 2000);
    }

    /**
     * Queries last commited partition offsets
     * and stores them
     * @param {Function} cb callback
     * @returns {undefined}
     */
    _storeCurrentOffsets(cb) {
        this._consumer.committed(5000, (err, topicPartitions) => {
            if (err) {
                this._log.error('Error while getting offsets', {
                    method: 'LogConsumer._storeCurrentOffsets',
                    topic: this._topic,
                    error: err.message,
                    consumerGroupId: this._consumerGroupId,
                });
                return cb();
            }
            // saving offsets
            this._offsets = topicPartitions;
            return cb();
        });
    }

    /**
     * Inintializes record stream
     * @returns {undefined}
     */
    _resetRecordStream() {
        this._listRecordStream = new ListRecordStream(this._log);
        this._listRecordStream.getOffset = this._getOffset.bind(this);
    }

    /**
     * Consumes kafka messages and writes them to record
     * stream
     * @param {Number} limit maximum messages to consume
     * @param {Function} cb callback
     * @returns {undefined}
     */
    _consumeKafkaMessages(limit, cb) {
        this._resetRecordStream();
        this._consumer.consume(limit, (err, messages) => {
            if (err) {
                this._log.error('An error occured while consuming messages', {
                    method: 'LogConsumer.readRecords',
                    topic: this._topic,
                    error: err.message,
                    consumerGroupId: this._consumerGroupId,
                });
                return cb();
            }
            // writing consumed messages to the stream
            messages.forEach(message => this._listRecordStream.write(message));
            return cb();
        });
    }

    /**
     * Reads a certain number of messages from oplog kafka topic
     * The caller of this function expects a stream to be returned
     * in the callback
     * @param {Object} params reading params
     * @param {number} params.limit maximum number of elements to fetch
     * @param {Function} cb callback
     * @returns {undefined}
     */
    readRecords(params, cb) {
        async.series([
            // waiting for the consumer group to rebalance
            next => this._waitForAssignment(0, next),
            // saving last commited offset
            next => this._storeCurrentOffsets(next),
            // consuming the desired number of messages at most
            next => this._consumeKafkaMessages(params.limit, next),
            next => {
                // ending and returning the stream
                this._listRecordStream.end();
                return next(null, { log: this._listRecordStream, tailable: false });
            }
        ], (err, res) => cb(err, res[3]));
    }
}

module.exports = LogConsumer;
