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
        this._topicPartition = null;
        this._log = logger;
    }

    /**
     * Get partition offsets
     * Offsets are stored in kafka are not managed
     * by the logReader, only keeping this function
     * to not alter the logic path in the logReader
     * @returns {null}
     */
    _getOffset() {
        return null;
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
            // Manually manage storing offsets to ensure they are only stored
            // after the batch processing is fully completed.
            'enable.auto.offset.store': false,
            // Default auto-commit interval is 5 seconds
            'enable.auto.commit': true,
            'offset_commit_cb': this._onOffsetCommit.bind(this),
            'metadata.broker.list': this._kafkaHosts,
            'group.id': this._consumerGroupId,
            'max.poll.interval.ms': this._maxPollIntervalMs,
        };
        const topicParams = {
            'auto.offset.reset': 'earliest',
        };
        this._consumer = new kafka.KafkaConsumer(consumerParams, topicParams);
        this._consumer.connect();
        this._consumer.once('ready', () => {
            this._consumer.subscribe([this._topic]);
            done();
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
     * Offset commit callback
     * @param {Error} err
     * @param {Object} topicPartitions
     * @returns {undefined}
     */
    _onOffsetCommit(err, topicPartitions) {
        if (err) {
            // NO_OFFSET is a "soft error" meaning that the same
            // offset is already committed, which occurs because of
            // auto-commit (e.g. if nothing was done by the producer
            // on this partition since last commit).
            if (err.code === kafka.CODES.ERRORS.ERR__NO_OFFSET) {
                return undefined;
            }
            this._log.error('Error committing offsets to kafka', {
                method: 'LogConsumer._onOffsetCommit',
                errorCode: err,
                topicPartitions,
                groupId: this._consumerGroupId,
            });
            return undefined;
        }
        this._log.debug('Offsets committed', {
            method: 'LogConsumer._onOffsetCommit',
            topicPartitions,
            groupId: this._consumerGroupId
        });
        return undefined;
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
            // Use a Map to store the latest offset for each partition
            const topicPartition = new Map();
             // store next offsets to commit
            messages.forEach(message => {
                topicPartition.set(`${message.topic}-${message.partition}`, {
                    topic: message.topic,
                    partition: message.partition,
                    offset: message.offset + 1, // next offset to commit
                });
                // writing consumed messages to the stream
                this._listRecordStream.write(message);
            });
            // format offsets for commit
            this._topicPartition = Array.from(topicPartition.values());
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
            // consuming the desired number of messages at most
            next => this._consumeKafkaMessages(params.limit, next),
            next => {
                // ending and returning the stream
                this._listRecordStream.end();
                return next(null, { log: this._listRecordStream, tailable: false });
            }
        ], (err, res) => cb(err, res?.[2]));
    }

    /**
     * Stores the offsets locally for the consumer to
     * auto commit them at a later time
     * @returns {undefined}
     */
    storeOffsets() {
        if (!this._topicPartition || this._topicPartition.length === 0) {
            this._log.debug('No offsets to store', {
                method: 'LogConsumer.storeOffsets',
                topic: this._topic,
                consumerGroupId: this._consumerGroupId,
            });
            return;
        }
        this._consumer.offsetsStore(this._topicPartition);
        this._log.info('Offsets stored', {
            method: 'LogConsumer.storeOffsets',
            consumerGroupId: this._consumerGroupId,
            topicPartition: this._topicPartition,
        });
        this._pendingCommit = true;
        this._topicPartition = null;
    }
}

module.exports = LogConsumer;
