const { EventEmitter } = require('events');
const kafka = require('node-rdkafka');
const async = require('async');
const jsutil = require('arsenal').jsutil;
const ListRecordStream = require('./ListRecordStream');
const KafkaBacklogMetrics = require('../../KafkaBacklogMetrics');
const { unassignStatus } = require('../../constants');

const EVENTS = {
    DRAINED: 'drained',
    UNASSIGNED: 'unassigned',
};

const DRAIN_TIMEOUT_MS = 30000;

class LogConsumer extends EventEmitter {

    /**
     * @constructor
     * @param {Object} kafkaConfig queue populator kafka config
     * @param {string} kafkaConfig.hosts kafka hosts
     * @param {string} kafkaConfig.topic kafka oplog topic
     * @param {string} kafkaConfig.consumerGroupId consumer group id
     * @param {Object} [kafkaConfig.consumerParams] extra rdkafka params
     * @param {Logger} logger logger
     */
    constructor(kafkaConfig, logger) {
        super();
        const { hosts, topic, consumerGroupId, maxPollIntervalMs,
            consumerParams } = kafkaConfig;
        this._kafkaHosts = hosts;
        this._consumerParams = consumerParams;
        this._maxPollIntervalMs = maxPollIntervalMs || 300000; // default to 5 minutes
        this._topic = topic;
        this._consumerGroupId = consumerGroupId;
        this._topicPartition = null;
        this._log = logger;
        this._consumer = null;
    }

    /**
     * Get partition offsets
     * Offsets are stored in kafka and not managed
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
            ...this._consumerParams,
            // This is the default in our current librdkafka version, but we
            // pin it explicitly because we depend on eager rebalancing and
            // don't want it changed implicitly by future version updates.
            'partition.assignment.strategy': 'range,roundrobin',
            // Manually manage storing offsets to ensure they are only stored
            // after the batch processing is fully completed.
            'enable.auto.offset.store': false,
            // Default auto-commit interval is 5 seconds
            'enable.auto.commit': true,
            'offset_commit_cb': this._onOffsetCommit.bind(this),
            'rebalance_cb': this._onRebalance.bind(this),
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
     * @param {kafka.KafkaError} err Rebalance event
     * @param {TopicPartition[]} assignment List of (un)assigned partitions
     * @returns {void}
     */
    _onRebalance(err, assignment) {
        if (err.code === kafka.CODES.ERRORS.ERR__ASSIGN_PARTITIONS) {
            this._log.info('rdkafka.assign', {
                method: 'LogConsumer._onRebalance',
                assignment,
                topic: this._topic,
                consumerGroupId: this._consumerGroupId,
            });
            this._assignPartitions(assignment);
        } else if (err.code === kafka.CODES.ERRORS.ERR__REVOKE_PARTITIONS) {
            this._log.info('rdkafka.revoke', {
                method: 'LogConsumer._onRebalance',
                assignment,
                topic: this._topic,
                consumerGroupId: this._consumerGroupId,
            });
            this._drainAndUnassign();
        } else {
            this._log.error('rdkafka.rebalance', {
                method: 'LogConsumer._onRebalance',
                err,
                assignment,
                topic: this._topic,
                consumerGroupId: this._consumerGroupId,
            });
        }
    }

    /**
     * Assign partitions to the consumer
     * @param {TopicPartition[]} assignment
     */
    _assignPartitions(assignment) {
        try {
            this._consumer.assign(assignment);
        } catch (e) {
            const logger = this._consumer.isConnected() ? this._log.error : this._log.debug;
            logger.bind(this._log)('rdkafka.assign failed', {
                method: 'LogConsumer._onRebalance',
                error: e.toString(),
                assignment,
                topic: this._topic,
                consumerGroupId: this._consumerGroupId,
            });
        }
    }

    /**
     * Unassign partitions from the consumer
     * @returns {void}
     */
    _unassignPartitions() {
        try {
            this._consumer.unassign();
        } catch (e) {
            const logger = this._consumer.isConnected() ? this._log.error : this._log.info;
            logger.bind(this._log)('rdkafka.unassign failed', {
                method: 'LogConsumer._onRebalance',
                error: e.toString(),
                topic: this._topic,
                consumerGroupId: this._consumerGroupId,
            });
        }
        this.emit(EVENTS.UNASSIGNED);
    }

    /**
     * Waits for the in-flight batch to be processed, then commits
     * stored offsets and unassigns the partitions from the consumer
     * @returns {void}
     */
    _drainAndUnassign() {
        let drainTimeout;
        const drainHandler = () => unassign(unassignStatus.DRAINED);

        const unassign = jsutil.once(status => {
            this.removeListener(EVENTS.DRAINED, drainHandler);
            clearTimeout(drainTimeout);
            drainTimeout = null;
            KafkaBacklogMetrics.onRebalance(this._topic, this._consumerGroupId, status);
            this._commitStoredOffsets();
            this._unassignPartitions();
        });

        if (!this._hasUnprocessedMessages()) {
            return unassign(unassignStatus.IDLE);
        }

        this.once(EVENTS.DRAINED, drainHandler);

        drainTimeout = setTimeout(() => {
            unassign(unassignStatus.TIMEOUT);
            this._log.error('Timeout waiting for the batch to be processed: ' +
                'consumer stuck, disconnecting', {
                method: 'LogConsumer._drainAndUnassign',
                topic: this._topic,
                consumerGroupId: this._consumerGroupId,
            });
            // disconnect so the healthcheck fails and the process
            // gets restarted
            this._consumer.disconnect();
        }, Math.min(DRAIN_TIMEOUT_MS, this._maxPollIntervalMs - 1000));

        return undefined;
    }

    /**
     * Best-effort commit of the stored offsets before releasing the
     * partitions; uncommitted offsets are re-consumed by the next
     * partition owner
     * @returns {void}
     */
    _commitStoredOffsets() {
        try {
            this._consumer.commit();
        } catch (e) {
            const logger = this._consumer.isConnected() &&
                e.code !== kafka.CODES.ERRORS.ERR__STATE ? this._log.error : this._log.info;
            logger.bind(this._log)('rdkafka.commit failed', {
                method: 'LogConsumer._commitStoredOffsets',
                error: e.toString(),
                topic: this._topic,
                consumerGroupId: this._consumerGroupId,
            });
        }
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
        const isConnected = this._consumer.isConnected();
        if (!isConnected || this._hasUnprocessedMessages()) {
            this._log.info('Skipping message consumption', {
                method: 'LogConsumer._consumeKafkaMessages',
                topic: this._topic,
                consumerGroupId: this._consumerGroupId,
                reason: !isConnected ? 'consumer not connected' :
                    'awaiting processing of previous batch',
            });
            return cb();
        }
        return this._consumer.consume(limit, (err, messages) => {
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
            // consuming the desired number of messages at most
            next => this._consumeKafkaMessages(params.limit, next),
            next => {
                // ending and returning the stream
                this._listRecordStream.end();
                return next(null, { log: this._listRecordStream, tailable: false });
            }
        ], (err, res) => cb(err, res?.[1]));
    }

    /**
     * Stores the offsets locally for the consumer to
     * auto commit them at a later time
     * @returns {undefined}
     */
    storeOffsets() {
        if (this._hasUnprocessedMessages()) {
            const topicPartition = this._topicPartition;
            this._topicPartition = null;
            // only store offsets while connected; skipped offsets are
            // re-consumed by the next partition owner.
            if (!this._consumer.isConnected()) {
                this._log.info('Skipping offsets store', {
                    method: 'LogConsumer.storeOffsets',
                    reason: 'consumer not connected',
                    topicPartition,
                    consumerGroupId: this._consumerGroupId,
                });
            } else {
                try {
                    this._consumer.offsetsStore(topicPartition);
                    this._log.info('Offsets stored', {
                        method: 'LogConsumer.storeOffsets',
                        consumerGroupId: this._consumerGroupId,
                        topicPartition,
                    });
                } catch (e) {
                    this._log.error('offsetsStore failed', {
                        method: 'LogConsumer.storeOffsets',
                        error: e.toString(),
                        topicPartition,
                        consumerGroupId: this._consumerGroupId,
                    });
                }
            }
        } else {
            this._log.debug('No offsets to store', {
                method: 'LogConsumer.storeOffsets',
                topic: this._topic,
                consumerGroupId: this._consumerGroupId,
            });
        }
        // batch fully processed: release any pending drain
        this.emit(EVENTS.DRAINED);
    }

    _hasUnprocessedMessages() {
        return this._topicPartition?.length > 0;
    }

    /**
     * LogConsumer is considered ready if it is connected
     * to the Kafka broker and ready to consume messages.
     * @returns {boolean}
     */
    isReady() {
        return this._consumer.isConnected();
    }

    /**
     * Closes the consumer connection, while making sure
     * that all messages are processed and offsets are committed.
     * @param {Function} cb
     */
    close(cb) {
        async.series([
            next => {
                if (this._consumer?.isConnected()) {
                    const subscription = this._consumer.subscription() || [];
                    if (subscription.length > 0) {
                        // we first unsubscribe from the topic
                        // to initiate the rebalance process
                        // allowing the message processing to finish
                        // and offsets to be committed
                        this.once(EVENTS.UNASSIGNED, () => next());
                        this._consumer.unsubscribe();
                        return null;
                    }
                }
                return next();
            },
            next => {
                if (this._consumer?.isConnected()) {
                    this._consumer.once('disconnected', () => next());
                    this._consumer.disconnect();
                    return null;
                }
                return next();
            },
        ], cb);
    }
}

module.exports = LogConsumer;
