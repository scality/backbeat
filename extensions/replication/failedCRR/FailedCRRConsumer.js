'use strict'; // eslint-disable-line strict

const { StatsModel } = require('arsenal').metrics;
const Logger = require('werelogs').Logger;
const redisClient = require('../../replication/utils/getRedisClient')();

const FailedCRRProducer = require('./FailedCRRProducer');
const BackbeatConsumer = require('../../../lib/BackbeatConsumer');
const BackbeatTask = require('../../../lib/tasks/BackbeatTask');
const config = require('../../../conf/Config');

// BackbeatConsumer constant defaults
const CONSUMER_FETCH_MAX_BYTES = 5000020;
const CONCURRENCY = 10;

class FailedCRRConsumer {
    /**
     * Create the retry consumer.
     */
    constructor() {
        this._repConfig = config.extensions.replication;
        this._kafkaConfig = config.kafka;
        this._topic = config.extensions.replication.replicationFailedTopic;
        this.logger = new Logger('Backbeat:FailedCRRConsumer');
        this._failedCRRProducer = new FailedCRRProducer(this._kafkaConfig);
        this._backbeatTask = new BackbeatTask();
        this._statsClient = new StatsModel(redisClient);
    }

    /**
     * Start the retry consumer by subscribing to the retry kafka topic. Setup
     * the failed CRR producer for pushing any failed redis operations back to
     * the queue.
     * @param {Function} cb - The callback function
     * @return {undefined}
     */
    start(cb) {
        const consumer = new BackbeatConsumer({
            kafka: {
                hosts: this._kafkaConfig.hosts,
                site: this._kafkaConfig.site,
            },
            topic: this._topic,
            groupId: 'backbeat-retry-group',
            concurrency: CONCURRENCY,
            queueProcessor: this.processKafkaEntry.bind(this),
            fetchMaxBytes: CONSUMER_FETCH_MAX_BYTES,
        });
        consumer.on('error', () => {});
        consumer.on('ready', () => {
            consumer.subscribe();
            this.logger.info('retry consumer is ready to consume entries');
        });
        return this._failedCRRProducer.setupProducer(err => {
            if (err) {
                this.logger.error('could not setup producer', {
                    method: 'FailedCRRConsumer.processKafkaEntry',
                    error: err,
                });
                return cb(err);
            }
            return cb();
        });
    }

    /**
     * Process an entry from the retry topic, and add the member in a Redis
     * sorted set.
     * @param {Object} kafkaEntry - The entry from the retry topic
     * @param {function} cb - The callback function
     * @return {undefined}
     */
    processKafkaEntry(kafkaEntry, cb) {
        const log = this.logger.newRequestLogger();
        let data;
        try {
            data = JSON.parse(kafkaEntry.value);
        } catch (err) {
            log.error('error processing retry entry', {
                method: 'FailedCRRConsumer.processKafkaEntry',
                error: err,
            });
            log.end();
            return cb();
        }
        return this._addSortedSetMember(data, kafkaEntry, log, cb);
    }

    /**
     * Attempt to add the Redis sorted set member, using an exponential backoff
     * should the set fail. If the backoff time is exceeded, push the entry back
     * into the retry entry topic for a later attempt.
     * @param {Object} data - The field and value for the Redis hash
     * @param {Object} kafkaEntry - The entry from the retry topic
     * @param {Werelogs} log - The werelogs logger
     * @param {Function} cb - The callback function
     * @return {undefined}
     */
    _addSortedSetMember(data, kafkaEntry, log, cb) {
        this._backbeatTask.retry({
            actionDesc: 'add redis sorted set member',
            logFields: {},
            actionFunc: done => this._addSortedSetMemberOnce(data, done),
            shouldRetryFunc: err => err.retryable,
            log,
        }, err => {
            if (err && err.retryable === true) {
                log.info('publishing entry back into the kafka queue');
                const entry = Buffer.from(kafkaEntry.value).toString();
                this._failedCRRProducer.publishFailedCRREntry(entry);
                return cb();
            }
            if (err) {
                log.error('could not add redis sorted set member', {
                    error: err,
                    data,
                });
                return cb(err);
            }
            log.info('successfully added redis sorted set member');
            return cb();
        });
    }

    /**
     * Attempt to add the sorted set member.
     * @param {Object} data - The key and value for the Redis key
     * @param {Function} cb - The callback function
     * @return {undefined}
     */
    _addSortedSetMemberOnce(data, cb) {
        const { key, member, score } = data;
        return this._statsClient.addToSortedSet(key, score, member, err => {
            if (err) {
                return cb({ retryable: true });
            }
            return cb();
        });
    }
}

module.exports = FailedCRRConsumer;
