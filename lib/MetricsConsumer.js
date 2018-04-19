'use strict'; // eslint-disable-line strict

const Logger = require('werelogs').Logger;
const { RedisClient } = require('arsenal').metrics;

const StatsModel = require('./models/StatsModel');
const BackbeatConsumer = require('./BackbeatConsumer');
const redisKeys = require('../extensions/replication/constants').redisKeys;

// BackbeatConsumer constant defaults
const CONSUMER_FETCH_MAX_BYTES = 5000020;
const CONCURRENCY = 10;

class MetricsConsumer {
    /**
     * @constructor
     * @param {object} rConfig - redis configurations
     * @param {string} rConfig.host - redis host
     * @param {number} rConfig.port - redis port
     * @param {object} mConfig - metrics configurations
     * @param {string} mConfig.topic - metrics topic name
     * @param {object} kafkaConfig - kafka configurations
     * @param {string} kafkaConfig.hosts - kafka hosts
     *   as "host:port[/chroot]"
     * @param {number} mConfig.intervalS - metrics interval for redis keys in
     *   seconds
     * @param {number} mConfig.expiryS - metrics expiry for redis keys in
     *   seconds
     */
    constructor(rConfig, mConfig, kafkaConfig) {
        this.mConfig = mConfig;
        this.kafkaConfig = kafkaConfig;

        this.logger = new Logger('Backbeat:MetricsConsumer');

        const interval = mConfig.intervalS;
        const expiry = mConfig.expiryS;

        const redisClient = new RedisClient(rConfig, this.logger);
        this._statsClient = new StatsModel(redisClient, interval, expiry);
    }

    start() {
        const consumer = new BackbeatConsumer({
            kafka: { hosts: this.kafkaConfig.hosts },
            topic: this.mConfig.topic,
            groupId: 'backbeat-metrics-group',
            concurrency: CONCURRENCY,
            queueProcessor: this.processKafkaEntry.bind(this),
            fetchMaxBytes: CONSUMER_FETCH_MAX_BYTES,
        });
        consumer.on('error', () => {});
        consumer.on('ready', () => {
            consumer.subscribe();
            this.logger.info('metrics processor is ready to consume entries');
        });
    }

    processKafkaEntry(kafkaEntry, done) {
        const log = this.logger.newRequestLogger();
        let data;
        try {
            data = JSON.parse(kafkaEntry.value);
        } catch (err) {
            log.error('error processing metrics entry', {
                method: 'MetricsConsumer.processKafkaEntry',
                error: err,
            });
            log.end();
            return done();
        }
        /*
            data = {
                timestamp: 1509416671977,
                ops: 5,
                bytes: 195,
                extension: 'crr',
                type: 'processed'
            }
        */
        const site = data.site;
        if (data.type === 'processed') {
            this._sendRequest(`${site}:${redisKeys.opsDone}`, data.ops);
            this._sendRequest(`${site}:${redisKeys.bytesDone}`, data.bytes);
        } else if (data.type === 'queued') {
            this._sendRequest(`${site}:${redisKeys.ops}`, data.ops);
            this._sendRequest(`${site}:${redisKeys.bytes}`, data.bytes);
        } else {
            // unknown type
            log.error('unknown type field encountered in metrics '
            + 'consumer', {
                method: 'MetricsConsumer.processKafkaEntry',
                dataType: data.type,
                data,
            });
        }
        log.end();
        return done();
    }

    _sendRequest(key, value) {
        this._statsClient.reportNewRequest(key, value);
    }
}

module.exports = MetricsConsumer;
