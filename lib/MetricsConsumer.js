'use strict'; // eslint-disable-line strict

const Logger = require('werelogs').Logger;
const { RedisClient, StatsModel } = require('arsenal').metrics;

const BackbeatConsumer = require('./BackbeatConsumer');
const redisKeys = require('../extensions/replication/constants').redisKeys;

// StatsClient constant defaults
const INTERVAL = 300; // 5 minutes;
const EXPIRY = 86400; // 24 hours

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
     */
    constructor(rConfig, mConfig, kafkaConfig) {
        this.mConfig = mConfig;
        this.kafkaConfig = kafkaConfig;

        this.logger = new Logger('Backbeat:MetricsConsumer');

        const redisClient = new RedisClient(rConfig, this.logger);
        this._statsClient = new StatsModel(redisClient, INTERVAL,
            EXPIRY);
    }

    /**
     * List of valid "type" field values for metric kafka entries
     * @param {string} type - type to check
     * @return {boolean} true if type is a valid metric type
     */
    static isValidMetricType(type) {
        const validTypes = ['completed', 'failed', 'queued', 'pendingOnly'];
        return validTypes.includes(type);
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

    _reportPending(site, redisKeys, ops, bytes) {
        if (ops > 0) {
            this._sendRequest('incrementKey', site, redisKeys, 'opsPending',
                ops);
        }
        if (ops < 0) {
            this._sendRequest('decrementKey', site, redisKeys, 'opsPending',
                Math.abs(ops));
        }
        if (bytes > 0) {
            this._sendRequest('incrementKey', site, redisKeys, 'bytesPending',
                bytes);
        }
        if (bytes < 0) {
            this._sendRequest('decrementKey', site, redisKeys, 'bytesPending',
                Math.abs(bytes));
        }
    }

    _sendSiteLevelRequests(data) {
        const { type, site, ops, bytes } = data;

        if (type === 'completed') {
            // Pending metrics
            this._reportPending(site, redisKeys, -ops, -bytes);
            // Other metrics
            this._sendRequest('reportNewRequest', site, redisKeys, 'opsDone',
                ops);
            this._sendRequest('reportNewRequest', site, redisKeys, 'bytesDone',
                bytes);
        } else if (type === 'failed') {
            // Pending metrics
            this._reportPending(site, redisKeys, -ops, -bytes);
            // Other metrics
            this._sendRequest('reportNewRequest', site, redisKeys, 'opsFail',
                ops);
            this._sendRequest('reportNewRequest', site, redisKeys, 'bytesFail',
                bytes);
        } else if (type === 'queued') {
            // Pending metrics
            this._reportPending(site, redisKeys, ops, bytes);
            // Other metrics
            this._sendRequest('reportNewRequest', site, redisKeys, 'ops', ops);
            this._sendRequest('reportNewRequest', site, redisKeys, 'bytes',
                bytes);
        } else if (type === 'pendingOnly') {
            this._reportPending(site, redisKeys, ops, bytes);
        }
        return undefined;
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
                type: 'queued'
            }
        */
        const isValidType = MetricsConsumer.isValidMetricType(data.type);
        if (!isValidType) {
            log.end().error('unknown type field encountered in metrics ' +
            'consumer', {
                method: 'MetricsConsumer.processKafkaEntry',
                dataType: data.type,
                data,
            });
            return done();
        }
        this._sendSiteLevelRequests(data);
        log.end();
        return done();
    }

    _sendRequest(action, site, redisKeys, keyType, value) {
        if (redisKeys[keyType]) {
            this._statsClient[action](`${site}:${redisKeys[keyType]}`,
                value || 0);
        }
    }
}

module.exports = MetricsConsumer;
