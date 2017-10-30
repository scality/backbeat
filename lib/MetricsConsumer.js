'use strict'; // eslint-disable-line strict

const Logger = require('werelogs').Logger;
const { RedisClient, StatsClient } = require('arsenal').metrics;

const BackbeatConsumer = require('./BackbeatConsumer');
const redisKeys = require('../extensions/replication/constants').redisKeys;

// StatsClient constant defaults
const INTERVAL = 300; // 5 minutes;
const EXPIRY = 900; // 15 minutes

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
     * @param {object} zkConfig - zookeeper configurations
     * @param {string} zkConfig.connectionString - zookeeper connection string
     *   as "host:port[/chroot]"
     */
    constructor(rConfig, mConfig, zkConfig) {
        this.mConfig = mConfig;
        this.zkConfig = zkConfig;

        this.logger = new Logger('Backbeat:MetricsConsumer');

        const redisClient = new RedisClient(rConfig, this.logger);
        this._statsClient = new StatsClient(redisClient, INTERVAL,
            EXPIRY);
    }

    start() {
        const consumer = new BackbeatConsumer({
            zookeeper: { connectionString: this.zkConfig.connectionString },
            topic: this.mConfig.topic,
            groupId: 'backbeat-metrics-group',
            concurrency: CONCURRENCY,
            queueProcessor: this.processKafkaEntry.bind(this),
            fetchMaxBytes: CONSUMER_FETCH_MAX_BYTES,
        });
        consumer.on('error', () => {});
        consumer.subscribe();

        this.logger.info('metrics processor is ready to consume entries');
    }

    processKafkaEntry(kafkaEntry, done) {
        let data;
        try {
            data = JSON.parse(kafkaEntry.value);
        } catch (err) {
            this.logger.error('error processing metrics entry', {
                method: 'MetricsConsumer.processKafkaEntry',
                error: err,
            });
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
        if (data.type === 'processed') {
            this._sendRequest(redisKeys.opsDone, data.ops);
            this._sendRequest(redisKeys.bytesDone, data.bytes);
        } else if (data.type === 'queued') {
            this._sendRequest(redisKeys.ops, data.ops);
            this._sendRequest(redisKeys.bytes, data.bytes);
        } else {
            // unknown type
            this.logger.error('unknown type field encountered in metrics '
            + 'consumer', {
                method: 'MetricsConsumer.processKafkaEntry',
                dataType: data.type,
                data,
            });
        }
        return done();
    }

    _sendRequest(key, value) {
        this._statsClient.reportNewRequest(key, value);
    }
}

module.exports = MetricsConsumer;
