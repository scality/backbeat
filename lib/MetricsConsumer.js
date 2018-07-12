'use strict'; // eslint-disable-line strict

const Logger = require('werelogs').Logger;
const { RedisClient, StatsModel } = require('arsenal').metrics;

const BackbeatConsumer = require('./BackbeatConsumer');
const redisKeys = require('../extensions/replication/constants').redisKeys;

// StatsClient constant defaults for site metrics
const INTERVAL = 300; // 5 minutes;
const EXPIRY = 900; // 15 minutes
const OBJECT_MONITORING_EXPIRY = 86400; // 24 hours

// BackbeatConsumer constant defaults
const CONSUMER_FETCH_MAX_BYTES = 5000020;
const CONCURRENCY = 10;

class MetricsConsumer {
    /**
     * @constructor
     * @param {object} rLocalCacheConfig - redis local cache configuration
     * @param {string} rLocalCacheConfig.host - redis local cache host
     * @param {number} rLocalCacheConfig.port - redis local cache port
     * @param {object} mConfig - metrics configurations
     * @param {string} mConfig.topic - metrics topic name
     * @param {object} kafkaConfig - kafka configurations
     * @param {string} kafkaConfig.hosts - kafka hosts
     *   as "host:port[/chroot]"
     */
    constructor(rLocalCacheConfig, mConfig, kafkaConfig) {
        this.mConfig = mConfig;
        this.kafkaConfig = kafkaConfig;

        this.logger = new Logger('Backbeat:MetricsConsumer');
        const redisClient = new RedisClient(rLocalCacheConfig, this.logger);
        this._siteStatsClient = new StatsModel(redisClient, INTERVAL,
            (EXPIRY + INTERVAL));
        this._objectStatsClient = new StatsModel(redisClient, INTERVAL,
            (OBJECT_MONITORING_EXPIRY + INTERVAL));
    }

    start() {
        let consumerReady = false;
        const consumer = new BackbeatConsumer({
            kafka: { hosts: this.kafkaConfig.hosts },
            topic: this.mConfig.topic,
            groupId: 'backbeat-metrics-group',
            concurrency: CONCURRENCY,
            queueProcessor: this.processKafkaEntry.bind(this),
            fetchMaxBytes: CONSUMER_FETCH_MAX_BYTES,
        });
        consumer.on('error', () => {
            if (!consumerReady) {
                this.logger.fatal('error starting metrics consumer');
                process.exit(1);
            }
        });
        consumer.on('ready', () => {
            consumerReady = true;
            consumer.subscribe();
            this.logger.info('metrics processor is ready to consume entries');
        });
    }
    _removePreferredRead(site) {
        return site.endsWith(':preferred_read') ?
            site.split(':')[0] : site;
    }

    _sendSiteLevelRequests(data) {
        const { type, site, ops, bytes } = data;
        if (type === 'completed') {
            this._sendSiteLevelRequest(`${site}:${redisKeys.opsDone}`, ops);
            this._sendSiteLevelRequest(`${site}:${redisKeys.bytesDone}`, bytes);
        } else if (type === 'failed') {
            this._sendSiteLevelRequest(`${site}:${redisKeys.opsFail}`, ops);
            this._sendSiteLevelRequest(`${site}:${redisKeys.bytesFail}`, bytes);
        } else if (type === 'queued') {
            this._sendSiteLevelRequest(`${site}:${redisKeys.ops}`, ops);
            this._sendSiteLevelRequest(`${site}:${redisKeys.bytes}`, bytes);
        }
        return undefined;
    }

    _sendObjectLevelRequests(data) {
        const { type, site, bytes, bucketName, objectKey, versionId } = data;
        if (type === 'completed') {
            const key = `${site}:${bucketName}:${objectKey}:` +
                `${versionId}:${redisKeys.objectBytesDone}`;
            this._sendObjectLevelRequest(key, bytes);
        } else if (type === 'queued') {
            const key = `${site}:${bucketName}:${objectKey}:` +
                `${versionId}:${redisKeys.objectBytes}`;
            this._sendObjectLevelRequest(key, bytes);
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
                type: 'processed'
            }
        */
        const operationTypes = ['completed', 'failed', 'queued'];
        const isValidType = operationTypes.includes(data.type);
        if (!isValidType) {
            log.error('unknown type field encountered in metrics consumer', {
                method: 'MetricsConsumer.processKafkaEntry',
                dataType: data.type,
                data,
            });
            log.end();
            return done();
        }
        if (data.site) {
            data.site = this._removePreferredRead(data.site);
        }
        if (data.bucketName && data.objectKey && data.versionId) {
            this._sendObjectLevelRequests(data);
        } else {
            this._sendSiteLevelRequests(data);
        }
        log.end();
        return done();
    }

    _sendSiteLevelRequest(key, value) {
        this._siteStatsClient.reportNewRequest(key, value);
    }

    _sendObjectLevelRequest(key, value) {
        this._objectStatsClient.reportNewRequest(key, value);
    }
}

module.exports = MetricsConsumer;
