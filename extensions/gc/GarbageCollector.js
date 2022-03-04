'use strict'; // eslint-disable-line

const http = require('http');
const { EventEmitter } = require('events');

const errors = require('arsenal').errors;
const Logger = require('werelogs').Logger;

const BackbeatConsumer = require('../../lib/BackbeatConsumer');
const ActionQueueEntry = require('../../lib/models/ActionQueueEntry');
const GarbageCollectorTask = require('./tasks/GarbageCollectorTask');

/**
 * @class GarbageCollector
 *
 * @classdesc Background task that deletes unused data blobs to
 * reclaim storage space
 */
class GarbageCollector extends EventEmitter {

    /**
     * @constructor
     * @param {Object} params - constructor params
     * @param {Object} params.kafkaConfig - kafka configuration object
     * @param {string} params.kafkaConfig.hosts - list of kafka
     *   brokers as "host:port[,host:port...]"
     * @param {Object} params.s3Config - S3 configuration
     * @param {Object} params.s3Config.host - s3 endpoint host
     * @param {Number} params.s3Config.port - s3 endpoint port
     * @param {Object} params.gcConfig - garbage collector
     * configuration object
     * @param {String} params.gcConfig.topic - garbage collector kafka
     * topic
     * @param {Object} params.gcConfig.auth - garbage collector
     *   authentication object
     * @param {Object} params.gcConfig.consumer - kafka consumer
     * object
     * @param {String} params.gcConfig.consumer.groupId - kafka
     * consumer group id
     * @param {number} [params.gcConfig.consumer.retry.timeoutS] -
     *  retry timeout in secs.
     * @param {number} [params.gcConfig.consumer.retry.maxRetries] -
     *  max retries before giving up
     * @param {Object} [params.gcConfig.consumer.retry.backoff] -
     *  backoff params
     * @param {number} [params.gcConfig.consumer.retry.backoff.min] -
     *  min. backoff in ms.
     * @param {number} [params.gcConfig.consumer.retry.backoff.max] -
     *  max. backoff in ms.
     * @param {number} [params.gcConfig.consumer.retry.backoff.jitter] -
     *  randomness
     * @param {number} [params.gcConfig.consumer.retry.backoff.factor] -
     *  backoff factor
     * @param {Number} [params.gcConfig.consumer.concurrency] - number
     *  of max allowed concurrent operations
     * @param {String} [params.transport='http'] - transport
     */
    constructor(params) {
        super();

        this._kafkaConfig = params.kafkaConfig;
        this._s3Config = params.s3Config;
        this._gcConfig = params.gcConfig;
        this._transport = params.transport || 'http';
        this._consumer = null;
        this._started = false;
        this._isActive = false;

        this._httpAgent = new http.Agent({ keepAlive: true });
        this._logger = new Logger('Backbeat:GC');
    }

    /**
     * Start kafka consumer. Emits a 'ready' event when
     * consumer is ready.
     *
     * @return {undefined}
     */
    start() {
        let consumerReady = false;
        this._consumer = new BackbeatConsumer({
            kafka: {
                hosts: this._kafkaConfig.hosts,
                site: this._kafkaConfig.site,
            },
            topic: this._gcConfig.topic,
            groupId: this._gcConfig.consumer.groupId,
            concurrency: this._gcConfig.consumer.concurrency,
            queueProcessor: this.processKafkaEntry.bind(this),
        });
        this._consumer.on('error', () => {
            if (!consumerReady) {
                this._logger.error('garbage collector failed to start the ' +
                                   'kafka consumer');
                process.exit(1);
            }
        });
        this._consumer.on('ready', () => {
            consumerReady = true;
            this._consumer.subscribe();
            this._logger.info('garbage collector service successfully started');
            return this.emit('ready');
        });
    }

    /**
     * Close the lifecycle consumer
     * @param {function} cb - callback function
     * @return {undefined}
     */
    close(cb) {
        this._logger.debug('closing garbage collector consumer');
        this._consumer.close(cb);
    }

    processKafkaEntry(kafkaEntry, done) {
        this._logger.debug('processing kafka entry');

        const actionEntry = ActionQueueEntry.createFromKafkaEntry(kafkaEntry);
        if (actionEntry.error) {
            this._logger.error(
                'malformed kafka entry from garbage collector topic',
                { error: actionEntry.error.message });
            return process.nextTick(() => done(errors.InternalError));
        }
        const task = new GarbageCollectorTask(this);
        return task.processActionEntry(actionEntry, done);
    }

    getStateVars() {
        return {
            s3Config: this._s3Config,
            gcConfig: this._gcConfig,
            transport: this._transport,
            httpAgent: this._httpAgent,
            logger: this._logger,
        };
    }

    isReady() {
        return this._consumer && this._consumer.isReady();
    }
}

module.exports = GarbageCollector;
