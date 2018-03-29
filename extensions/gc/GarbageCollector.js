'use strict'; // eslint-disable-line

const async = require('async');

const errors = require('arsenal').errors;
const Logger = require('werelogs').Logger;

const BackbeatConsumer = require('../../../lib/BackbeatConsumer');
const GarbageCollectorTask = require('./tasks/GarbageCollectorTask');

const DEFAULT_CONCURRENCY = 10;

/**
 * @class GarbageCollector
 *
 * @classdesc Background task that deletes unused data blobs to
 * reclaim storage space
 */
class GarbageCollector {

    /**
     * @constructor
     * @param {Object} kafkaConfig - kafka configuration object
     * @param {string} kafkaConfig.hosts - list of kafka brokers
     *   as "host:port[,host:port...]"

     * @param {Object} gcConfig - garbage collector configuration
     * object
     * @param {String} gcConfig.topic - garbage collector kafka topic
     * @param {Number} [gcConfig.concurrency=10] - maximum
     *   number of concurrent GC operations allowed
     */
    constructor(kafkaConfig, gcConfig) {
        this.kafkaConfig = kafkaConfig;
        this.gcConfig = gcConfig;
        this._concurrency = this.gcConfig.concurrency || DEFAULT_CONCURRENCY;
        this._consumer = null;
        this._started = false;
        this._isActive = false;

        this.logger = new Logger('Backbeat:GC');
    }

    /**
     * Start kafka consumer. Emits a 'ready' event when
     * consumer is ready.
     *
     * @return {undefined}
     */
    start() {
        this._consumer = new BackbeatConsumer({
            kafka: { hosts: this.kafkaConfig.hosts },
            topic: this.gcConfig.topic,
            groupId: this.gcConfig.consumer.groupId,
            concurrency: this.lcConfig.consumer.concurrency,
            queueProcessor: this.processKafkaEntry.bind(this),
        });
        this._consumer.on('error', () => {});
        this._consumer.on('ready', () => {
            this._consumer.subscribe();
            this.logger.info('garbage collector successfully started');
            return this.emit('ready');
        });
    }

    /**
     * Close the lifecycle consumer
     * @param {function} cb - callback function
     * @return {undefined}
     */
    close(cb) {
        this.logger.debug('closing garbage collector consumer');
        this._consumer.close(cb);
    }

    processKafkaEntry(kafkaEntry, done) {
        this.logger.debug('processing kafka entry');

        let entryData;
        try {
            entryData = JSON.parse(kafkaEntry.value);
        } catch (err) {
            this.logger.error('error processing garbage collector entry',
                              { error: err });
            return process.nextTick(() => done(errors.InternalError));
        }
        const task = new GarbageCollectorTask(this);
        return task.processQueueEntry(entryData, done);
    }

    getStateVars() {
        return {
            gcConfig: this.gcConfig,
            logger: this.logger,
        };
    }
}

module.exports = GarbageCollector;
