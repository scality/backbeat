'use strict';

const { EventEmitter } = require('events');
const Logger = require('werelogs').Logger;
const async = require('async');
const { CODES } = require('node-rdkafka');
const { ZenkoMetrics } = require('arsenal').metrics;

const BackbeatConsumer = require('../../../lib/BackbeatConsumer');
const messageUtil = require('../utils/message');
const DeliveryProducerPool = require('./DeliveryProducerPool');

// target label used when the entry could not be parsed, so no destination
// is known for it
const UNKNOWN_TARGET = 'unknown';

const deliveredEvents = ZenkoMetrics.createCounter({
    name: 's3_notification_delivery_worker_delivered_total',
    help: 'Total number of notifications delivered to an external destination',
    labelNames: ['target'],
});

const droppedEvents = ZenkoMetrics.createCounter({
    name: 's3_notification_delivery_worker_dropped_total',
    help: 'Total number of notifications dropped without being delivered',
    labelNames: ['target', 'reason'],
});

const deliveryDelay = ZenkoMetrics.createHistogram({
    name: 's3_notification_delivery_worker_delivery_delay_seconds',
    help: 'Time between sending a notification and receiving its delivery report',
    labelNames: ['target', 'status'],
    buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30],
});

function onDelivered(target) {
    deliveredEvents.inc({ target });
}

function onDropped(target, reason) {
    droppedEvents.inc({ target, reason });
}

function observeDelay(target, status, delay) {
    deliveryDelay.observe({ target, status }, delay);
}

class DeliveryWorker extends EventEmitter {
    /**
     * Create a delivery worker, consuming a shared delivery topic and
     * dispatching each entry to the external destination named by the entry
     * itself.
     *
     * Unlike the per-destination queue processor, one worker serves every
     * destination: the destination id and the notification configuration id
     * are carried by the record, so no bucket notification configuration
     * lookup is needed here.
     *
     * @constructor
     * @param {Object} kafkaConfig - kafka configuration object
     * @param {string} kafkaConfig.hosts - list of kafka brokers
     *   as "host:port[,host:port...]"
     * @param {Object} notifConfig - notification configuration object
     * @param {Object[]} notifConfig.destinations - destination configurations
     * @param {Object} notifConfig.deliveryPool - delivery pool configuration
     * @param {String} notifConfig.deliveryPool.topic - delivery topic name
     * @param {String} notifConfig.deliveryPool.groupId - kafka consumer group
     *   id, shared by every worker of the pool
     * @param {number} notifConfig.deliveryPool.concurrency - how many
     *   notifications can be in flight at once
     * @param {number} notifConfig.deliveryPool.maxQueued - how many
     *   notifications can be queued for processing
     */
    constructor(kafkaConfig, notifConfig) {
        super();
        this.kafkaConfig = kafkaConfig;
        this.notifConfig = notifConfig;
        this.deliveryPoolConfig = notifConfig.deliveryPool;
        this._destinationsById = {};
        (notifConfig.destinations || []).forEach(destConfig => {
            this._destinationsById[destConfig.resource] = destConfig;
        });
        this._consumer = null;
        this._producerPool = null;

        this.logger = new Logger('Backbeat:Notification:DeliveryWorker');
    }

    /**
     * Compute the ordering key of a consumed entry.
     *
     * The default ordering of BackbeatConsumer is by kafka key, which would
     * serialize every notification of a whole destination. Ordering per
     * object keeps the per-object ordering guarantee while letting objects of
     * the same destination be delivered in parallel.
     *
     * The parsed entry is stashed on the entry object, which is the same
     * object later handed to processKafkaEntry, so the payload is parsed once.
     *
     * @param {object} ctx - task context pushed by BackbeatConsumer
     * @return {string|undefined} ordering key, or undefined to leave the
     *   entry unordered
     */
    _orderBy(ctx) {
        const entry = ctx && ctx.entry;
        if (!entry) {
            return undefined;
        }
        let parsed;
        try {
            parsed = JSON.parse(entry.value);
        } catch {
            // leave it unordered, processKafkaEntry counts the drop
            return undefined;
        }
        entry._notifEntry = parsed;
        return `${parsed.destinationId}|${parsed.bucket}/${parsed.key}`;
    }

    /**
     * Start the producer pool and the kafka consumer. Emits a 'ready' event
     * when the consumer is ready.
     *
     * @param {object} [options] options object
     * @param {boolean} [options.disableConsumer] - true to disable startup of
     *   the consumer (for testing: one has to call processKafkaEntry()
     *   explicitly)
     * @param {function} done callback
     * @return {undefined}
     */
    start(options, done) {
        this._producerPool = new DeliveryProducerPool({
            destinationsById: this._destinationsById,
            deliveryPoolConfig: this.deliveryPoolConfig,
            logger: this.logger,
        });
        this._producerPool.start();
        async.series([
            next => {
                if (options && options.disableConsumer) {
                    this.emit('ready');
                    return process.nextTick(next);
                }
                const { topic, groupId, concurrency, maxQueued } = this.deliveryPoolConfig;
                this._consumer = new BackbeatConsumer({
                    kafka: {
                        hosts: this.kafkaConfig.hosts,
                        site: this.kafkaConfig.site,
                        compressionType: this.kafkaConfig.compressionType,
                        requiredAcks: this.kafkaConfig.requiredAcks,
                    },
                    topic,
                    groupId,
                    concurrency,
                    maxQueued,
                    // librdkafka defaults to 'latest': a worker joining with a
                    // fresh group would skip everything already in the topic
                    fromOffset: 'earliest',
                    queueProcessor: this.processKafkaEntry.bind(this),
                    orderByFunc: ctx => this._orderBy(ctx),
                });
                this._consumer.on('error', err => {
                    this.logger.error('error starting notification delivery consumer',
                        { method: 'DeliveryWorker.start', error: err.message });
                    // crash if got error at startup
                    if (!this.isReady()) {
                        return next(err);
                    }
                    return undefined;
                });
                this._consumer.on('ready', () => {
                    this._consumer.subscribe();
                    this.logger.info('delivery worker is ready to consume ' +
                        'notification entries');
                    this.emit('ready');
                    return next();
                });
                return undefined;
            },
        ], err => {
            if (err) {
                this.logger.error('error starting notification delivery worker',
                    { method: 'DeliveryWorker.start', error: err.message });
                return done(err);
            }
            return done();
        });
    }

    /**
     * Stop the kafka consumer and close every pooled producer
     *
     * @param {function} done - callback
     * @return {undefined}
     */
    stop(done) {
        async.series([
            next => {
                if (this._consumer) {
                    return this._consumer.close(next);
                }
                return process.nextTick(next);
            },
            next => {
                if (this._producerPool) {
                    return this._producerPool.closeAll(next);
                }
                return process.nextTick(next);
            },
        ], err => done(err));
    }

    /**
     * Process a kafka entry: deliver it to the external destination named by
     * the entry.
     *
     * The callback is held until the delivery report is received, so that the
     * consumer offset is only committed once the notification has left the
     * process. A delivery failure is counted and the entry is dropped: the
     * callback is never called with an error, which the consumer would report
     * as a consumer level error.
     *
     * @param {object} kafkaEntry - entry consumed from the delivery topic
     * @param {function} done - callback function
     * @return {undefined}
     */
    processKafkaEntry(kafkaEntry, done) {
        let parsed = kafkaEntry._notifEntry;
        if (!parsed) {
            try {
                parsed = JSON.parse(kafkaEntry.value);
            } catch (error) {
                this.logger.error('error parsing JSON entry', {
                    method: 'DeliveryWorker.processKafkaEntry',
                    error: error.message,
                });
                onDropped(UNKNOWN_TARGET, 'parse_error');
                return done();
            }
        }
        const { destinationId, bucket, key } = parsed;
        const destConfig = this._destinationsById[destinationId];
        if (!destConfig) {
            this.logger.warn('no destination configured for entry, dropping', {
                method: 'DeliveryWorker.processKafkaEntry',
                destinationId,
                bucket,
                key,
            });
            onDropped(destinationId || UNKNOWN_TARGET, 'unknown_destination');
            return done();
        }
        return this._producerPool.get(destinationId, (err, producer) => {
            if (err) {
                this.logger.error('could not get a producer for destination, dropping', {
                    method: 'DeliveryWorker.processKafkaEntry',
                    destinationId,
                    bucket,
                    key,
                    error: err.message,
                });
                onDropped(destinationId, 'producer_error');
                return done();
            }
            const message = messageUtil.transformToSpec(parsed);
            const msg = {
                // for Kafka keyed partitioning, to map a particular bucket
                // and key to a partition
                key: `${bucket}/${key}`,
                message: JSON.stringify(message),
            };
            const startTime = Date.now();
            this.logger.debug('sending message to external destination', {
                method: 'DeliveryWorker.processKafkaEntry',
                destinationId,
                bucket,
                key,
                eventType: parsed.eventType,
            });
            // one entry per send call: BackbeatProducer aggregates delivery
            // reports per send, batching would conflate outcomes of entries
            // owned by different consumer offsets
            return producer.send([msg], sendErr => {
                const delay = (Date.now() - startTime) / 1000;
                if (sendErr) {
                    const reason = sendErr.code === CODES.ERRORS.ERR__MSG_TIMED_OUT ?
                        'delivery_timeout' : 'delivery_error';
                    this.logger.error('error delivering notification to external destination', {
                        method: 'DeliveryWorker.processKafkaEntry',
                        destinationId,
                        bucket,
                        key,
                        reason,
                        error: sendErr.message,
                    });
                    observeDelay(destinationId, 'failure', delay);
                    onDropped(destinationId, reason);
                    return done();
                }
                observeDelay(destinationId, 'success', delay);
                onDelivered(destinationId);
                return done();
            });
        });
    }

    /**
     * Checks if the delivery worker is ready to consume
     *
     * @returns {boolean} is delivery worker ready
     */
    isReady() {
        return !!(this._consumer && this._consumer.isReady());
    }

    /**
     * Handle ProbeServer metrics
     *
     * @param {http.HTTPServerResponse} res - HTTP Response to respond with
     * @param {Logger} log - Logger
     * @returns {undefined}
     */
    async handleMetrics(res, log) {
        log.debug('metrics requested');
        res.writeHead(200, {
            'Content-Type': ZenkoMetrics.asPrometheusContentType(),
        });
        const metrics = await ZenkoMetrics.asPrometheus();
        res.end(metrics);
    }
}

module.exports = DeliveryWorker;
