const async = require('async');
const { ZenkoMetrics } = require('arsenal').metrics;
const errors = require('arsenal').errors;

const DeliveryKafkaProducer = require('./DeliveryKafkaProducer');

const lanesGauge = ZenkoMetrics.createGauge({
    name: 's3_notification_delivery_worker_lanes',
    help: 'Number of notification deliveries currently in flight',
});

const producersGauge = ZenkoMetrics.createGauge({
    name: 's3_notification_delivery_worker_producers',
    help: 'Number of open producers per external destination endpoint',
    labelNames: ['endpoint'],
});

/**
 * A producer to one external destination, along with the bookkeeping the
 * pool needs to decide when it can be closed.
 */
class PooledProducer {
    /**
     * @constructor
     * @param {Object} params - constructor params
     * @param {string} params.destinationId - destination id (resource name)
     * @param {string} params.endpoint - kafka hosts string of the destination
     * @param {function} params.onSendStarted - called when a delivery starts
     * @param {function} params.onSendFinished - called when a delivery ends
     * @param {Logger} params.logger - logger object
     */
    constructor(params) {
        this.destinationId = params.destinationId;
        this.endpoint = params.endpoint;
        this.producer = null;
        this.ready = false;
        this.lastUsed = Date.now();
        this.inFlight = 0;
        this.waiters = [];
        this._onSendStarted = params.onSendStarted;
        this._onSendFinished = params.onSendFinished;
        this._log = params.logger;
    }

    attach(producer) {
        this.producer = producer;
    }

    markReady() {
        this.ready = true;
        this.lastUsed = Date.now();
    }

    addWaiter(cb) {
        this.waiters.push(cb);
    }

    /**
     * Hand this producer, or the error that prevented it from connecting, to
     * everyone that asked for it while it was connecting
     *
     * @param {Error} [err] - error that prevented the producer from connecting
     * @return {undefined}
     */
    flushWaiters(err) {
        const waiters = this.waiters;
        this.waiters = [];
        waiters.forEach(cb => (err ? cb(err) : cb(null, this)));
    }

    /**
     * True when this producer is holding no delivery, so closing it cannot
     * lose a delivery report that a consumer offset is waiting on
     *
     * @return {boolean} whether the producer can be closed
     */
    isIdle() {
        return this.ready && this.inFlight === 0;
    }

    /**
     * Send messages and keep track of the delivery being in flight
     *
     * @param {Object[]} messages - messages to send
     * @param {function} cb - callback called on the delivery report
     * @return {undefined}
     */
    send(messages, cb) {
        this.inFlight++;
        this.lastUsed = Date.now();
        this._onSendStarted();
        this.producer.send(messages, err => {
            this.inFlight--;
            this.lastUsed = Date.now();
            this._onSendFinished();
            cb(err);
        });
    }

    close(cb) {
        const done = cb || (() => {});
        if (!this.producer) {
            return process.nextTick(done);
        }
        return this.producer.close(err => {
            if (err) {
                this._log.error('error closing producer', {
                    method: 'PooledProducer.close',
                    destinationId: this.destinationId,
                    endpoint: this.endpoint,
                    error: err.message,
                });
            }
            done();
        });
    }
}

/**
 * Pool of producers to external destinations, keyed by destination id.
 *
 * Producers are created on demand and closed once they have been idle for
 * producerIdleMs, so that a worker consuming a shared topic only holds
 * connections to the destinations it actually delivers to.
 */
class DeliveryProducerPool {
    /**
     * @constructor
     * @param {Object} params - constructor params
     * @param {Object} params.destinationsById - destination configurations
     *   keyed by destination id (resource name)
     * @param {Object} params.deliveryPoolConfig - delivery pool configuration
     * @param {number} params.deliveryPoolConfig.deliveryTimeoutMs - time after
     *   which librdkafka expires a message that could not be delivered
     * @param {number} params.deliveryPoolConfig.producerIdleMs - time after
     *   which an unused producer is closed
     * @param {number} params.deliveryPoolConfig.maxProducers - maximum number
     *   of producers kept open at once
     * @param {Logger} params.logger - logger object
     */
    constructor(params) {
        const { deliveryTimeoutMs, producerIdleMs, maxProducers } = params.deliveryPoolConfig;
        this._destinationsById = params.destinationsById;
        this._deliveryTimeoutMs = deliveryTimeoutMs;
        this._producerIdleMs = producerIdleMs;
        this._maxProducers = maxProducers;
        this._log = params.logger;
        // destination id -> PooledProducer
        this._producers = new Map();
        // endpoints ever seen, to reset their gauge when they drop out
        this._knownEndpoints = new Set();
        this._inFlight = 0;
        this._reapTimer = null;
        this._closed = false;
    }

    /**
     * Start the periodic reaping of idle producers
     *
     * @return {undefined}
     */
    start() {
        if (this._reapTimer) {
            return;
        }
        this._reapTimer = setInterval(() => this._reapIdleProducers(),
            Math.max(1, Math.floor(this._producerIdleMs / 2)));
        // do not keep the process alive just for the reaper
        if (this._reapTimer.unref) {
            this._reapTimer.unref();
        }
    }

    /**
     * Get a producer for the given destination, creating and connecting it
     * if needed. Concurrent calls made while a producer is connecting are
     * queued and served with the same producer once it is ready.
     *
     * @param {string} destinationId - destination id (resource name)
     * @param {function} done - callback: done(err, producer), where producer
     *   exposes send(messages, cb)
     * @return {undefined}
     */
    get(destinationId, done) {
        if (this._closed) {
            return process.nextTick(() => done(errors.InternalError.customizeDescription(
                'delivery producer pool is closed')));
        }
        const existing = this._producers.get(destinationId);
        if (existing) {
            if (existing.ready) {
                existing.lastUsed = Date.now();
                return process.nextTick(() => done(null, existing));
            }
            existing.addWaiter(done);
            return undefined;
        }
        const destConfig = this._destinationsById[destinationId];
        if (!destConfig) {
            return process.nextTick(() => done(errors.InternalError.customizeDescription(
                `no destination configured for "${destinationId}"`)));
        }
        this._evictIfAtCapacity();
        const { host, port } = destConfig;
        const entry = new PooledProducer({
            destinationId,
            endpoint: port ? `${host}:${port}` : host,
            onSendStarted: () => this._onSendStarted(),
            onSendFinished: () => this._onSendFinished(),
            logger: this._log,
        });
        entry.addWaiter(done);
        this._producers.set(destinationId, entry);
        this._connect(entry, destConfig);
        return undefined;
    }

    _connect(entry, destConfig) {
        const { topic, pollIntervalMs, auth, requiredAcks, compressionType } = destConfig;
        const producer = new DeliveryKafkaProducer({
            kafka: { hosts: entry.endpoint },
            topic,
            pollIntervalMs,
            auth,
            compressionType,
            requiredAcks,
            deliveryTimeoutMs: this._deliveryTimeoutMs,
        });
        entry.attach(producer);
        producer.once('error', err => {
            this._log.error('error connecting producer to external destination', {
                method: 'DeliveryProducerPool._connect',
                destinationId: entry.destinationId,
                endpoint: entry.endpoint,
                topic,
                error: err.message,
            });
            // forget it, so that the next entry for this destination retries
            // instead of waiting on a producer that will never be ready
            if (this._producers.get(entry.destinationId) === entry) {
                this._producers.delete(entry.destinationId);
            }
            entry.flushWaiters(err);
        });
        producer.once('ready', () => {
            producer.removeAllListeners('error');
            // BackbeatProducer emits 'error' from the delivery report path,
            // an unhandled 'error' event would take down the process
            producer.on('error', err => {
                this._log.error('error from delivery producer', {
                    method: 'DeliveryProducerPool._connect',
                    destinationId: entry.destinationId,
                    endpoint: entry.endpoint,
                    topic,
                    error: err.message,
                });
            });
            entry.markReady();
            this._updateProducersGauge();
            this._log.info('opened producer to external destination', {
                method: 'DeliveryProducerPool._connect',
                destinationId: entry.destinationId,
                endpoint: entry.endpoint,
                topic,
            });
            entry.flushWaiters(null);
        });
    }

    _onSendStarted() {
        this._inFlight++;
        lanesGauge.set(this._inFlight);
    }

    _onSendFinished() {
        this._inFlight--;
        lanesGauge.set(this._inFlight);
    }

    /**
     * Close producers that have been idle for longer than producerIdleMs.
     * A producer with deliveries in flight is never closed, its delivery
     * reports are still needed to release the consumer offsets.
     *
     * @return {undefined}
     */
    _reapIdleProducers() {
        const now = Date.now();
        this._producers.forEach((entry, destinationId) => {
            if (!entry.isIdle() || now - entry.lastUsed < this._producerIdleMs) {
                return;
            }
            // remove from the map before closing, so that a get() racing with
            // the close creates a new producer instead of reusing this one
            this._producers.delete(destinationId);
            this._updateProducersGauge();
            this._log.info('closing idle producer', {
                method: 'DeliveryProducerPool._reapIdleProducers',
                destinationId,
                endpoint: entry.endpoint,
                idleMs: now - entry.lastUsed,
            });
            entry.close();
        });
    }

    /**
     * Make room for a new producer when the pool is at capacity, by closing
     * the least recently used idle one. If every producer is busy the cap is
     * exceeded rather than dropping deliveries in flight.
     *
     * @return {undefined}
     */
    _evictIfAtCapacity() {
        if (this._producers.size < this._maxProducers) {
            return;
        }
        let lru = null;
        this._producers.forEach(entry => {
            if (!entry.isIdle()) {
                return;
            }
            if (lru === null || entry.lastUsed < lru.lastUsed) {
                lru = entry;
            }
        });
        if (lru === null) {
            this._log.warn('producer pool is at capacity and all producers are busy, ' +
                'temporarily exceeding the limit', {
                method: 'DeliveryProducerPool._evictIfAtCapacity',
                maxProducers: this._maxProducers,
                producers: this._producers.size,
            });
            return;
        }
        this._producers.delete(lru.destinationId);
        this._updateProducersGauge();
        this._log.info('evicting least recently used producer', {
            method: 'DeliveryProducerPool._evictIfAtCapacity',
            destinationId: lru.destinationId,
            endpoint: lru.endpoint,
        });
        lru.close();
    }

    _updateProducersGauge() {
        const counts = new Map();
        this._producers.forEach(entry => {
            this._knownEndpoints.add(entry.endpoint);
            counts.set(entry.endpoint, (counts.get(entry.endpoint) || 0) + 1);
        });
        this._knownEndpoints.forEach(endpoint => {
            producersGauge.set({ endpoint }, counts.get(endpoint) || 0);
        });
    }

    /**
     * Close every producer in the pool
     *
     * @param {function} done - callback
     * @return {undefined}
     */
    closeAll(done) {
        this._closed = true;
        if (this._reapTimer) {
            clearInterval(this._reapTimer);
            this._reapTimer = null;
        }
        const entries = [...this._producers.values()];
        this._producers.clear();
        this._updateProducersGauge();
        return async.each(entries, (entry, next) => entry.close(next), () => done());
    }
}

module.exports = DeliveryProducerPool;
