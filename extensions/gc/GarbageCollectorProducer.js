'use strict'; // eslint-disable-line strict

const { Logger } = require('werelogs');

const BackbeatProducer = require('../../lib/BackbeatProducer');
const config = require('../../conf/Config');

class GarbageCollectorProducer {
    /**
     * Create the garbage collector producer.
     */
    constructor() {
        this._kafkaConfig = config.kafka;
        this._topic = config.extensions.gc.topic;
        this._producer = null;
        this._log = new Logger('Backbeat:GarbageCollectorProducer');
    }

    /**
     * Set up the retry producer.
     * @param {function} [cb] - Optional callback called when startup
     * is complete
     * @return {undefined}
     */
    setupProducer(cb) {
        const producer = new BackbeatProducer({
            kafka: { hosts: this._kafkaConfig.hosts },
            topic: this._topic,
        });
        producer.once('error', () => {});
        producer.once('ready', () => {
            producer.removeAllListeners('error');
            producer.on('error', err =>
                this._log.error('error from backbeat producer', {
                    error: err,
                }));
            this._producer = producer;
            if (cb) {
                return cb();
            }
            return undefined;
        });
    }

    /**
     * Publish an action entry to the backbeat-gc Kafka topic.
     *
     * Supported action type: 'deleteData'
     * The action is expected to contain a "target.locations"
     * attribute containing an array of locations to get rid of, where
     * each item is an object containing with the following attributes:
     * - key: data location key
     * - dataStoreName: data location constraint name
     * - size: data location size in bytes
     * - [dataStoreVersionId]: version ID of data location, needed for
     *   cloud backends
     * @param {ActionQueueEntry} entry - the action entry to send to
     * the GC service
     * @param {Function} cb - The callback function
     * @return {undefined}
     */
    publishActionEntry(entry, cb) {
        this._producer.send([{ message: entry.toKafkaMessage() }], err => {
            if (err) {
                this._log.error('error publishing GC.deleteData entry', {
                    error: err,
                    method: 'GarbageCollectorProducer.publishActionEntry',
                });
            }
            return cb(err);
        });
    }
}

module.exports = GarbageCollectorProducer;
