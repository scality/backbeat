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
     * Publish a 'deleteData' message to the backbeat-gc Kafka topic.
     *
     * @param {object[]} dataLocations - array of data locations to gc
     * @param {string} dataLocations[].key - data location key
     * @param {string} dataLocations[].dataStoreName - data location
     *   constraint name
     * @param {number} dataLocations[].size - object size in bytes
     * @return {undefined}
     */
    publishDeleteDataEntry(dataLocations) {
        this._producer.send([{ message: JSON.stringify({
            action: 'deleteData',
            target: {
                locations: dataLocations.map(location => ({
                    key: location.key,
                    dataStoreName: location.dataStoreName,
                    size: location.size,
                })),
            },
        }) }], err => {
            if (err) {
                this._log.error('error publishing GC.deleteData entry', {
                    error: err,
                    method: 'GarbageCollectorProducer.publishDeleteDataEntry',
                });
            }
        });
    }
}

module.exports = GarbageCollectorProducer;
