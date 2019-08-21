'use strict'; // eslint-disable-line strict

const { Logger } = require('werelogs');

const BackbeatProducer = require('../../../lib/BackbeatProducer');
const config = require('../../../lib/Config');

class FailedCRRProducer {
    /**
     * Create the retry producer.
     */
    constructor() {
        this._kafkaConfig = config.kafka;
        this._topic = config.extensions.replication.replicationFailedTopic;
        this._producer = null;
        this._log = new Logger('Backbeat:FailedCRRProducer');
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
     * Publish the given message to the retry Kafka topic.
     * @param {String} message - The message to publish
     * @param {Function} cb - The callback function
     * @return {undefined}
     */
    publishFailedCRREntry(message, cb) {
        this._producer.send([{ message }], err => {
            if (err) {
                this._log.trace('error publishing retry entry');
            }
            return cb();
        });
    }

    isReady() {
        return this._producer && this._producer.isReady();
    }
}

module.exports = FailedCRRProducer;
