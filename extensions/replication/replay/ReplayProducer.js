'use strict'; // eslint-disable-line strict

const { Logger } = require('werelogs');

const BackbeatProducer = require('../../../lib/BackbeatProducer');

class ReplayProducer {
    /**
     * Create the retry producer.
     *
     * @param {object} kafkaConfig - kafka config param
     */
    constructor(kafkaConfig, topic) {
        this._kafkaConfig = kafkaConfig;
        this._topic = topic;
        this._producer = null;
        this._log = new Logger('Backbeat:ReplayProducer');
    }

    /**
     * Set up the retry producer.
     * @param {function} [cb] - Optional callback called when startup
     * is complete
     * @return {undefined}
     */
    setupProducer(cb) {
        console.log('SETUP_PRODUCER this._topic!!!', this._topic);
        this._producer = new BackbeatProducer({
            kafka: { hosts: this._kafkaConfig.hosts },
            topic: this._topic,
        });
        this._producer.once('error', () => {});
        this._producer.once('ready', () => {
            this._producer.removeAllListeners('error');
            this._producer.on('error', err =>
                this._log.error('error from backbeat producer', {
                    error: err,
                }));
            if (cb) {
                cb();
            }
        });
    }

    /**
     * Publish the given message to the retry Kafka topic.
     * @param {String} message - The message to publish
     * @param {Function} [deliveryReportCb] - called when Kafka
     * returns a delivery report
     * @return {undefined}
     */
    publishReplayEntry(message, deliveryReportCb) {
        this._producer.send([message], err => {
            if (err) {
                this._log.trace('error publishing retry entry');
            }
            if (deliveryReportCb) {
                deliveryReportCb();
            }
        });
    }
}

module.exports = ReplayProducer;
