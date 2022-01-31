'use strict'; // eslint-disable-line strict

const { Logger } = require('werelogs');

const BackbeatProducer = require('../../../lib/BackbeatProducer');

class ReplayProducer {
    /**
     * Create the replay producer.
     *
     * @param {object} kafkaConfig - kafka config param
     * @param {string} topic - topic name
     */
    constructor(kafkaConfig, topic) {
        this._kafkaConfig = kafkaConfig;
        this._topic = topic;
        this._producer = null;
        this._log = new Logger('Backbeat:ReplayProducer');
        this._log.addDefaultFields({ topic });
    }

    /**
     * Set up the replay producer.
     * @param {function} [cb] - Optional callback called when startup
     * is complete
     * @return {undefined}
     */
    setupProducer(cb) {
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
     * Publish the given message to the replay Kafka topic.
     * @param {String} message - The message to publish
     * @param {Function} [deliveryReportCb] - called when Kafka
     * returns a delivery report
     * @return {undefined}
     */
    publishReplayEntry(message, deliveryReportCb) {
        this._producer.send([message], err => {
            if (err) {
                this._log.trace('error publishing replay entry');
            }
            if (deliveryReportCb) {
                deliveryReportCb();
            }
        });
    }
}

module.exports = ReplayProducer;
