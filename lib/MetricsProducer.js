'use strict'; // eslint-disable-line strict

const async = require('async');
const { Logger } = require('werelogs');

const BackbeatProducer = require('./BackbeatProducer');
const MetricsModel = require('./models/MetricsModel');

class MetricsProducer {
    /**
    * @constructor
    * @param {Object} kafkaConfig - kafka connection config
    * @param {Object} mConfig - metrics configurations
    */
    constructor(kafkaConfig, mConfig) {
        this._kafkaConfig = kafkaConfig;
        this._topic = mConfig.topic;

        this._producer = null;
        this._log = new Logger('MetricsProducer');
    }

    setupProducer(done) {
        const producer = new BackbeatProducer({
            kafka: { hosts: this._kafkaConfig.hosts },
            topic: this._topic,
        });
        producer.once('error', done);
        producer.once('ready', () => {
            producer.removeAllListeners('error');
            producer.on('error', err => {
                this._log.error('error from backbeat producer',
                               { error: err });
            });
            this._producer = producer;
            done();
        });
    }

    /**
     * @param {Object} extMetrics - an object where keys are all sites for a
     *   given extension and values are the metrics for the site
     *   (i.e. { my-site: { ops: 1, bytes: 124 } } )
     * @param {String} type - type of metric (queueud or processed)
     * @param {String} ext - extension (i.e. 'crr')
     * @param {function} cb - callback
     * @return {undefined}
     */
    publishMetrics(extMetrics, type, ext, cb) {
        async.each(Object.keys(extMetrics), (siteName, done) => {
            const { ops, bytes } = extMetrics[siteName];
            const message = new MetricsModel(ops, bytes, ext, type,
                siteName).serialize();
            this._producer.send([{ message }], err => {
                if (err) {
                    // Using trace here because errors are already logged in
                    // BackbeatProducer. This is to log to see source of caller
                    this._log.trace(`error publishing ${type} metrics for` +
                        `extension metrics ${ext}`);
                }
                done();
            });
        }, cb);
    }
}

module.exports = MetricsProducer;
