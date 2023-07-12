const { EventEmitter } = require('events');
const { Producer } = require('node-rdkafka');
const joi = require('@hapi/joi');

const errors = require('arsenal').errors;
const jsutil = require('arsenal').jsutil;
const Logger = require('werelogs').Logger;

const { withTopicPrefix } = require('./util/topic');

// waits for an ack for messages
const REQUIRE_ACKS = 1;
// time in ms. to wait for acks from Kafka
const ACK_TIMEOUT = 5000;
// max time in ms. to wait for flush to complete
const FLUSH_TIMEOUT = 5000;

/**
* Backbeat replication topic is currently setup with a config
* max.message.bytes of 5MB. Producers need to update their
* messageMaxBytes to get at least 5MB put in the Kafka topic, adding a
* little extra bytes of padding for approximation.
*/
const PRODUCER_MESSAGE_MAX_BYTES = 5000020;
const PRODUCER_POLL_INTERVAL_MS = 2000;

const CLIENT_ID = 'BackbeatProducer';

class BackbeatProducer extends EventEmitter {

    /**
    * constructor
    * @param {Object} config - config
    * @param {string} config.topic - Kafka topic to write to
    * @param {Object} config.kafka - kafka connection config
    * @param {string} config.kafka.hosts - kafka hosts list
    * as "host:port[,host:port...]"
    * @param {number} [config.messageMaxBytes] - maximum size of a single message
    * @param {number} [config.pollIntervalMs] - producer poll interval
    * between delivery report fetches, in milliseconds
    */
    constructor(config) {
        super();

        const configJoi = {
            kafka: joi.object({
                hosts: joi.string().required(),
            }).required(),
            topic: joi.string(),
            pollIntervalMs: joi.number().default(PRODUCER_POLL_INTERVAL_MS),
            messageMaxBytes: joi.number().default(PRODUCER_MESSAGE_MAX_BYTES),
        };
        const validConfig = joi.attempt(config, configJoi,
                                        'invalid config params');
        const { kafka, topic, pollIntervalMs, messageMaxBytes } = validConfig;

        this._kafkaHosts = kafka.hosts;
        this._log = new Logger(CLIENT_ID);
        this._topic = topic;
        this._ready = false;

        // create a new producer instance
        this._producer = new Producer({
            'metadata.broker.list': this._kafkaHosts,
            'message.max.bytes': messageMaxBytes,
            'dr_cb': true,
        }, {
            'request.required.acks': REQUIRE_ACKS,
            'request.timeout.ms': ACK_TIMEOUT,
        });
        this._ready = false;
        this._producer.connect();
        this._producer.on('ready', () => {
            this._ready = true;
            this.emit('ready');
            this._producer.setPollInterval(pollIntervalMs);
            this._producer.on('delivery-report',
                              this._onDeliveryReport.bind(this));
        });
        this._producer.on('event.error', error => {
            // This is a bit hacky: the "broker transport failure"
            // error occurs when the kafka broker reaps the idle
            // connections every few minutes, and librdkafka handles
            // reconnection automatically anyway, so we ignore those
            // harmless errors (moreover with the current
            // implementation there's no way to access the original
            // error code, so we match the message instead).
            if (!['broker transport failure',
                  'all broker connections are down']
                .includes(error.message)) {
                this._log.error('error with producer', {
                    error: error.message,
                    method: 'BackbeatProducer.constructor',
                });
                this.emit('error', error);
            }
        });
        return this;
    }

    getKafkaProducer() {
        return this._producer;
    }

    /**
     * get metadata from kafka topics
     * @param {object} params - call params
     * @param {string} params.topic - topic name
     * @param {number} params.timeout - timeout for the request
     * @param {function} cb - callback: cb(err, response)
     * @return {undefined}
     */
    getMetadata(params, cb) {
        this._producer.getMetadata(params, cb);
    }

    _onDeliveryReport(error, report) {
        const sendCtx = report.opaque;
        const cbOnce = sendCtx.cbOnce;
        --sendCtx.pendingReportsCount;
        if (error) {
            this._log.error('error in delivery report retrieval', {
                error: error.message,
                method: 'BackbeatProducer._onDeliveryReport',
            });
            this.emit('error', error);
            return cbOnce(error);
        }
        this._log.debug('delivery report received', { report });
        if (sendCtx.pendingReportsCount === 0) {
            // all delivery reports received (if errors occurred, the
            // callback will have been called earlier so this will be
            // a no-op)
            cbOnce();
        }
        return undefined;
    }

    /**
    * synchronous check for producer's status
    * @return {bool} - check result
    */
    isReady() {
        return this._ready;
    }

    /**
    * sends entries/messages to the topic configured in producer
    * @param {Object[]} entries - array of entries objects with properties
    * key and message ([{ key: 'foo', message: 'hello world'}, ...])
    * @param {callback} deliveryReportsCb - callback called when Kafka
    * returns a delivery report for this message:
    * deliveryReportCb(err, deliveryReports).
    * NOTE: it can take a couple seconds for a delivery report to be
    * received, hence it may not be a good idea to actively wait for
    * this callback in the critical path.
    * @return {this} current instance
    */
    send(entries, deliveryReportsCb) {
        if (!this._topic) {
            process.nextTick(() => {
                this._log.error('no topic configured to send messages to', {
                    method: 'BackbeatProducer.send',
                });
                deliveryReportsCb(errors.InternalError);
            });
            return this;
        }
        return this._sendToTopic(this._topic, entries, deliveryReportsCb);
    }

    /**
    * sends entries/messages to the given topic
    * @param {string} topic - topic to send messages to
    * @param {Object[]} entries - array of entries objects with properties
    * key and message ([{ key: 'foo', message: 'hello world'}, ...])
    * @param {callback} deliveryReportsCb - callback called when Kafka
    * returns a delivery report for this message:
    * deliveryReportCb(err, deliveryReports).
    * NOTE: it can take a couple seconds for a delivery report to be
    * received, hence it may not be a good idea to actively wait for
    * this callback in the critical path.
    * @return {this} current instance
    */
    sendToTopic(topic, entries, deliveryReportsCb) {
        return this._sendToTopic(withTopicPrefix(topic), entries, deliveryReportsCb);
    }

    _sendToTopic(topic, entries, cb) {
        this._log.debug('publishing entries', {
            method: 'BackbeatProducer._sendToTopic',
            topic,
            entryCount: entries.length,
        });
        if (!this._ready) {
            process.nextTick(() => {
                this._log.error('producer is not ready yet', {
                    method: 'BackbeatProducer._sendToTopic',
                    ready: this._ready,
                });
                cb(errors.InternalError);
            });
            return this;
        }
        if (entries.length === 0) {
            process.nextTick(cb);
            return this;
        }
        const sendCtx = { cbOnce: jsutil.once(cb),
                          pendingReportsCount: entries.length,
                          receivedReports: [] };
        try {
            entries.forEach(item => {
                let partition = null;
                if (item.partition !== undefined && item.key === 'canary') {
                    partition = item.partition;
                }
                this._producer.produce(
                    topic,
                    partition, // partition
                    Buffer.from(item.message), // value
                    item.key, // key (for keyed partitioning)
                    Date.now(), // timestamp
                    sendCtx // opaque
                );
            });
        } catch (err) {
            this._log.error('error publishing entries', {
                method: 'BackbeatProducer._sendToTopic',
                topic,
                error: err.message,
            });
            process.nextTick(
                () => sendCtx.cbOnce(errors.InternalError.
                                     customizeDescription(err.message)));
        }
        return this;
    }


    /**
    * close client connection
    * @param {callback} cb - cb()
    * @return {object} this - current class instance
    */
    close(cb) {
        this._producer.flush(FLUSH_TIMEOUT, err => {
            this._ready = false;
            if (err) {
                this._log.error('error flushing entries', {
                    error: err,
                    method: 'BackbeatProducer.close',
                });
            }
            this._producer.disconnect();
            return cb(err);
        });
        return this;
    }
}

module.exports = BackbeatProducer;
