const { EventEmitter } = require('events');
const { Producer } = require('node-rdkafka');
const joi = require('@hapi/joi');

const errors = require('arsenal').errors;
const jsutil = require('arsenal').jsutil;
const Logger = require('werelogs').Logger;

const authUtil = require('../utils/auth');

// waits for an ack for messages
const REQUIRE_ACKS = 1;
// time in ms. to wait for acks from Kafka
const ACK_TIMEOUT = 5000;
// max time in ms. to wait for flush to complete
const FLUSH_TIMEOUT = 5000;

const PRODUCER_MESSAGE_MAX_BYTES = 5000020;

const CLIENT_ID = 'KafkaNotificationProducer';

class KafkaProducer extends EventEmitter {

    /**
    * constructor
    * @param {Object} config - config
    * @param {string} config.topic - Kafka topic to write to
    * @param {string} config.auth - kafka auth configuration
    * @param {Object} config.kafka - kafka connection config
    * @param {string} config.kafka.hosts - kafka hosts list
    */
    constructor(config) {
        super();

        const configJoi = {
            kafka: joi.object({
                hosts: joi.string().required(),
            }).required(),
            topic: joi.string().required(),
            pollIntervalMs: joi.number().default(2000),
            messageMaxBytes: joi.number().default(PRODUCER_MESSAGE_MAX_BYTES),
            auth: joi.object().optional(),
        };
        const validConfig = joi.attempt(config, configJoi,
            'invalid config params');
        const {
            kafka,
            topic,
            pollIntervalMs,
            messageMaxBytes,
            auth,
        } = validConfig;
        this._kafkaHosts = kafka.hosts;
        this._log = new Logger(CLIENT_ID);
        this._topic = topic;
        this._ready = false;
        const authObject = auth ? authUtil.generateKafkaAuthObject(auth) : {};
        const producerOptions = {
            'metadata.broker.list': this._kafkaHosts,
            'batch.num.messages': 1000000,
            'queue.buffering.max.ms': 10,            
            'message.max.bytes': messageMaxBytes,
            'dr_cb': true,
        };
        Object.assign(producerOptions, authObject);
        // create a new producer instance
        this._producer = new Producer(producerOptions, {
            'request.required.acks': REQUIRE_ACKS,
            'request.timeout.ms': ACK_TIMEOUT,
        });
        this._ready = false;
        this._producer.connect({}, error => {
            if (error) {
                this._log.info('error connecting to broker', {
                    error,
                    method: 'KafkaProducer.constructor',
                });
            }
        });
        this._producer.on('ready', () => {
            this._ready = true;
            this.emit('ready');
            this._producer.setPollInterval(pollIntervalMs);
            this._producer.on('delivery-report',
                this._onDeliveryReport.bind(this));
        });
        this._producer.on('event.error', error => {
            this._log.error('error with producer', {
                error: error.message,
                method: 'KakfaProducer.constructor',
            });
            this.emit('error', error);
        });
        return this;
    }

    _onDeliveryReport(error, report) {
        const sendCtx = report.opaque;
        const cbOnce = sendCtx.cbOnce;
        --sendCtx.pendingReportsCount;
        if (error) {
            this._log.error('error in delivery report retrieval', {
                error: error.message,
                method: 'KakfaProducer._onDeliveryReport',
            });
            this.emit('error', error);
            return cbOnce(error);
        }
        this._log.debug('delivery report received', { report });
        if (sendCtx.pendingReportsCount === 0) {
            cbOnce();
        }
        return undefined;
    }

    /**
    * sends entries/messages to the given topic
    * @param {Object[]} entries - array of entries objects with properties
    * key and message ([{ key: 'foo', message: 'hello world'}, ...])
    * @param {callback} cb - cb(err)
    * @return {this} current instance
    */
    send(entries, cb) {
        this._log.debug('publishing entries',
            { method: 'KakfaProducer.send' });
        if (!this._ready) {
            return process.nextTick(() => {
                this._log.error('producer is not ready yet', {
                    method: 'KakfaProducer.send',
                    ready: this._ready,
                });
                cb(errors.InternalError);
            });
        }
        if (entries.length === 0) {
            return process.nextTick(cb);
        }
        const sendCtx = {
            cbOnce: jsutil.once(cb),
            pendingReportsCount: entries.length,
        };
        try {
            // TODO: try to avoid stringify operation
            entries.forEach(item => this._producer.produce(
                this._topic,
                null, // partition
                Buffer.from(JSON.stringify(item.message)), // value
                item.key, // key (for keyed partitioning)
                Date.now(), // timestamp
                sendCtx // opaque
            ));
        } catch (err) {
            this._log.error('error publishing entries', {
                error: err,
                method: 'KakfaProducer.send',
            });
            return process.nextTick(
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
                    method: 'KakfaProducer.close',
                });
            }
            this._producer.disconnect();
            return cb(err);
        });
        return this;
    }
}

module.exports = KafkaProducer;
