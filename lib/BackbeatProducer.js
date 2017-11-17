const { EventEmitter } = require('events');
const { Producer } = require('node-rdkafka');
const joi = require('joi');

const errors = require('arsenal').errors;
const jsutil = require('arsenal').jsutil;
const Logger = require('werelogs').Logger;

// waits for an ack for messages
const REQUIRE_ACKS = 1;
// time in ms. to wait for acks from Kafka
const ACK_TIMEOUT = 5000;
// max time in ms. to wait for flush to complete
const FLUSH_TIMEOUT = 5000;

const CLIENT_ID = 'BackbeatProducer';

class BackbeatProducer extends EventEmitter {

    /**
    * constructor
    * @param {Object} config - config
    * @param {string} config.topic - Kafka topic to write to
    * @param {Object} config.kafka - kafka connection config
    * @param {string} config.kafka.hosts - kafka hosts list
    * as "host:port[,host:port...]"
    */
    constructor(config) {
        super();

        const configJoi = {
            kafka: joi.object({
                hosts: joi.string().required(),
            }).required(),
            topic: joi.string().required(),
            pollIntervalMs: joi.number().default(2000),
        };
        const validConfig = joi.attempt(config, configJoi,
                                        'invalid config params');
        const { kafka, topic, pollIntervalMs } = validConfig;

        this._kafkaHosts = kafka.hosts;
        this._log = new Logger(CLIENT_ID);
        this._topic = topic;
        this._ready = false;

        // create a new producer instance
        this._producer = new Producer({
            'metadata.broker.list': this._kafkaHosts,
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
            this._log.error('error with producer', {
                error: error.message,
                method: 'BackbeatProducer.constructor',
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
    * sends entries/messages to the given topic
    * @param {Object[]} entries - array of entries objects with properties
    * key and message ([{ key: 'foo', message: 'hello world'}, ...])
    * @param {callback} cb - cb(err)
    * @return {this} current instance
    */
    send(entries, cb) {
        this._log.debug('publishing entries',
            { method: 'BackbeatProducer.send' });
        if (!this._ready) {
            return process.nextTick(() => {
                this._log.error('producer is not ready yet', {
                    method: 'BackbeatProducer.send',
                    ready: this._ready,
                });
                cb(errors.InternalError);
            });
        }
        if (entries.length === 0) {
            return process.nextTick(cb);
        }
        const sendCtx = { cbOnce: jsutil.once(cb),
                          pendingReportsCount: entries.length };
        try {
            entries.forEach(item => this._producer.produce(
                this._topic,
                null, // partition
                new Buffer(item.message), // value
                item.key, // key (for keyed partitioning)
                Date.now(), // timestamp
                sendCtx // opaque
            ));
        } catch (err) {
            this._log.error('error publishing entries', {
                error: err,
                method: 'BackbeatProducer.send',
            });
            return process.nextTick(
                () => sendCtx.cbOnce(errors.InternalError.
                                     customizeDescription(err.message)));
        }
        return this;
    }

    /**
    * close client connection
    * @param {callback} cb - cb(err)
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
