const { EventEmitter } = require('events');
const { Producer } = require('node-rdkafka');
const joi = require('joi');

const { errors, jsutil } = require('arsenal');
const Logger = require('werelogs').Logger;

const { withTopicPrefix } = require('./util/topic');
const KafkaBacklogMetrics = require('./KafkaBacklogMetrics');
const Constants = require('./constants');

const DEFAULT_POLL_INTERVAL = 2000;
const PRODUCER_MESSAGE_MAX_BYTES = 5000020;
const FLUSH_TIMEOUT = 5000;


class BackbeatProducer extends EventEmitter {

    constructor(config) {
        super();

        const validConfig = joi.attempt(
            config,
            this.getConfigJoi(),
            'invalid config params'
        );
        this._config = validConfig;
        this.setFromConfig(validConfig);

        this._ready = false;
        this._log = new Logger(this.getClientId());

        this._producer = new Producer(this.producerConfig, this.topicConfig);

        this.connect();
        this.setListeners();
        return this;
    }

    getConfigJoi() {
        return joi.object(
            {
                kafka: joi.object({
                    hosts: joi.string().required(),
                }).required(),
                topic: joi.string(),
                pollIntervalMs: joi.number().default(DEFAULT_POLL_INTERVAL),
                messageMaxBytes: joi.number().default(PRODUCER_MESSAGE_MAX_BYTES),
            }
        );
    }

    getClientId() {
        return 'BackbeatProducer';
    }

    getRequireAcks() {
        return 'all';
    }

    getAckTimeout() {
        return 5000;
    }

    get producerConfig() {
        return {
            'metadata.broker.list': this._kafkaHosts,
            'message.max.bytes': this._messageMaxBytes,
            'dr_cb': true,
            'compression.type': Constants.compressionType,
        };
    }

    get topicConfig() {
        return {
            'request.required.acks': this.getRequireAcks(),
            'request.timeout.ms': this.getAckTimeout(),
        };
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

    isReady() {
        return this._ready;
    }

    setFromConfig(joiResult) {
        const {
            kafka,
            topic,
            pollIntervalMs,
            messageMaxBytes,
        } = joiResult;
        this._kafkaHosts = kafka.hosts;
        this._topic = topic && withTopicPrefix(topic);
        this._pollIntervalMs = pollIntervalMs;
        this._messageMaxBytes = messageMaxBytes;
    }

    connect() {
        this._producer.connect({ timeout: 30000 }, () => {
            const opts = {
                topic: withTopicPrefix('backbeat-sanitycheck'),
                timeout: 10000,
            };
            this._producer.getMetadata(opts, err => {
                if (err) {
                    this.emit('error', err);
                }
            });
        });
    }

    setListeners() {
        this._producer.on('ready', this.onReady.bind(this));
        this._producer.on('event.error', this.onEventError.bind(this));
    }

    onDeliveryReport(error, report) {
        const sendCtx = report.opaque;
        const cbOnce = sendCtx.cbOnce;
        sendCtx.receivedReports.push(report);
        --sendCtx.pendingReportsCount;
        KafkaBacklogMetrics.onDeliveryReportReceived(error);
        if (error) {
            this._log.error('error in delivery report retrieval', {
                error: error.message,
                method: 'BackbeatProducer._onDeliveryReport',
            });
            this.emit('error', error);
            return cbOnce(error);
        }
        const { topic, partition, offset, timestamp } = report;
        const key = report.key && report.key.toString();
        this._log.debug('delivery report received',
            { topic, partition, offset, timestamp, key });
        KafkaBacklogMetrics.onMessagePublished(
            topic, partition, timestamp / 1000);
        if (sendCtx.pendingReportsCount === 0) {
            // all delivery reports received (if errors occurred, the
            // callback will have been called earlier so this will be
            // a no-op)
            cbOnce(null, sendCtx.receivedReports);
        }
        return undefined;
    }

    onReady() {
        this._ready = true;
        this.emit('ready');
        this._producer.setPollInterval(this._pollIntervalMs);
        this._producer.on('delivery-report', this.onDeliveryReport.bind(this));
    }

    onEventError(error) {
        // This is a bit hacky: the "broker transport failure"
        // error occurs when the kafka broker reaps the idle
        // connections every few minutes, and librdkafka handles
        // reconnection automatically anyway, so we ignore those
        // harmless errors (moreover with the current
        // implementation there's no way to access the original
        // error code, so we match the message instead).
        const config = this._config;
        if (!['broker transport failure',
            'all broker connections are down']
            .includes(error.message)) {
            this._log.error('error with producer', {
                config,
                error: error.message,
                method: `${this.getClientId()}.constructor`,
            });
            this.emit('error', error);
        }
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
            method: `${this.getClientId()}._sendToTopic`,
            topic,
            entryCount: entries.length,
        });
        if (!this._ready) {
            process.nextTick(() => {
                this._log.error('producer is not ready yet', {
                    method: `${this.getClientId()}._sendToTopic`,
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
        const sendCtx = {
            cbOnce: jsutil.once(cb),
            pendingReportsCount: entries.length,
            receivedReports: []
        };
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
                method: `${this.getClientId()}._sendToTopic`,
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
                    method: `${this.getClientId()}.close`,
                });
            }
            this._producer.disconnect();
            return cb(err);
        });
        return this;
    }
}

module.exports = BackbeatProducer;
