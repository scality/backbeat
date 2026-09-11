const { EventEmitter } = require('events');
const fs = require('fs');
const joi = require('joi');

const { errors } = require('arsenal');
const Logger = require('werelogs').Logger;

const { authSchema } = require('../NotificationConfigValidator');
const { withTopicPrefix } = require('../../../lib/util/topic');
const {
    KAFKA_PRODUCER_MESSAGE_MAX_BYTES,
    KAFKA_PRODUCER_DEFAULT_REQUIRED_ACKS,
} = require('../../../lib/config.joi');
const { getAuthFilePath } = require('../utils/auth');
const { processCredentials } = require('./kerberosCredentials');
const { gssapiPlatformaticSaslOption } = require('./saslGssapi');

const CONNECT_TIMEOUT_MS = 10000;
const DEFAULT_DELIVERY_TIMEOUT_MS = 30000;
const CONNECT_RETRIES = 2;

// @platformatic/kafka compresses in process and ships every codec kafka
// defines (gzip and zstd through node's zlib, snappy and lz4 through its
// wasm helpers). Matched case insensitively, as librdkafka does.
const COMPRESSION_TYPES = {
    none: undefined,
    gzip: 'gzip',
    snappy: 'snappy',
    lz4: 'lz4',
    zstd: 'zstd',
};

// what a kafka destination's own schema defaults compressionType to, rather
// than the default of the shared backbeat producer config
const DEFAULT_COMPRESSION_TYPE = 'none';

/**
 * Producer to a kerberised external kafka destination on @platformatic/kafka,
 * authenticating as the destination's own principal.
 *
 * Same surface and same reason to exist as KerberosKafkaProducer, which sits
 * on kafkajs: a 'ready' or 'error' event after construction, send(entries, cb)
 * called back on the delivery report, and close(cb). It names the principal
 * per client through the client's custom SASL authenticate hook, so one
 * delivery worker can serve destinations belonging to different principals.
 *
 * The client library is pure JavaScript and maintained, where kafkajs has not
 * shipped since 2023; the GSSAPI mechanism itself is the same code for both.
 */
class PlatformaticKerberosProducer extends EventEmitter {
    /**
     * @constructor
     * @param {Object} config - producer configuration, same shape as the one
     *   KafkaProducer takes
     * @param {Object} config.kafka - kafka configuration
     * @param {string} config.kafka.hosts - broker list
     * @param {string} [config.topic] - topic to send to
     * @param {Object} config.auth - destination auth configuration, must be
     *   of type 'kerberos'
     * @param {number} [config.deliveryTimeoutMs] - bound on how long a send
     *   may take before it is reported as failed
     */
    constructor(config) {
        super();
        const validConfig = joi.attempt(config, this.getConfigJoi(), 'invalid config params');
        this._log = new Logger(this.getClientId());
        this.setFromConfig(validConfig);
        this._ready = false;
        this._producer = null;
        this._connect();
    }

    getConfigJoi() {
        return joi.object({
            kafka: joi.object({
                hosts: joi.string().required(),
            }).required(),
            topic: joi.string(),
            auth: authSchema,
            // accepted so that the pool can build any producer from the same
            // config; this client has no poll loop to schedule
            pollIntervalMs: joi.number(),
            maxRequestSize: joi.number().default(KAFKA_PRODUCER_MESSAGE_MAX_BYTES),
            compressionType: joi.string().default(DEFAULT_COMPRESSION_TYPE),
            requiredAcks: joi.number().default(KAFKA_PRODUCER_DEFAULT_REQUIRED_ACKS),
            deliveryTimeoutMs: joi.number().default(DEFAULT_DELIVERY_TIMEOUT_MS),
        });
    }

    getClientId() {
        return 'NotificationPlatformaticKerberosProducer';
    }

    setFromConfig(joiResult) {
        const {
            kafka, topic, auth, compressionType, requiredAcks, deliveryTimeoutMs,
        } = joiResult;
        this._brokers = kafka.hosts.split(',').map(host => host.trim()).filter(host => host);
        this._topic = topic && withTopicPrefix(topic);
        this._auth = auth || {};
        this._requiredAcks = requiredAcks;
        this._deliveryTimeoutMs = deliveryTimeoutMs;
        const key = String(compressionType).toLowerCase();
        if (!Object.prototype.hasOwnProperty.call(COMPRESSION_TYPES, key)) {
            throw new Error(`unsupported compressionType "${compressionType}" for a ` +
                'kerberos destination, use none, gzip, snappy, lz4 or zstd');
        }
        this._compression = COMPRESSION_TYPES[key];
        if (this._auth.type !== 'kerberos') {
            throw new Error('PlatformaticKerberosProducer needs an auth configuration of type kerberos');
        }
    }

    /**
     * Resolve the destination's keytab, and prepare the process so that MIT
     * can hand out a credential for this principal.
     *
     * @return {undefined}
     */
    _prepareCredentials() {
        const { keytab, credentialSource } = this._auth;
        if (credentialSource === 'ccache') {
            // the operator populates the credential cache collection out of
            // band; nothing to set up here beyond leaving KRB5CCNAME alone
            return;
        }
        const keytabPath = getAuthFilePath(keytab);
        if (keytabPath === null) {
            throw new Error(`Keytab file ${keytab} not found`);
        }
        processCredentials.registerKeytab(keytabPath, this._log);
    }

    /**
     * TLS material for a SASL_SSL destination, read from the same files the
     * node-rdkafka path points librdkafka at
     *
     * @return {Object|undefined} node tls connection options
     */
    _tlsOption() {
        if (this._auth.protocol !== 'SASL_SSL') {
            return undefined;
        }
        const { ca, client, key, keyPassword } = this._auth;
        const readOrThrow = (fileName, label) => {
            const filePath = getAuthFilePath(fileName);
            if (filePath === null) {
                throw new Error(`${label} file ${fileName} not found`);
            }
            return fs.readFileSync(filePath);
        };
        const tls = {};
        if (ca) {
            tls.ca = [readOrThrow(ca, 'CA')];
        }
        if (client) {
            tls.cert = readOrThrow(client, 'Client certificate');
        }
        if (key) {
            tls.key = readOrThrow(key, 'Key');
            if (keyPassword) {
                tls.passphrase = keyPassword;
            }
        }
        return tls;
    }

    /**
     * Build the client. Kept apart from _connect so tests can exercise the
     * surface without the library.
     *
     * @return {Object} platformatic Producer
     */
    _buildProducer() {
        // required here so that a deployment leaving the flag off never loads
        // the client library or the native GSSAPI binding
        const { Producer } = require('@platformatic/kafka');
        return new Producer({
            clientId: this.getClientId(),
            bootstrapBrokers: this._brokers,
            tls: this._tlsOption(),
            sasl: gssapiPlatformaticSaslOption({
                kerberos: require('kerberos'),
                principal: this._auth.principal,
                serviceName: this._auth.serviceName,
                logger: this._log,
            }),
            autocreateTopics: false,
            acks: this._requiredAcks,
            compression: this._compression,
            timeout: this._deliveryTimeoutMs,
            connectTimeout: CONNECT_TIMEOUT_MS,
            retries: CONNECT_RETRIES,
        });
    }

    _connect() {
        let producer;
        try {
            this._prepareCredentials();
            producer = this._buildProducer();
        } catch (err) {
            // a bad keytab or an unusable auth config must not throw out of
            // the constructor: the pool and the destination both learn about
            // a producer that cannot connect from the 'error' event
            process.nextTick(() => this.emit('error', err));
            return;
        }
        this._producer = producer;
        producer.on('error', err => {
            this._log.warn('client error', {
                method: 'PlatformaticKerberosProducer._connect',
                error: err.message,
            });
        });
        // the client connects lazily; a metadata request forces the SASL
        // handshake now, so a wrong principal or keytab surfaces as 'error'
        // instead of on the first send
        const topics = this._topic ? [this._topic] : [];
        producer.metadata({ topics }).then(() => {
            this._ready = true;
            this._log.info('connected to kerberised kafka destination', {
                method: 'PlatformaticKerberosProducer._connect',
                brokers: this._brokers,
                topic: this._topic,
                principal: this._auth.principal,
            });
            this.emit('ready');
        }).catch(err => this.emit('error', err));
    }

    /**
     * Send entries to the configured topic
     *
     * @param {Object[]} entries - entries with a key and a message,
     *   [{ key: 'foo', message: 'hello world' }, ...]
     * @param {function} deliveryReportsCb - called once the broker has
     *   acknowledged the batch: cb(err, deliveryReports)
     * @return {this} current instance
     */
    send(entries, deliveryReportsCb) {
        if (!this._topic) {
            process.nextTick(() => {
                this._log.error('no topic configured to send messages to', {
                    method: 'PlatformaticKerberosProducer.send',
                });
                deliveryReportsCb(errors.InternalError);
            });
            return this;
        }
        if (!this._ready) {
            process.nextTick(() => {
                this._log.error('producer is not ready yet', {
                    method: 'PlatformaticKerberosProducer.send',
                });
                deliveryReportsCb(errors.InternalError);
            });
            return this;
        }
        if (entries.length === 0) {
            process.nextTick(deliveryReportsCb);
            return this;
        }
        const now = BigInt(Date.now());
        this._producer.send({
            messages: entries.map(entry => ({
                topic: this._topic,
                key: entry.key === undefined || entry.key === null
                    ? undefined : Buffer.from(String(entry.key)),
                value: Buffer.from(entry.message),
                timestamp: now,
            })),
        }).then(result => {
            // the delivery pool reads the report the way node-rdkafka shapes
            // it: one entry per acknowledged record
            const offsets = (result && result.offsets) || [];
            deliveryReportsCb(null, offsets.map(offset => ({
                topic: offset.topic,
                partition: offset.partition,
                offset: offset.offset,
            })));
        }).catch(err => {
            this._log.error('error publishing entries', {
                method: 'PlatformaticKerberosProducer.send',
                topic: this._topic,
                error: err.message,
            });
            deliveryReportsCb(errors.InternalError.customizeDescription(err.message));
        });
        return this;
    }

    /**
     * Disconnect the producer, flushing what it is holding
     *
     * @param {function} cb - callback
     * @return {this} current instance
     */
    close(cb) {
        this._ready = false;
        if (!this._producer) {
            process.nextTick(cb);
            return this;
        }
        this._producer.close()
            .then(() => cb())
            .catch(err => {
                this._log.error('error closing producer', {
                    method: 'PlatformaticKerberosProducer.close',
                    error: err.message,
                });
                cb(err);
            });
        return this;
    }
}

module.exports = PlatformaticKerberosProducer;
