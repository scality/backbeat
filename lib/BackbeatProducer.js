const assert = require('assert');
const { EventEmitter } = require('events');
const { Client, KeyedMessage, Producer } = require('kafka-node');

const errors = require('arsenal').errors;
const Logger = require('werelogs').Logger;
/**
* compression params define compression for log compaction in topics. It is
* not recommended to enable by default as the compression options tend to
* consume a lot of CPU.
*/
const NO_COMPRESSION = 0;
const GZIP_COMPRESSION = 1; // eslint-disable-line
const SNAPPY_COMPRESSION = 2; // eslint-disable-line

// time in ms. to wait for acks from Kafka
const ACK_TIMEOUT = 100;
// use keyed-message mechanism for partitioning by default. This lets Kafka
// choose a partition based on the key and ensures entries with the same key
// ends up in the same partition
const KEYED_PARTITIONER = 3;
// default kafka partitioning
const KAFKA_PARTITIONER = 0;
// waits for an ack for messages
const REQUIRE_ACKS = 1;

const LOG_DEFAULT = { logLevel: 'info', dumpLevel: 'error' };
const ZOOKEEPER_DEFAULT = { host: 'localhost', port: 2181 };

const CLIENT_ID = 'BackbeatProducer';

class BackbeatProducer extends EventEmitter {

    /**
    * constructor
    * @param {Object} config - config
    * @param {string} topic - Kafka topic to write to
    * @param {number} [partition] - partition in a topic to write to
    * @param {Object} [config.zookeeper] - zookeeper endpoint config
    * @param {string} config.zookeeper.host - zookeeper host
    * @param {number} config.zookeeper.port - zookeeper port
    * @param {object} [config.log] - logger config
    * @param {object} config.log.logLevel - default log level
    * @param {object} config.log.dumpLevel - dump level for logger
    */
    constructor(config) {
        super();
        const { zookeeper, log, topic, partition, sslOptions } = config;

        assert(typeof topic === 'string', 'config: topic must be a string');
        if (zookeeper !== undefined) {
            assert(typeof zookeeper === 'object', 'config: zookeeper must be' +
                'an object');
            assert(typeof zookeeper.host === 'string', 'config: ' +
                'zookeeper.host must be a string');
            assert(typeof zookeeper.port === 'number', 'config: ' +
                'zookeeper.port must be a number');
        }
        if (sslOptions !== undefined) {
            assert(typeof sslOptions === 'object', 'config: sslOptions must ' +
                'be an object');
        }
        if (log !== undefined) {
            assert(typeof log === 'object', 'config: log must be an object');
            assert(typeof log.logLevel === 'string', 'config: log.logLevel ' +
                'must be a string');
            assert(typeof log.dumpLevel === 'string', 'config: log.dumpLevel' +
                ' must be a string');
        }
        if (partition !== undefined) {
            assert(typeof partition === 'number', 'config: partition must be ' +
                'a number');
            this._partition = partition;
        }
        const { host, port } = zookeeper || ZOOKEEPER_DEFAULT;
        const { logLevel, dumpLevel } = log || LOG_DEFAULT;
        this._zookeeperEndpoint = `${host}:${port}`;
        this._log = new Logger(CLIENT_ID, {
            level: logLevel,
            dump: dumpLevel,
        });
        this._topic = topic;
        this._ready = false;
        // create zookeeper client
        this._client = new Client(this._zookeeperEndpoint, CLIENT_ID,
            { sslOptions });
        // create a new producer instance
        this._producer = new Producer(this._client, {
            // configuration for when to consider a message as acknowledged
            requireAcks: REQUIRE_ACKS,
            // amount of time in ms. to wait for all acks
            ackTimeoutMs: ACK_TIMEOUT,
            // uses keyed-message partitioner to ensure messages with the same
            // key end up in one partition
            partitionerType: partition === undefined ? KEYED_PARTITIONER :
                KAFKA_PARTITIONER,
            // controls compression of the message
            attributes: NO_COMPRESSION,
        });
        this._ready = false;
        this._producer.on('ready', () => {
            this._ready = true;
            this.emit('ready');
        });
        this._producer.on('error', error => {
            this._ready = false;
            this._log.error('error with producer', {
                errStack: error.stack,
                error: error.message,
                method: 'BackbeatProducer.constructor',
            });
            this.emit('error', error);
        });
        return this;
    }

    /**
    * create topic - works only when auto.create.topics.enable=true for the
    * Kafka server. It sends a metadata request to the server which will create
    * the topic
    * @param {callback} cb - cb(err, data)
    * @return {this} - current instance
    */
    createTopic(cb) {
        this._producer.createTopics([this._topic], cb);
        return this;
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
        const messages = this._partition === undefined ?
            entries.map(item => new KeyedMessage(item.key, item.message)) :
            entries.map(item => item.message);
        const payload = [{
            topic: this._topic,
            messages,
        }];
        this._client.refreshMetadata([this._topic], () =>
            this._producer.send(payload, err => {
                if (err) {
                    this._log.error('error publishing entries', {
                        error: err,
                        method: 'BackbeatProducer.send',
                    });
                    return cb(errors.InternalError.
                        customizeDescription(err.message));
                }
                return cb();
            })
        );
        return this;
    }

    /**
    * close client connection
    * @param {callback} cb - cb(err)
    * @return {object} this - current class instance
    */
    close(cb) {
        this._producer.close(() => {
            this._ready = false;
            cb();
        });
        return this;
    }
}

module.exports = BackbeatProducer;
