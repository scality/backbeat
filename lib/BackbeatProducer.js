const { EventEmitter } = require('events');
const { Client, KeyedMessage, Producer } = require('kafka-node');
const joi = require('joi');

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

const CLIENT_ID = 'BackbeatProducer';

class BackbeatProducer extends EventEmitter {

    /**
    * constructor
    * @param {Object} config - config
    * @param {string} config.topic - Kafka topic to write to
    * @param {number} [config.partition] - partition in a topic to write to
    * @param {Object} [config.zookeeper] - zookeeper endpoint config
    * @param {string} config.zookeeper.connectionString - zookeeper connection
    * string as "host:port[/chroot]"
    */
    constructor(config) {
        super();

        const configJoi = {
            zookeeper: {
                connectionString: joi.string().required(),
            },
            sslOptions: joi.object(),
            topic: joi.string().required(),
            partition: joi.number(),
        };
        const validConfig = joi.attempt(config, configJoi,
                                        'invalid config params');
        const { zookeeper, sslOptions, topic, partition } = validConfig;

        this._partition = partition;
        this._zookeeperEndpoint = zookeeper.connectionString;
        this._log = new Logger(CLIENT_ID);
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
            this._log.error('error with producer', {
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
        const payload = this._partition === undefined ?
                  entries.map(item => ({
                      topic: this._topic,
                      messages: new KeyedMessage(item.key, item.message),
                      key: item.key,
                  })) :
                  entries.map(item => ({
                      topic: this._topic,
                      messages: item.message,
                      partition: this._partition,
                  }));
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
