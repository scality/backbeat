'use strict'; // eslint-disable-line strict

const async = require('async');
const uuid = require('uuid/v4');

const BackbeatConsumer = require('../BackbeatConsumer');
const Probe = require('./Probe');

// In-Sync Replicas
const ISRS = 3;

const CONSUMER_FETCH_MAX_BYTES = 5000020;

// Time limit for deep healthcheck
const DH_TIMELIMIT = 60000;  // 1 minute
const DH_INTERVAL = 500; // retry intervals

/**
 * Handles healthcheck routes
 *
 * @class
 */
class Healthcheck {
    /**
     * @constructor
     * @param {object} repConfig - extensions.replication configs
     * @param {object} zkConfig - zookeeper configs
     * @param {node-zookeeper-client.Client} zkClient - zookeeper client
     * @param {BackbeatProducer} crrProducer - producer for CRR topic
     * @param {BackbeatProducer} crrStatusProducer - CRR status producer
     * @param {BackbeatProducer} metricProducer - producer for metric
     */
    constructor(repConfig, zkConfig, zkClient, crrProducer, crrStatusProducer,
    metricProducer) {
        this._repConfig = repConfig;
        this._zkConfig = zkConfig;
        this._zkClient = zkClient;
        this._crrProducer = crrProducer;
        this._crrStatusProducer = crrStatusProducer;
        this._metricProducer = metricProducer;
    }

    _checkProducersReady() {
        return this._crrProducer.isReady() && this._metricProducer.isReady()
            && this._crrStatusProducer.isReady();
    }

    _getConnectionDetails() {
        return {
            zookeeper: {
                status: this._zkClient.getState().name === 'SYNC_CONNECTED' ?
                    'ok' : 'error',
                details: this._zkClient.getState(),
            },
            kafkaProducer: {
                status: this._checkProducersReady() ? 'ok' : 'error',
            },
        };
    }

    /**
     * Checks health of in-sync replicas
     * @param {object} md - topic metadata object
     * @return {string} 'ok' if ISR is healthy, else 'error'
     */
    _checkISRHealth(md) {
        // eslint-disable-next-line consistent-return
        const keys = Object.keys(md);
        for (let i = 0; i < keys.length; i++) {
            if (md[keys[i]].isr && md[keys[i]].isr.length !== ISRS) {
                return 'error';
            }
        }
        return 'ok';
    }

    /**
     * Builds the healthcheck response
     * @param {function} cb - callback(error, data)
     * @return {undefined}
     */
    getHealthcheck(cb) {
        const client = this._crrProducer.getKafkaClient();

        client.loadMetadataForTopics([], (err, res) => {
            if (err) {
                const error = {
                    method: 'Healthcheck.getHealthcheck',
                    message: 'error calling Kafka loadMetadataForTopics',
                };
                return cb(error);
            }
            const response = res.map(i => (Object.assign({}, i)));
            const connections = {};
            try {
                const topicMD = {};
                response.forEach((obj, idx) => {
                    if (obj.metadata && obj.metadata[this._repConfig.topic]) {
                        const copy = JSON.parse(JSON.stringify(obj.metadata[
                            this._repConfig.topic]));
                        topicMD.metadata = copy;
                        response.splice(idx, 1);
                    }
                });
                response.push(topicMD);

                connections.isrHealth = this._checkISRHealth(topicMD.metadata);
            } finally {
                Object.assign(connections, this._getConnectionDetails());

                response.push({
                    internalConnections: connections,
                });

                return cb(null, response);
            }
        });
    }

    /**
     * Get deep healthcheck
     * A deep healthcheck will send entries to BackbeatProducer to produce to
     * Kafka, catch these entries using the same global Kafka topic used for
     * Backbeat CRR, but using a new BackbeatConsumer for each health request
     * @param {function} cb - callback(error, data)
     * @return {undefined}
     */
    getDeepHealthcheck(cb) {
        if (this._probe !== null) {
            // TODO: Based on previous request, given how deep healthcheck
            //  currently works, cannot handle parallel requests above cluster
            //  count. Will need to refactor this
            const error = {
                method: 'Healthcheck.getDeepHealthcheck',
                error: 'Another request still processing',
            };
            return cb(error);
        }
        if (!this._crrProducer || !this._crrStatusProducer) {
            const error = {
                method: 'Healthcheck.getDeepHealthcheck',
                error: 'Backbeat Producer not set',
            };
            return cb(error);
        }

        const start = Date.now();
        const id = `deephealthcheck-${uuid().replace(/-/g, '')}`;
        this._probe = new Probe(id);

        return async.waterfall([
            done => this._getTopicPartitions(done),
            (partitions, done) => {
                this._probe.setupStore(partitions);
                this._createEntries(id, partitions, done);
            },
            (entries, done) => this._crrProducer.send(entries, done),
            done => this._createConsumer('deephealthcheck-group', done),
        ], (err, consumer) => {
            if (err || consumer === undefined) {
                const error = {
                    method: 'Healthcheck.getDeepHealthcheck',
                    error: err,
                };
                return cb(error);
            }
            this._probe.on('collect', (partition, id) => {
                if (id === this._probe.getId()) {
                    this._probe.setStoreData(partition, 'ok');
                }
            });

            return async.retry({
                times: Number.parseInt(DH_TIMELIMIT / DH_INTERVAL, 10),
                interval: DH_INTERVAL,
                errorFilter: err => err === 'retry',
            },
            this._waitAndCheck.bind(this, start),
            (err, elapsed) => {
                if (err) {
                    // replace all undefined values as 'error'
                    this._probe.setStoreErrors();
                }
                this._probe.setStoreData('timeElapsed', elapsed);

                // cleanup
                const response = this._probe.getStore();
                this._probe.removeAllListeners();
                this._probe = null;
                consumer.close(() => {});

                return cb(null, response);
            });
        });
    }

    /**
     * Create a new Backbeat Consumer
     * @param {string} groupId - group id name
     * @param {function} cb - callback(error, response)
     * @return {undefined}
     */
    _createConsumer(groupId, cb) {
        const consumer = new BackbeatConsumer({
            zookeeper: { connectionString: this._zkConfig.connectionString },
            topic: this._repConfig.topic,
            groupId,
            concurrency: this._repConfig.queueProcessor.concurrency,
            queueProcessor: this._processKafkaEntry.bind(this),
            fetchMaxBytes: CONSUMER_FETCH_MAX_BYTES,
        });
        consumer.on('error', () => {});
        consumer.subscribe();
        return cb(null, consumer);
    }

    /**
     * Create kafka entries to send to BackbeatProducer for deep healthcheck
     * @param {string} id - unique identifier for the deep healthcheck request
     * @param {array} partitions - array of kafka topic partitions
     * @param {function} cb - callback(error, data)
     * @return {undefined}
     */
    _createEntries(id, partitions, cb) {
        const entries = partitions.map(partition => ({
            partition,
            message: '{"healthcheckKey":true}',
            key: id,
        }));

        return cb(null, entries);
    }

    /**
     * Get kafka topic partitions
     * @param {function} cb - callback(error, data)
     * @return {undefined}
     */
    _getTopicPartitions(cb) {
        const client = this._crrProducer.getKafkaClient();

        client.loadMetadataForTopics([], (err, res) => {
            if (err) {
                const error = {
                    method: 'Healthcheck._getTopicPartitions',
                    error: err,
                };
                return cb(error);
            }
            const partitions = Object.keys(res[0]).map(i =>
                Number.parseInt(i, 10));
            return cb(null, partitions);
        });
    }

    _processKafkaEntry(kafkaEntry) {
        // Here, I will be receiving all kafka entries
        // only capture entries with set property `healthcheckKey`
        const healthcheckEntry = JSON.parse(kafkaEntry.value).healthcheckKey;
        if (healthcheckEntry) {
            // I want to access partition # and return healthy for
            // that partition
            this._probe.emit('collect', kafkaEntry.partition, kafkaEntry.key);
        }

        // Possibly use `kafkaEntry.topic` to check health of important topics
        // and keep track of which topic it is.
        // if (kafkaEntry.topic === this._repConfig.topic)
    }

    _waitAndCheck(start, cb) {
        setTimeout(() => {
            const now = Date.now();
            const elapsed = now - start;
            if (this._probe.checkStore() === 0) {
                // all done, success
                return cb(null, elapsed);
            }

            if (elapsed > DH_TIMELIMIT) {
                // time limit exceeded
                return cb('stop', elapsed);
            }
            return cb('retry');
        });
    }
}

module.exports = Healthcheck;
