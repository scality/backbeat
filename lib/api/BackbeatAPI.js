'use strict'; // eslint-disable-line strict

const async = require('async');
const zookeeper = require('node-zookeeper-client');

const { errors } = require('arsenal');

const BackbeatProducer = require('../BackbeatProducer');
const routes = require('./routes');

// In-Sync Replicas
const ISRS = 3;

/**
 * Class representing Backbeat API endpoints and internals
 *
 * @class
 */
class BackbeatAPI {
    /**
     * @constructor
     * @param {object} config - configurations for setup
     * @param {werelogs.Logger} logger - Logger object
     */
    constructor(config, logger) {
        this._zkConfig = config.zookeeper;
        this._repConfig = config.extensions.replication;
        this._crrTopic = this._repConfig.topic;
        this._crrStatusTopic = this._repConfig.replicationStatusTopic;
        this._metricsTopic = config.metrics.topic;
        this._queuePopulator = config.queuePopulator;
        this._kafkaHost = config.kafka.hosts;
        this._logger = logger;

        this._crrProducer = null;
        this._crrStatusProducer = null;
        this._metricProducer = null;
        this._zkClient = null;
    }

    /**
     * Check if incoming request is valid
     * @param {string} route - request route
     * @return {boolean} true/false
     */
    isValidRoute(route) {
        return routes.some(r => r.path.includes(route));
    }

    /**
     * Check if Zookeeper and Producer are connected
     * @return {boolean} true/false
     */
    isConnected() {
        return this._zkClient.getState().name === 'SYNC_CONNECTED'
            && this._checkProducersReady();
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
     * @return {boolean} true if ISR health is ok
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
     * Get Kafka healthcheck
     * @param {function} cb - callback(error, data)
     * @return {undefined}
     */
    healthcheck(cb) {
        const client = this._crrProducer.getKafkaClient();

        // TODO: refactor by calling specific topic
        client.loadMetadataForTopics([], (err, res) => {
            if (err) {
                this._logger.error('error getting healthcheck');
                return cb(errors.InternalError
                    .customizeDescription('error getting healthcheck metadata' +
                    ' for topics'));
            }
            const response = res.map(i => (Object.assign({}, i)));
            const connections = {};
            const topicMD = {};
            response.forEach((obj, idx) => {
                if (obj.metadata && obj.metadata[this._repConfig.topic]) {
                    let copy;
                    try {
                        copy = JSON.parse(JSON.stringify(obj.metadata[
                            this._repConfig.topic]));
                        topicMD.metadata = copy;
                        response.splice(idx, 1);
                    } catch (e) {
                        this._logger.error('error getting topic metadata');
                    }
                }
            });
            response.push(topicMD);

            if (topicMD.metadata) {
                connections.isrHealth = this._checkISRHealth(topicMD.metadata);
            }

            Object.assign(connections, this._getConnectionDetails());
            response.push({
                internalConnections: connections,
            });

            return cb(null, response);
        });
    }

    setupInternals(cb) {
        async.parallel([
            done => this._setZookeeper(done),
            done => this._setProducer(this._metricsTopic, (err, producer) => {
                if (err) {
                    return done(err);
                }
                this._metricProducer = producer;
                return done();
            }),
            done => this._setProducer(this._crrTopic, (err, producer) => {
                if (err) {
                    return done(err);
                }
                this._crrProducer = producer;
                return done();
            }),
            done => this._setProducer(this._crrStatusTopic, (err, producer) => {
                if (err) {
                    return done(err);
                }
                this._crrStatusProducer = producer;
                return done();
            }),
        ], err => {
            if (err) {
                this._logger.error('error setting up internal clients');
                return cb(err);
            }
            this._logger.info('BackbeatAPI setup ready');
            return cb();
        });
    }

    _setProducer(topic, cb) {
        const producer = new BackbeatProducer({
            zookeeper: { connectionString: this._zkConfig.connectionString },
            topic,
        });

        producer.once('error', cb);
        producer.once('ready', () => {
            producer.removeAllListeners('error');
            producer.on('error', error => {
                this._logger.error('error from backbeat producer', { error });
            });
            return cb(null, producer);
        });
    }

    _setZookeeper(cb) {
        const populatorZkPath = this._queuePopulator.zookeeperPath;
        const zookeeperUrl =
            `${this._zkConfig.connectionString}${populatorZkPath}`;

        const zkClient = zookeeper.createClient(zookeeperUrl, {
            autoCreateNamespace: this._zkConfig.autoCreateNamespace,
        });
        zkClient.connect();

        zkClient.once('error', cb);
        zkClient.once('connected', () => {
            zkClient.removeAllListeners('error');
            this._zkClient = zkClient;
            return cb();
        });
    }
}

module.exports = BackbeatAPI;
