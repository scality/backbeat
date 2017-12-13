'use strict'; // eslint-disable-line strict

const zookeeper = require('node-zookeeper-client');

const { errors } = require('arsenal');

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
     * @param {BackbeatProducer} crrProducer - producer for CRR topic
     * @param {BackbeatProducer} metricProducer - producer for metric topic
     */
    constructor(config, logger, crrProducer, metricProducer) {
        this._zkConfig = config.zookeeper;
        this._repConfig = config.extensions.replication;
        this._queuePopulator = config.queuePopulator;
        this._logger = logger;

        this._crrProducer = crrProducer;
        this._metricProducer = metricProducer;
        this._zkClient = null;
    }

    /**
     * Check if incoming request is valid
     * @param {string} route - request route
     * @return {boolean} true/false
     */
    isValidRoute(route) {
        return routes.map(route => route.path).includes(route);
    }

    /**
     * Check if Zookeeper and Producer are connected
     * @return {boolean} true/false
     */
    isConnected() {
        return this._zkClient.getState().name === 'SYNC_CONNECTED'
            && this._crrProducer.isReady() && this._metricProducer.isReady();
    }

    _getConnectionDetails() {
        return {
            zookeeper: {
                status: this._zkClient.getState().name === 'SYNC_CONNECTED' ?
                    'ok' : 'error',
                details: this._zkClient.getState(),
            },
            kafkaProducer: {
                status: (this._crrProducer.isReady() &&
                    this._metricProducer.isReady()) ? 'ok' : 'error',
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
        const client = this._crrProducer.getProducer().client;

        client.loadMetadataForTopics([], (err, res) => {
            if (err) {
                this._logger.trace('error getting healthcheck');
                return cb(errors.InternalError);
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

    setZookeeper(cb) {
        const populatorZkPath = this._queuePopulator.zookeeperPath;
        const zookeeperUrl =
            `${this._zkConfig.connectionString}${populatorZkPath}`;

        const zkClient = zookeeper.createClient(zookeeperUrl, {
            autoCreateNamespace: this._zkConfig.autoCreateNamespace,
        });
        zkClient.connect();

        zkClient.once('error', cb);
        zkClient.once('state', event => {
            if (event.name !== 'SYNC_CONNECTED' || event.code !== 3) {
                return cb('error setting up zookeeper');
            }
            zkClient.removeAllListeners('error');
            this._zkClient = zkClient;
            return cb();
        });
    }
}

module.exports = BackbeatAPI;
