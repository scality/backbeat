'use strict'; // eslint-disable-line strict

const IN_SYNC_REPLICAS = 3;

/**
 * Handles healthcheck routes
 *
 * @class
 */
class Healthcheck {
    /**
     * @constructor
     * @param {object} repConfig - extensions.replication configs
     * @param {node-zookeeper-client.Client} zkClient - zookeeper client
     * @param {BackbeatProducer} crrProducer - producer for CRR topic
     * @param {BackbeatProducer} crrStatusProducer - CRR status producer
     * @param {BackbeatProducer} metricProducer - producer for metric
     */
    constructor(repConfig, zkClient, crrProducer, crrStatusProducer,
    metricProducer) {
        this._repConfig = repConfig;
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
     * @param {object} topicMd - topic metadata object
     * @return {string} 'ok' if ISR is healthy, else 'error'
     */
    _checkISRHealth(topicMd) {
        let status = 'ok';
        topicMd.partitions.forEach(partition => {
            if (partition.isrs &&
                partition.isrs.length !== IN_SYNC_REPLICAS) {
                status = 'error';
            }
        });
        return status;
    }

    /**
     * Ensure that there is an in-sync replica for each partition.
     * @param {object} topicMd - topic metadata object
     * @return {boolean} true if each partition has a replica, false otherwise
     */
    _isMissingISR(topicMd) {
        return topicMd.partitions.some(partition =>
            (partition.isrs && partition.isrs.length === 0));
    }

    /**
     * Builds the healthcheck response
     * @param {function} cb - callback(error, data)
     * @return {undefined}
     */
    getHealthcheck(cb) {
        const params = { topic: this._repConfig.topic,
                         timeout: 10000 };
        this._crrProducer.getMetadata(params, (err, res) => {
            if (err) {
                const error = {
                    method: 'Healthcheck.getHealthcheck',
                    error: 'error getting healthcheck metadata for topics',
                };
                return cb(error);
            }
            const response = {};
            const topics = {};
            const connections = {};
            const topicMd = res.topics.find(
                topic => topic.name === this._repConfig.topic);
            if (topicMd) {
                topics[this._repConfig.topic] = topicMd;
                connections.isrHealth = this._checkISRHealth(topicMd);
            }
            if (topicMd && this._isMissingISR(topicMd)) {
                const error = {
                    method: 'Healthcheck.getHealthcheck',
                    error: 'no in-sync replica for partition',
                    topicMd,
                };
                return cb(error);
            }
            Object.assign(connections, this._getConnectionDetails());
            response.topics = topics;
            response.internalConnections = connections;
            return cb(null, response);
        });
    }
}

module.exports = Healthcheck;
