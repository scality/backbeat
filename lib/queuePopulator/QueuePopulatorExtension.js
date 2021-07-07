const zookeeper = require('../clients/zookeeper');
const assert = require('assert');

const zookeeper = require('../clients/zookeeper');

class QueuePopulatorExtension {
    /**
     * @constructor
     * @param {Object} params - constructor params
     * @param {Object} params.config - extension-specific
     *   configuration object
     * @param {Logger} params.logger - logger object
     */
    constructor(params) {
        this.extConfig = params.config;
        this.log = params.logger;
        this._batch = null;

        // metrics
        this._metricsStore = {};
    }

    /**
     * Set the zookeeper configuration
     * @param {Object} zkConfig - The zookeeper configuration
     * @return {QueuePopulatorExtension} Current instance of
     * QueuePopulatorExtension
     */
    setZkConfig(zkConfig) {
        this.zkConfig = zkConfig;
        return this;
    }

    /**
     * Create a zookeeper client for the extension
     * @param  {Function} cb - The callback function
     * @return {undefined}
     */
    setupZookeeper(cb) {
        const { connectionString, autoCreateNamespace } = this.zkConfig;
        this.log.info('opening zookeeper connection for populator extensions', {
            zookeeperUrl: connectionString,
        });
        this.zkClient = zookeeper.createClient(connectionString, {
            autoCreateNamespace,
        });
        this.zkClient.connect();
        this.zkClient.once('error', cb);
        this.zkClient.once('ready', () => {
            // just in case there would be more 'error' events emitted
            this.zkClient.removeAllListeners('error');
            cb();
        });
    }

    /**
     * Prepare a kafka entry for publishing
     *
     * This function is meant to be called synchronously by subclasses
     * in filter() implementation, called when new metadata log
     * entries are received. It's extension's responsibility to decide
     * which topic(s) to publish to and what payload to include in
     * kafka messages then.
     *
     * @param {string} topic - topic name
     * @param {string} key - key of the kafka entry, for keyed partitioning
     * @param {string} message - kafka message
     * @param {Object} [optEntriesToPublish] - optional batch
     * @return {undefined}
     */
    publish(topic, key, message, optEntriesToPublish) {
        let __batch;
        if (optEntriesToPublish) {
            __batch = optEntriesToPublish;
        } else {
            __batch = this._batch;
        }

        assert(__batch,
               'logic error: QueuePopulatorExtension.publish() called ' +
               'without an active batch. Please make sure it\'s called ' +
               'synchronously from the filter() method.');

        const kafkaEntry = { key: encodeURIComponent(key), message };
        this.log.trace('queueing kafka entry to topic',
                       { key: kafkaEntry.key, topic });
        if (__batch[topic] === undefined) {
            __batch[topic] = [kafkaEntry];
        } else {
            __batch[topic].push(kafkaEntry);
        }
    }

    /**
     * Process a metadata log entry in extension subclass
     *
     * This method should be implemented by subclasses of
     * QueuePopulatorExtension for synchronous filtering
     * @param {Object} entry - metadata log entry
     * @param {string} entry.bucket - bucket name
     * @param {string} entry.type - entry type ('put'|'del')
     * @param {string} entry.key - object key in log
     * @param {string} entry.value - object value in log
     * @return {undefined}
     */
    filter(entry) { // eslint-disable-line no-unused-vars
        // sub classes can also implement 'filterAsync' for asynchronous
        // filtering operations.
    }

    /**
     * Internal use by QueuePopulator
     *
     * @param {Object} batch - current batch to be published
     * @return {undefined}
     */
    setBatch(batch) {
        this._batch = batch;
    }

    unsetBatch() {
        this._batch = null;
    }

    /**
     * Get currently stored metrics and reset the counters
     * @return {Object} metrics accumulated since last called
     */
    getAndResetMetrics() {
        const tempStore = this._metricsStore;
        this._metricsStore = {};
        return tempStore;
    }

    /**
     * Set or accumulate metrics based on site
     * @param {String} site - name of site
     * @param {Number} bytes - total bytes to set or increment by
     * @return {undefined}
     */
    _incrementMetrics(site, bytes) {
        if (!this._metricsStore[site]) {
            this._metricsStore[site] = {
                ops: 1,
                bytes,
            };
        } else {
            this._metricsStore[site].ops++;
            this._metricsStore[site].bytes += bytes;
        }
    }
}

module.exports = QueuePopulatorExtension;
