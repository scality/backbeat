const assert = require('assert');

class QueuePopulatorExtension {
    /**
     * @constructor
     * @param {Object} params - constructor params
     * @param {Object} params.config - extension-specific
     *   configuration object
     * @param {Logger} params.logger - logger object
     */
    constructor(params) {
        this.log = params.logger;
        this._batch = null;
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
     * @return {undefined}
     */
    publish(topic, key, message) {
        assert(this._batch,
               'logic error: QueuePopulatorExtension.publish() called ' +
               'without an active batch. Please make sure it\'s called ' +
               'synchronously from the filter() method.');

        const kafkaEntry = { key: encodeURIComponent(key), message };
        this.log.trace('queueing kafka entry to topic',
                       { key: kafkaEntry.key, topic });
        if (this._batch[topic] === undefined) {
            this._batch[topic] = [kafkaEntry];
        } else {
            this._batch[topic].push(kafkaEntry);
        }
    }

    /**
     * Process a metadata log entry in extension subclass
     *
     * This method must be implemented by subclasses of QueuePopulatorExtension
     * @param {Object} entry - metadata log entry
     * @param {string} entry.bucket - bucket name
     * @param {string} entry.type - entry type ('put'|'del')
     * @param {string} entry.key - object key in log
     * @param {string} entry.value - object value in log
     * @return {undefined}
     */
    filter(entry) { // eslint-disable-line no-unused-vars
        assert(false,
               'sub-classes of QueuePopulatorExtension must implement ' +
               'the filter() method');
    }

    /**
     * Internal use by QueuePopulator
     *
     * @param {Object} batch - batch to publish
     * @return {undefined}
     */
    setBatch(batch) {
        this._batch = batch;
    }

    endBatch() {
        this._batch = null;
    }
}

module.exports = QueuePopulatorExtension;
