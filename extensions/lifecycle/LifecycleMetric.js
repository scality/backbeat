const { Logger } = require('werelogs');

const MetricsModel = require('../../lib/models/MetricsModel');

class LifecycleMetric {
    constructor() {
        this._log = new Logger('LifecycleMetric');
    }

    withProducer(producer) {
        this._producer = producer;
        return this;
    }

    withSite(site) {
        this._site = site;
        return this;
    }

    /**
     * Setter for an instance of ActionQueueEntry.
     * @param {ActionQueueEntry} entry - The entry to publish a metric for
     * @return {LifecycleMetric} - The current instance
     */
    withEntry(entry) {
        this._entry = entry;
        return this;
    }
    
    withObjectSize(objectSize) {
        this._objectSize = objectSize;
        return this;
    }
    
    withMetricType(type) {
        this._metricType = type;
        return this;
    }

    _createProducerMessage() {
        const { size } = this._entry.getAttribute('source.object');
        // TODO: Do we really care about the target bucket?
        const { bucket, key, version } = this._entry.getAttribute('target');
        const metricsModel = new MetricsModel()
            .withExtension(constants.extension)
            .withMetricType(this._metricType)
            .withBytes(size)
            .withBucketName(bucket)
            .withObjectKey(key)
            .withVersionId(version);
        return metricsModel.serialize();
    }
    
    publishQueuedEntry(entry) {
        this.withEntry(entry)
            .withMetricType(contants.metric.queued)
            .publish()
    }

    publish() {
        const message = this._createProducerMessage();
        return this._producer.send([{ message }], err => {
            if (err) {
                this._log.error('error publishing metric', Object.assign({
                    error: err,
                    metricsType: this._metricType,
                }, this._entry.getLogInfo()));
            }
        });
    }
}

module.exports = LifecycleMetric;
