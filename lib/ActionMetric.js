const { Logger } = require('werelogs');

const MetricsModel = require('./models/MetricsModel');

class ActionMetric {
    constructor() {
        this._log = new Logger('ActionMetric');
    }

    withProducer(producer) {
        this._producer = producer;
        return this;
    }

    withSite(site) {
        this._site = site;
        return this;
    }

    withObjectSize(objectSize) {
        this._objectSize = objectSize;
        return this;
    }

    withEntry(entry) {
        this._entry = entry;
        return this;
    }

    withMetricType(type) {
        this._metricsType = type;
        return this;
    }

    withExtension(extension) {
        this._extension = extension;
        return this;
    }

    _isLifecycleAction() {
        const { origin } = this._entry.getContext();
        return origin !== undefined && origin === 'lifecycle';
    }

    _createProducerMessage() {
        const { bucket, key, version } = this._entry.getAttribute('target');
        const metricsModel = new MetricsModel()
            .withBytes(this._objectSize)
            .withExtension(this._extension)
            .withType(this._metricsType)
            .withSite(this._site)
            .withBucketName(bucket)
            .withObjectKey(key)
            .withVersionId(version);
        return metricsModel.serialize();
    }

    publish() {
        // Lifecycle metrics not yet implemented.
        if (this._isLifecycleAction()) {
            return undefined;
        }
        const message = this._createProducerMessage();
        return this._producer.send([{ message }], err => {
            if (err) {
                this._log.error('error publishing metric', Object.assign({
                    error: err,
                    metricsType: this._metricsType,
                }, this._entry.getLogInfo()));
            }
        });
    }
}

module.exports = ActionMetric;
