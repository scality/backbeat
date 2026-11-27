const joi = require('joi');

const KafkaProducer = require('../destination/KafkaProducer');

/**
 * Producer used by the delivery worker to publish notifications to an
 * external destination.
 *
 * It behaves like the notification KafkaProducer, with an added bound on
 * how long librdkafka keeps retrying a message before it expires. The
 * delivery worker holds the consumer offset until the delivery report is
 * received, so an unbounded retry would block the offset forever.
 */
class DeliveryKafkaProducer extends KafkaProducer {

    getConfigJoi() {
        return super.getConfigJoi()
            .append({ deliveryTimeoutMs: joi.number() });
    }

    getClientId() {
        return 'NotificationDeliveryProducer';
    }

    setFromConfig(joiResult) {
        super.setFromConfig(joiResult);
        this._deliveryTimeoutMs = joiResult.deliveryTimeoutMs;
    }

    get topicConfig() {
        const base = super.topicConfig;
        if (this._deliveryTimeoutMs === undefined) {
            return base;
        }
        return {
            ...base,
            'message.timeout.ms': this._deliveryTimeoutMs,
        };
    }

}

module.exports = DeliveryKafkaProducer;
