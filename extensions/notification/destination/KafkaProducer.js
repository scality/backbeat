const joi = require('joi');
const { authSchema } = require('../NotificationConfigValidator');

const BackbeatProducer = require('../../../lib/BackbeatProducer');
const authUtil = require('../utils/auth');

class KafkaProducer extends BackbeatProducer {

    getConfigJoi() {
        return super.getConfigJoi()
            .append({ auth: authSchema })
            .keys({ topic: joi.string() });
    }

    getClientId() {
        return 'NotificationProducer';
    }

    setFromConfig(joiResult) {
        super.setFromConfig(joiResult);
        this._auth = joiResult.auth ? authUtil.generateKafkaAuthObject(joiResult.auth) : {};
    }

    get producerConfig() {
        const base = super.producerConfig;
        return {
            ...base,
            ...this._auth
        };
    }

}

module.exports = KafkaProducer;
