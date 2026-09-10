const DeliveryKafkaProducer = require('../deliveryWorker/DeliveryKafkaProducer');

/**
 * Which producer serves a destination.
 *
 * Everything keeps using node-rdkafka except a kerberos destination on a
 * process configured for the pure JS stack. A kerberos destination is the
 * only case where the client stack changes what the destination can do at
 * all: node-rdkafka takes the process default GSSAPI credential, so the
 * second kerberised destination in a process authenticates as the first
 * one's principal.
 *
 * @param {Object} destConfig - destination configuration
 * @param {string} kerberosProducer - configured stack for kerberos
 *   destinations, 'rdkafka' or 'kafkajs'
 * @return {boolean} whether the pure JS kerberos producer should be used
 */
function usesKerberosProducer(destConfig, kerberosProducer) {
    const auth = destConfig && destConfig.auth;
    return kerberosProducer === 'kafkajs' && !!auth && auth.type === 'kerberos';
}

/**
 * Build a producer for one destination.
 *
 * @param {Object} params - factory params
 * @param {Object} params.destConfig - destination configuration
 * @param {Object} params.producerConfig - configuration to hand the producer,
 *   the shape KafkaProducer takes
 * @param {string} [params.kerberosProducer] - configured stack for kerberos
 *   destinations, defaults to 'rdkafka'
 * @return {EventEmitter} producer exposing send(entries, cb) and close(cb),
 *   emitting 'ready' or 'error'
 */
function createDeliveryProducer(params) {
    const { destConfig, producerConfig, kerberosProducer } = params;
    if (usesKerberosProducer(destConfig, kerberosProducer)) {
        // required here so that a deployment leaving the flag off never loads
        // the native GSSAPI binding
        const KerberosKafkaProducer = require('./KerberosKafkaProducer');
        return new KerberosKafkaProducer(producerConfig);
    }
    return new DeliveryKafkaProducer(producerConfig);
}

module.exports = {
    createDeliveryProducer,
    usesKerberosProducer,
};
