'use strict'; // eslint-disable-line
/* eslint no-param-reassign: 0 */

function updateCircuitBreakerConfigForImplicitOutputQueue(cbConf, groupId, topic) {
    if (!cbConf || !cbConf.probes) {
        return cbConf;
    }

    cbConf.probes.forEach(p => {
        if (p.type !== 'kafkaConsumerLag') {
            return;
        }

        if (!Object.prototype.hasOwnProperty.call(p, 'implicitSingleOutputTopic')) {
            return;
        }

        const implicitSingleOutputTopic = p.implicitSingleOutputTopic;
        delete p.implicitSingleOutputTopic;

        if (!implicitSingleOutputTopic) {
            return;
        }

        if (groupId) {
            p.consumerGroupName = groupId;
        }

        if (topic) {
            p.topicName = topic;
        }

        return;
    });

    return cbConf;
}

module.exports = {
    updateCircuitBreakerConfigForImplicitOutputQueue,
};
