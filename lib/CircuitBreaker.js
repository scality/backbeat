'use strict'; // eslint-disable-line
/* eslint no-param-reassign: 0 */

function updateCircuitBreakerConfigForImplicitOutputQueue(cbConf, groupId, topic) {
    if (!cbConf || !cbConf.probes) {
        return cbConf;
    }

    cbConf.probes.forEach(p => {
        if (p.type !== 'kafkaConsumerLag') {
            return cbConf;
        }

        if (!p.implicitSingleOutputTopic) {
            return cbConf;
        }

        delete p.implicitSingleOutputTopic;

        if (groupId) {
            p.consumerGroupName = groupId;
        }

        if (topic) {
            p.topicName = topic;
        }

        return undefined;
    });

    return cbConf;
}

module.exports = {
    updateCircuitBreakerConfigForImplicitOutputQueue
};
