'use strict'; // eslint-disable-line
/* eslint no-param-reassign: 0 */

const { ZenkoMetrics } = require('arsenal').metrics;

const collectDefaultMetricsIntervalMs = 10000;

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

const circuitBreakerGauge = ZenkoMetrics.createGauge({
    name: 's3_circuit_breaker',
    help: 'Circuit Breaker State',
    labelNames: ['type'],
});

const circuitBreakerCounter = ZenkoMetrics.createCounter({
    name: 's3_circuit_breaker_errors_count',
    help: 'total number of circuit breaker errors',
    labelNames: [],
});

function startCircuitBreakerMetricsExport(cb, cbType, intervalMs = collectDefaultMetricsIntervalMs) {
    const type = cbType || 'generic';
    setInterval(() => {
        if (cb.failedProbes) {
            circuitBreakerCounter.inc();
        }
        circuitBreakerGauge.set({ type }, cb.state);
    }, intervalMs);
}

module.exports = {
    circuitBreakerCounter,
    circuitBreakerGauge,
    startCircuitBreakerMetricsExport,
    updateCircuitBreakerConfigForImplicitOutputQueue,
};
