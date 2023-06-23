const assert = require('assert');
const {
    circuitBreakerGauge,
    startCircuitBreakerMetricsExport,
    updateCircuitBreakerConfigForImplicitOutputQueue
} = require('../../../../lib/CircuitBreaker');

describe('updateCircuitBreakerConfigForImplicitOutputQueue', () => {
    it('should inject kafka conf if implicit flag', () => {
        const cbConf = {
            probes: [
                {
                    type: 'kafkaConsumerLag',
                    implicitSingleOutputTopic: true,
                }
            ],
        };

        const res = updateCircuitBreakerConfigForImplicitOutputQueue(
            cbConf, 'group', 'topic'
        );

        assert.deepStrictEqual(res, {
            probes: [
                {
                    type: 'kafkaConsumerLag',
                    consumerGroupName: 'group',
                    topicName: 'topic',
                }
            ],
        });
    });

    it('should inject topic conf if implicit flag and group not provided', () => {
        const cbConf = {
            probes: [
                {
                    type: 'kafkaConsumerLag',
                    implicitSingleOutputTopic: true,
                    consumerGroupName: 'group',
                }
            ],
        };

        const res = updateCircuitBreakerConfigForImplicitOutputQueue(
            cbConf, null, 'topic'
        );

        assert.deepStrictEqual(res, {
            probes: [
                {
                    type: 'kafkaConsumerLag',
                    consumerGroupName: 'group',
                    topicName: 'topic',
                }
            ],
        });
    });

    it('should inject consumer conf if implicit flag and topic not provided', () => {
        const cbConf = {
            probes: [
                {
                    type: 'kafkaConsumerLag',
                    implicitSingleOutputTopic: true,
                    topicName: 'topic',
                }
            ],
        };

        const res = updateCircuitBreakerConfigForImplicitOutputQueue(
            cbConf, 'group', null
        );

        assert.deepStrictEqual(res, {
            probes: [
                {
                    type: 'kafkaConsumerLag',
                    consumerGroupName: 'group',
                    topicName: 'topic',
                }
            ],
        });
    });

    it('should not inject kafka conf if no implicit flag', () => {
        const cbConf = {
            probes: [
                {
                    type: 'kafkaConsumerLag',
                }
            ],
        };

        const res = updateCircuitBreakerConfigForImplicitOutputQueue(
            cbConf, 'group', 'topic'
        );

        assert.deepStrictEqual(res, {
            probes: [
                {
                    type: 'kafkaConsumerLag',
                }
            ],
        });
    });

    it('should not inject kafka conf if implicit flag false', () => {
        const cbConf = {
            probes: [
                {
                    type: 'kafkaConsumerLag',
                    implicitSingleOutputTopic: false,
                }
            ],
        };

        const res = updateCircuitBreakerConfigForImplicitOutputQueue(
            cbConf, 'group', 'topic'
        );

        assert.deepStrictEqual(res, {
            probes: [
                {
                    type: 'kafkaConsumerLag',
                }
            ],
        });
    });

    it('should not inject kafka conf if wrong probe type', () => {
        const cbConf = {
            probes: [
                {
                    type: 'noop',
                    implicitSingleOutputTopic: true,
                }
            ],
        };

        const res = updateCircuitBreakerConfigForImplicitOutputQueue(
            cbConf, 'group', 'topic'
        );

        assert.deepStrictEqual(res, {
            probes: [
                {
                    type: 'noop',
                    implicitSingleOutputTopic: true,
                }
            ],
        });
    });

    it('should not inject kafka conf if no probes', () => {
        const cbConf = {};

        const res = updateCircuitBreakerConfigForImplicitOutputQueue(
            cbConf, 'group', 'topic'
        );

        assert.deepStrictEqual(res, {});
    });
});

describe('startCircuitBreakerMetricsExport', () => {
    it('should export circuit breaker state', done => {
        const cb = { state: 1234 };
        startCircuitBreakerMetricsExport(cb, 'test', 10);
        setTimeout(async () => {
            const { values: [{ value, labels }] } = await circuitBreakerGauge.get();
            assert.deepStrictEqual(labels.type, 'test');
            assert.deepStrictEqual(value, 1234);
            done();
        }, 20);
    });
});
