/* eslint no-template-curly-in-string: 0 */
const assert = require('assert');
const {
    extractBucketProcessorCircuitBreakerConfigs,
    CircuitBreakerGroup,
} = require('../../../extensions/lifecycle/CircuitBreakerGroup');
const config = require('../../config.json');
const locations = require('../../../conf/locationConfig.json');
const logger = require('../../utils/fakeLogger');
const { BreakerState } = require('breakbeat').CircuitBreaker;

describe('extractBucketProcessorCircuitBreakerConfigs', () => {
    function formatProbeConfig(probe, template, value) {
        const withClause = probe.query.match(/^when\s?\(\{(.*?)\}\)\sand\s/);
        let query = withClause ? probe.query.replace(withClause[0], '') : probe.query;
        if (template) {
            query = query.replaceAll(template, value);
        }
        return {
            nominalEvaluateIntervalMs: 60000,
            stabilizingEvaluateIntervalMs: 60000,
            trippedEvaluateIntervalMs: 60000,
            stabilizeAfterNSuccesses: 2,
            probes: [{
                ...probe,
                query
            }],
        };
    }

    const globalProbe = {
        type: 'prometheusQuery',
        query: 'kafka_consumergroup_group_lag{}',
        threshold: 100,
        prometheus: {
            endpoint: 'http://prometheus:9090',
        },
    };

    const topicTemplatedProbe = {
        type: 'prometheusQuery',
        query: 'kafka_consumergroup_group_lag{topic="${topic}"}',
        threshold: 100,
        prometheus: {
            endpoint: 'http://prometheus:9090',
        },
    };

    const topicSpecificTemplatedProbe = {
        type: 'prometheusQuery',
        query: 'when({topic="cold-archive-req-location-dmf-v1"}) ' +
            'and kafka_consumergroup_group_lag{group="${topic}",topic="${topic}"}',
        threshold: 100,
        prometheus: {
            endpoint: 'http://prometheus:9090',
        },
    };

    const topicSpecificNoTemplateProbe = {
        type: 'prometheusQuery',
        query: 'when({topic="cold-archive-req-location-dmf-v1"}) and kafka_consumergroup_group_lag{}',
        threshold: 100,
        prometheus: {
            endpoint: 'http://prometheus:9090',
        },
    };

    const topicSpecificLocationTemplateProbe = {
        type: 'prometheusQuery',
        query: 'when({topic="cold-archive-req-location-dmf-v1"}) and s3_sorbet_is_throttled{location="${location}"}',
        threshold: 100,
        prometheus: {
            endpoint: 'http://prometheus:9090',
        },
    };

    const locationTemplatedProbe = {
        type: 'prometheusQuery',
        query: 's3_sorbet_is_throttled{location="${location}"}',
        threshold: 100,
        prometheus: {
            endpoint: 'http://prometheus:9090',
        },
    };

    const locationSpecificTemplatedProbe = {
        type: 'prometheusQuery',
        query: 'when({location="dmf-v1"}) and kafka_consumergroup_group_lag{group="${location}", topic="${location}"}',
        threshold: 100,
        prometheus: {
            endpoint: 'http://prometheus:9090',
        },
    };

    const locationSpecificNoTemplateProbe = {
        type: 'prometheusQuery',
        query: 'when({location="dmf-v1"}) and kafka_consumergroup_group_lag{}',
        threshold: 100,
        prometheus: {
            endpoint: 'http://prometheus:9090',
        },
    };

    const locationSpecificTopicTemplateProbe = {
        type: 'prometheusQuery',
        query: 'when({location="dmf-v1"}) and kafka_consumergroup_group_lag{topic="${topic}"}',
        threshold: 100,
        prometheus: {
            endpoint: 'http://prometheus:9090',
        },
    };

    const locationAndTopicSpecificNoTemplateProbe = {
        type: 'prometheusQuery',
        query: 'when({topic="cold-archive-req-location-dmf-v1",location="dmf-v1"}) and kafka_consumergroup_group_lag{}',
        threshold: 100,
        prometheus: {
            endpoint: 'http://prometheus:9090',
        },
    };

    const invalidDoubleTemplate = {
        type: 'prometheusQuery',
        query: 's3_sorbet_is_throttled{location="${location}",topic="${topic}"}',
        threshold: 100,
        prometheus: {
            endpoint: 'http://prometheus:9090',
        },
    };

    const invalidInactiveProbe = {
        type: 'prometheusQuery',
        query: 'when({transition="false",expiration="false"}) and s3_sorbet_is_throttled{}',
        threshold: 100,
        prometheus: {
            endpoint: 'http://prometheus:9090',
        },
    };

    const disabledTransition = {
        type: 'prometheusQuery',
        query: 'when({transition="false"}) and s3_sorbet_is_throttled{}',
        threshold: 100,
        prometheus: {
            endpoint: 'http://prometheus:9090',
        },
    };

    const disabledExpiration = {
        type: 'prometheusQuery',
        query: 'when({expiration="false"}) and s3_sorbet_is_throttled{}',
        threshold: 100,
        prometheus: {
            endpoint: 'http://prometheus:9090',
        },
    };

    const notPrometheusQueryProbe = {
        type: 'noop',
        returnConstantValue: true,
    };

    [
        {
            it: 'should return empty object if no bucket processor circuit breaker',
            probes: [],
            expected: {
                global: {
                    nominalEvaluateIntervalMs: 60000,
                    stabilizingEvaluateIntervalMs: 60000,
                    trippedEvaluateIntervalMs: 60000,
                    stabilizeAfterNSuccesses: 2,
                    probes: [],
                },
                circuitBreakers: {
                    transition: {
                        location: {},
                        topic: {},
                        global: [],
                    },
                    expiration: {
                        location: {},
                        topic: {},
                        global: [],
                    },
                },
            },
        }, {
            it: 'should the config of global probes',
            probes: [
                globalProbe,
            ],
            expected: {
                global: {
                    nominalEvaluateIntervalMs: 60000,
                    stabilizingEvaluateIntervalMs: 60000,
                    trippedEvaluateIntervalMs: 60000,
                    stabilizeAfterNSuccesses: 2,
                    probes: [
                        {
                            ...globalProbe
                        },
                    ],
                },
                circuitBreakers: {
                    transition: {
                        location: {},
                        topic: {},
                        global: [],
                    },
                    expiration: {
                        location: {},
                        topic: {},
                        global: [],
                    },
                },
            },
        }, {
            it: 'should build circuit breaker for each topic',
            probes: [
                topicTemplatedProbe,
            ],
            expected: {
                global: {
                    nominalEvaluateIntervalMs: 60000,
                    stabilizingEvaluateIntervalMs: 60000,
                    trippedEvaluateIntervalMs: 60000,
                    stabilizeAfterNSuccesses: 2,
                    probes: [],
                },
                circuitBreakers: {
                    transition: {
                        location: {},
                        topic: {
                            'backbeat-test-dummy-object-task': [
                                formatProbeConfig(
                                    topicTemplatedProbe,
                                    '${topic}',
                                    'backbeat-test-dummy-object-task',
                                ),
                            ],
                            'backbeat-data-mover': [
                                formatProbeConfig(
                                    topicTemplatedProbe,
                                    '${topic}',
                                    'backbeat-data-mover',
                                ),
                            ],
                            'cold-archive-req-location-dmf-v1': [
                                formatProbeConfig(
                                    topicTemplatedProbe,
                                    '${topic}',
                                    'cold-archive-req-location-dmf-v1',
                                ),
                            ],
                        },
                        global: [],
                    },
                    expiration: {
                        location: {},
                        topic: {
                            'backbeat-test-dummy-object-task': [
                                formatProbeConfig(
                                    topicTemplatedProbe,
                                    '${topic}',
                                    'backbeat-test-dummy-object-task',
                                ),
                            ],
                            'backbeat-data-mover': [
                                formatProbeConfig(
                                    topicTemplatedProbe,
                                    '${topic}',
                                    'backbeat-data-mover',
                                ),
                            ],
                            'cold-archive-req-location-dmf-v1': [
                                formatProbeConfig(
                                    topicTemplatedProbe,
                                    '${topic}',
                                    'cold-archive-req-location-dmf-v1',
                                ),
                            ],
                        },
                        global: [],
                    },
                },
            },
        }, {
            it: 'should build circuit breaker for specified topic and replace template',
            probes: [
                topicSpecificTemplatedProbe,
            ],
            expected: {
                global: {
                    nominalEvaluateIntervalMs: 60000,
                    stabilizingEvaluateIntervalMs: 60000,
                    trippedEvaluateIntervalMs: 60000,
                    stabilizeAfterNSuccesses: 2,
                    probes: [],
                },
                circuitBreakers: {
                    transition: {
                        location: {},
                        topic: {
                            'cold-archive-req-location-dmf-v1': [
                                formatProbeConfig(
                                    topicSpecificTemplatedProbe,
                                    '${topic}',
                                    'cold-archive-req-location-dmf-v1',
                                ),
                            ],
                        },
                        global: [],
                    },
                    expiration: {
                        location: {},
                        topic: {
                            'cold-archive-req-location-dmf-v1': [
                                formatProbeConfig(
                                    topicSpecificTemplatedProbe,
                                    '${topic}',
                                    'cold-archive-req-location-dmf-v1',
                                ),
                            ],
                        },
                        global: [],
                    },
                },
            },
        }, {
            it: 'should build circuit breaker for specified topic when no template set',
            probes: [
                topicSpecificNoTemplateProbe,
            ],
            expected: {
                global: {
                    nominalEvaluateIntervalMs: 60000,
                    stabilizingEvaluateIntervalMs: 60000,
                    trippedEvaluateIntervalMs: 60000,
                    stabilizeAfterNSuccesses: 2,
                    probes: [],
                },
                circuitBreakers: {
                    transition: {
                        location: {},
                        topic: {
                            'cold-archive-req-location-dmf-v1': [
                                formatProbeConfig(
                                    topicSpecificNoTemplateProbe,
                                    '',
                                    '',
                                ),
                            ],
                        },
                        global: [],
                    },
                    expiration: {
                        location: {},
                        topic: {
                            'cold-archive-req-location-dmf-v1': [
                                formatProbeConfig(
                                    topicSpecificNoTemplateProbe,
                                    '',
                                    '',
                                ),
                            ],
                        },
                        global: [],
                    },
                },
            },
        }, {
            it: 'should build circuit breaker for specified topic and all location when location template set',
            probes: [
                topicSpecificLocationTemplateProbe,
            ],
            expected: {
                global: {
                    nominalEvaluateIntervalMs: 60000,
                    stabilizingEvaluateIntervalMs: 60000,
                    trippedEvaluateIntervalMs: 60000,
                    stabilizeAfterNSuccesses: 2,
                    probes: [],
                },
                circuitBreakers: {
                    transition: {
                        location: {
                            'us-east-1': [
                                formatProbeConfig(
                                    topicSpecificLocationTemplateProbe,
                                    '${location}',
                                    'us-east-1',
                                ),
                            ],
                            'us-east-2': [
                                formatProbeConfig(
                                    topicSpecificLocationTemplateProbe,
                                    '${location}',
                                    'us-east-2',
                                ),
                            ],
                            'wontwork-location': [
                                formatProbeConfig(
                                    topicSpecificLocationTemplateProbe,
                                    '${location}',
                                    'wontwork-location',
                                ),
                            ],
                            'location-dmf-v1': [
                                formatProbeConfig(
                                    topicSpecificLocationTemplateProbe,
                                    '${location}',
                                    'location-dmf-v1',
                                ),
                            ],
                        },
                        topic: {
                            'cold-archive-req-location-dmf-v1': [
                                formatProbeConfig(
                                    topicSpecificLocationTemplateProbe,
                                    '${location}',
                                    'us-east-1',
                                ),
                                formatProbeConfig(
                                    topicSpecificLocationTemplateProbe,
                                    '${location}',
                                    'us-east-2',
                                ),
                                formatProbeConfig(
                                    topicSpecificLocationTemplateProbe,
                                    '${location}',
                                    'wontwork-location',
                                ),
                                formatProbeConfig(
                                    topicSpecificLocationTemplateProbe,
                                    '${location}',
                                    'location-dmf-v1',
                                ),
                            ],
                        },
                        global: [],
                    },
                    expiration: {
                        location: {
                            'us-east-1': [
                                formatProbeConfig(
                                    topicSpecificLocationTemplateProbe,
                                    '${location}',
                                    'us-east-1',
                                ),
                            ],
                            'us-east-2': [
                                formatProbeConfig(
                                    topicSpecificLocationTemplateProbe,
                                    '${location}',
                                    'us-east-2',
                                ),
                            ],
                            'wontwork-location': [
                                formatProbeConfig(
                                    topicSpecificLocationTemplateProbe,
                                    '${location}',
                                    'wontwork-location',
                                ),
                            ],
                            'location-dmf-v1': [
                                formatProbeConfig(
                                    topicSpecificLocationTemplateProbe,
                                    '${location}',
                                    'location-dmf-v1',
                                ),
                            ],
                        },
                        topic: {
                            'cold-archive-req-location-dmf-v1': [
                                formatProbeConfig(
                                    topicSpecificLocationTemplateProbe,
                                    '${location}',
                                    'us-east-1',
                                ),
                                formatProbeConfig(
                                    topicSpecificLocationTemplateProbe,
                                    '${location}',
                                    'us-east-2',
                                ),
                                formatProbeConfig(
                                    topicSpecificLocationTemplateProbe,
                                    '${location}',
                                    'wontwork-location',
                                ),
                                formatProbeConfig(
                                    topicSpecificLocationTemplateProbe,
                                    '${location}',
                                    'location-dmf-v1',
                                ),
                            ],
                        },
                        global: [],
                    },
                },
            },
        }, {
            it: 'should build circuit breaker for each location when template is set',
            probes: [
                locationTemplatedProbe,
            ],
            expected: {
                global: {
                    nominalEvaluateIntervalMs: 60000,
                    stabilizingEvaluateIntervalMs: 60000,
                    trippedEvaluateIntervalMs: 60000,
                    stabilizeAfterNSuccesses: 2,
                    probes: [],
                },
                circuitBreakers: {
                    transition: {
                        location: {
                            'us-east-1': [
                                formatProbeConfig(
                                    locationTemplatedProbe,
                                    '${location}',
                                    'us-east-1',
                                ),
                            ],
                            'us-east-2': [
                                formatProbeConfig(
                                    locationTemplatedProbe,
                                    '${location}',
                                    'us-east-2',
                                ),
                            ],
                            'wontwork-location': [
                                formatProbeConfig(
                                    locationTemplatedProbe,
                                    '${location}',
                                    'wontwork-location',
                                ),
                            ],
                            'location-dmf-v1': [
                                formatProbeConfig(
                                    locationTemplatedProbe,
                                    '${location}',
                                    'location-dmf-v1',
                                ),
                            ],
                        },
                        topic: {},
                        global: [],
                    },
                    expiration: {
                        location: {
                            'us-east-1': [
                                formatProbeConfig(
                                    locationTemplatedProbe,
                                    '${location}',
                                    'us-east-1',
                                ),
                            ],
                            'us-east-2': [
                                formatProbeConfig(
                                    locationTemplatedProbe,
                                    '${location}',
                                    'us-east-2',
                                ),
                            ],
                            'wontwork-location': [
                                formatProbeConfig(
                                    locationTemplatedProbe,
                                    '${location}',
                                    'wontwork-location',
                                ),
                            ],
                            'location-dmf-v1': [
                                formatProbeConfig(
                                    locationTemplatedProbe,
                                    '${location}',
                                    'location-dmf-v1',
                                ),
                            ],
                        },
                        topic: {},
                        global: [],
                    },
                },
            },
        }, {
            it: 'should build circuit breaker for specified location and replace template',
            probes: [
                locationSpecificTemplatedProbe,
            ],
            expected: {
                global: {
                    nominalEvaluateIntervalMs: 60000,
                    stabilizingEvaluateIntervalMs: 60000,
                    trippedEvaluateIntervalMs: 60000,
                    stabilizeAfterNSuccesses: 2,
                    probes: [],
                },
                circuitBreakers: {
                    transition: {
                        location: {
                            'dmf-v1': [
                                formatProbeConfig(
                                    locationSpecificTemplatedProbe,
                                    '${location}',
                                    'dmf-v1',
                                ),
                            ],
                        },
                        topic: {},
                        global: [],
                    },
                    expiration: {
                        location: {
                            'dmf-v1': [
                                formatProbeConfig(
                                    locationSpecificTemplatedProbe,
                                    '${location}',
                                    'dmf-v1',
                                ),
                            ],
                        },
                        topic: {},
                        global: [],
                    },
                },
            },
        }, {
            it: 'should build circuit breaker for specified location when no template set',
            probes: [
                locationSpecificNoTemplateProbe,
            ],
            expected: {
                global: {
                    nominalEvaluateIntervalMs: 60000,
                    stabilizingEvaluateIntervalMs: 60000,
                    trippedEvaluateIntervalMs: 60000,
                    stabilizeAfterNSuccesses: 2,
                    probes: [],
                },
                circuitBreakers: {
                    transition: {
                        location: {
                            'dmf-v1': [
                                formatProbeConfig(
                                    locationSpecificNoTemplateProbe,
                                    '',
                                    '',
                                ),
                            ],
                        },
                        topic: {},
                        global: [],
                    },
                    expiration: {
                        location: {
                            'dmf-v1': [
                                formatProbeConfig(
                                    locationSpecificNoTemplateProbe,
                                    '',
                                    '',
                                ),
                            ],
                        },
                        topic: {},
                        global: [],
                    },
                },
            },
        }, {
            it: 'should build circuit breaker for specified location and all topics when topic template set',
            probes: [
                locationSpecificTopicTemplateProbe,
            ],
            expected: {
                global: {
                    nominalEvaluateIntervalMs: 60000,
                    stabilizingEvaluateIntervalMs: 60000,
                    trippedEvaluateIntervalMs: 60000,
                    stabilizeAfterNSuccesses: 2,
                    probes: [],
                },
                circuitBreakers: {
                    transition: {
                        location: {
                            'dmf-v1': [
                                formatProbeConfig(
                                    locationSpecificTopicTemplateProbe,
                                    '${topic}',
                                    'backbeat-test-dummy-object-task',
                                ),
                                formatProbeConfig(
                                    locationSpecificTopicTemplateProbe,
                                    '${topic}',
                                    'backbeat-data-mover',
                                ),
                                formatProbeConfig(
                                    locationSpecificTopicTemplateProbe,
                                    '${topic}',
                                    'cold-archive-req-location-dmf-v1',
                                ),
                            ],
                        },
                        topic: {
                            'backbeat-test-dummy-object-task': [
                                formatProbeConfig(
                                    locationSpecificTopicTemplateProbe,
                                    '${topic}',
                                    'backbeat-test-dummy-object-task',
                                ),
                            ],
                            'backbeat-data-mover': [
                                formatProbeConfig(
                                    locationSpecificTopicTemplateProbe,
                                    '${topic}',
                                    'backbeat-data-mover',
                                ),
                            ],
                            'cold-archive-req-location-dmf-v1': [
                                formatProbeConfig(
                                    locationSpecificTopicTemplateProbe,
                                    '${topic}',
                                    'cold-archive-req-location-dmf-v1',
                                ),
                            ],
                        },
                        global: [],
                    },
                    expiration: {
                        location: {
                            'dmf-v1': [
                                formatProbeConfig(
                                    locationSpecificTopicTemplateProbe,
                                    '${topic}',
                                    'backbeat-test-dummy-object-task',
                                ),
                                formatProbeConfig(
                                    locationSpecificTopicTemplateProbe,
                                    '${topic}',
                                    'backbeat-data-mover',
                                ),
                                formatProbeConfig(
                                    locationSpecificTopicTemplateProbe,
                                    '${topic}',
                                    'cold-archive-req-location-dmf-v1',
                                ),
                            ],
                        },
                        topic: {
                            'backbeat-test-dummy-object-task': [
                                formatProbeConfig(
                                    locationSpecificTopicTemplateProbe,
                                    '${topic}',
                                    'backbeat-test-dummy-object-task',
                                ),
                            ],
                            'backbeat-data-mover': [
                                formatProbeConfig(
                                    locationSpecificTopicTemplateProbe,
                                    '${topic}',
                                    'backbeat-data-mover',
                                ),
                            ],
                            'cold-archive-req-location-dmf-v1': [
                                formatProbeConfig(
                                    locationSpecificTopicTemplateProbe,
                                    '${topic}',
                                    'cold-archive-req-location-dmf-v1',
                                ),
                            ],
                        },
                        global: [],
                    },
                },
            },
        }, {
            it: 'should build circuit breaker for specified location and topic when no template set',
            probes: [
                locationAndTopicSpecificNoTemplateProbe,
            ],
            expected: {
                global: {
                    nominalEvaluateIntervalMs: 60000,
                    stabilizingEvaluateIntervalMs: 60000,
                    trippedEvaluateIntervalMs: 60000,
                    stabilizeAfterNSuccesses: 2,
                    probes: [],
                },
                circuitBreakers: {
                    transition: {
                        location: {
                            'dmf-v1': [
                                formatProbeConfig(
                                    locationAndTopicSpecificNoTemplateProbe,
                                    '',
                                    '',
                                ),
                            ],
                        },
                        topic: {
                            'cold-archive-req-location-dmf-v1': [
                                formatProbeConfig(
                                    locationAndTopicSpecificNoTemplateProbe,
                                    '',
                                    '',
                                ),
                            ],
                        },
                        global: [],
                    },
                    expiration: {
                        location: {
                            'dmf-v1': [
                                formatProbeConfig(
                                    locationAndTopicSpecificNoTemplateProbe,
                                    '',
                                    '',
                                ),
                            ],
                        },
                        topic: {
                            'cold-archive-req-location-dmf-v1': [
                                formatProbeConfig(
                                    locationAndTopicSpecificNoTemplateProbe,
                                    '',
                                    '',
                                ),
                            ],
                        },
                        global: [],
                    },
                },
            },
        }, {
            it: 'should ignore probe that uses both topic and location templates',
            probes: [
                invalidDoubleTemplate,
            ],
            expected: {
                global: {
                    nominalEvaluateIntervalMs: 60000,
                    stabilizingEvaluateIntervalMs: 60000,
                    trippedEvaluateIntervalMs: 60000,
                    stabilizeAfterNSuccesses: 2,
                    probes: [],
                },
                circuitBreakers: {
                    transition: {
                        location: {},
                        topic: {},
                        global: [],
                    },
                    expiration: {
                        location: {},
                        topic: {},
                        global: [],
                    },
                },
            },
        }, {
            it: 'should ignore probe that has all workflows disabled',
            probes: [
                invalidInactiveProbe,
            ],
            expected: {
                global: {
                    nominalEvaluateIntervalMs: 60000,
                    stabilizingEvaluateIntervalMs: 60000,
                    trippedEvaluateIntervalMs: 60000,
                    stabilizeAfterNSuccesses: 2,
                    probes: [],
                },
                circuitBreakers: {
                    transition: {
                        location: {},
                        topic: {},
                        global: [],
                    },
                    expiration: {
                        location: {},
                        topic: {},
                        global: [],
                    },
                },
            },
        }, {
            it: 'should create a global circuit breaker only used for expiration',
            probes: [
                disabledTransition,
            ],
            expected: {
                global: {
                    nominalEvaluateIntervalMs: 60000,
                    stabilizingEvaluateIntervalMs: 60000,
                    trippedEvaluateIntervalMs: 60000,
                    stabilizeAfterNSuccesses: 2,
                    probes: [],
                },
                circuitBreakers: {
                    transition: {
                        location: {},
                        topic: {},
                        global: [],
                    },
                    expiration: {
                        location: {},
                        topic: {},
                        global: [
                            formatProbeConfig(
                                disabledTransition,
                                '',
                                '',
                            ),
                        ],
                    },
                },
            },
        }, {
            it: 'should create a global circuit breaker only used for transition',
            probes: [
                disabledExpiration,
            ],
            expected: {
                global: {
                    nominalEvaluateIntervalMs: 60000,
                    stabilizingEvaluateIntervalMs: 60000,
                    trippedEvaluateIntervalMs: 60000,
                    stabilizeAfterNSuccesses: 2,
                    probes: [],
                },
                circuitBreakers: {
                    transition: {
                        location: {},
                        topic: {},
                        global: [
                            formatProbeConfig(
                                disabledExpiration,
                                '',
                                '',
                            ),
                        ],
                    },
                    expiration: {
                        location: {},
                        topic: {},
                        global: [],
                    },
                },
            },
        }, {
            it: 'consider non PrometheusQuery probes global',
            probes: [
                notPrometheusQueryProbe,
            ],
            expected: {
                global: {
                    nominalEvaluateIntervalMs: 60000,
                    stabilizingEvaluateIntervalMs: 60000,
                    trippedEvaluateIntervalMs: 60000,
                    stabilizeAfterNSuccesses: 2,
                    probes: [{
                        type: 'noop',
                        returnConstantValue: true,
                    }],
                },
                circuitBreakers: {
                    transition: {
                        location: {},
                        topic: {},
                        global: [],
                    },
                    expiration: {
                        location: {},
                        topic: {},
                        global: [],
                    },
                },
            },
        }
    ].forEach(item => {
        it(item.it, () => {
            const circuitBreakerConfig = {
                nominalEvaluateIntervalMs: 60000,
                stabilizingEvaluateIntervalMs: 60000,
                trippedEvaluateIntervalMs: 60000,
                stabilizeAfterNSuccesses: 2,
                probes: item.probes,
            };
            const res = extractBucketProcessorCircuitBreakerConfigs(
                circuitBreakerConfig,
                config.extensions.lifecycle,
                config.extensions.replication,
                locations,
                logger,
            );
            assert.deepStrictEqual(res.global, item.expected.global);
            Object.keys(item.expected.circuitBreakers.transition.location).forEach(loc => {
                assert.deepStrictEqual(
                    res.circuitBreakerGroup.circuitBreakers.transition.location[loc]?.map(cb => cb._config),
                    item.expected.circuitBreakers.transition.location[loc],
                );
            });
            Object.keys(item.expected.circuitBreakers.transition.topic).forEach(topic => {
                assert.deepStrictEqual(
                    res.circuitBreakerGroup.circuitBreakers.transition.topic[topic]?.map(cb => cb._config),
                    item.expected.circuitBreakers.transition.topic[topic],
                );
            });
            Object.keys(item.expected.circuitBreakers.expiration.location).forEach(loc => {
                assert.deepStrictEqual(
                    res.circuitBreakerGroup.circuitBreakers.expiration.location[loc]?.map(cb => cb._config),
                    item.expected.circuitBreakers.expiration.location[loc],
                );
            });
            Object.keys(item.expected.circuitBreakers.expiration.topic).forEach(topic => {
                assert.deepStrictEqual(
                    res.circuitBreakerGroup.circuitBreakers.expiration.topic[topic]?.map(cb => cb._config),
                    item.expected.circuitBreakers.expiration.topic[topic],
                );
            });
            assert.deepStrictEqual(
                res.circuitBreakerGroup.circuitBreakers.expiration.global.map(cb => cb._config),
                item.expected.circuitBreakers.expiration.global,
            );
            assert.deepStrictEqual(
                res.circuitBreakerGroup.circuitBreakers.transition.global.map(cb => cb._config),
                item.expected.circuitBreakers.transition.global,
            );
        });
    });
});

const nominalBreaker = {
    state: () => BreakerState.Nominal
};

const trippedBreaker = {
    state: () => BreakerState.Tripped
};

describe('shouldCircuitBreak', () => {

    [
        {
            it: 'Should check all global circuit breakers when nothing specified',
            breakers: {
                expiration: {
                    location: {},
                    topic: {},
                    global: [
                        trippedBreaker,
                    ],
                },
                transition: {
                    location: {},
                    topic: {},
                    global: [
                        trippedBreaker,
                    ]
                },
            },
            workflow: '',
            location: '',
            topic: '',
            shouldBreak: true
        }, {
            it: 'Should check global expiration circuit breakers',
            breakers: {
                expiration: {
                    location: {},
                    topic: {},
                    global: [
                        trippedBreaker,
                    ],
                },
                transition: {
                    location: {},
                    topic: {},
                    global: [
                        nominalBreaker,
                    ]
                },
            },
            workflow: 'expiration',
            location: '',
            topic: '',
            shouldBreak: true
        }, {
            it: 'Should check global transition circuit breakers',
            breakers: {
                expiration: {
                    location: {},
                    topic: {},
                    global: [
                        nominalBreaker,
                    ],
                },
                transition: {
                    location: {},
                    topic: {},
                    global: [
                        trippedBreaker,
                    ]
                },
            },
            workflow: 'transition',
            location: '',
            topic: '',
            shouldBreak: true
        }, {
            it: 'Should check location specific breakers',
            breakers: {
                expiration: {
                    location: {
                        'dmf-v1': [
                            trippedBreaker,
                        ],
                    },
                    topic: {},
                    global: [],
                },
                transition: {
                    location: {
                        'dmf-v1': [
                            trippedBreaker,
                        ],
                    },
                    topic: {},
                    global: []
                },
            },
            workflow: '',
            location: 'dmf-v1',
            topic: '',
            shouldBreak: true
        }, {
            it: 'Should check location specific breakers enabled for transition',
            breakers: {
                expiration: {
                    location: {
                        'dmf-v1': [
                            nominalBreaker,
                        ],
                    },
                    topic: {},
                    global: [],
                },
                transition: {
                    location: {
                        'dmf-v1': [
                            trippedBreaker,
                        ],
                    },
                    topic: {},
                    global: []
                },
            },
            workflow: 'transition',
            location: 'dmf-v1',
            topic: '',
            shouldBreak: true
        }, {
            it: 'Should check location specific breakers enabled for expiration',
            breakers: {
                expiration: {
                    location: {
                        'dmf-v1': [
                            trippedBreaker,
                        ],
                    },
                    topic: {},
                    global: [],
                },
                transition: {
                    location: {
                        'dmf-v1': [
                            nominalBreaker,
                        ],
                    },
                    topic: {},
                    global: []
                },
            },
            workflow: 'expiration',
            location: 'dmf-v1',
            topic: '',
            shouldBreak: true
        }, {
            it: 'Should check topic specific breakers',
            breakers: {
                expiration: {
                    location: {},
                    topic: {
                        'dmf-v1-topic': [
                            trippedBreaker,
                        ],
                    },
                    global: [],
                },
                transition: {
                    location: {},
                    topic: {
                        'dmf-v1-topic': [
                            trippedBreaker,
                        ],
                    },
                    global: []
                },
            },
            workflow: '',
            location: '',
            topic: 'dmf-v1-topic',
            shouldBreak: true
        }, {
            it: 'Should check topic specific breakers enabled for transition',
            breakers: {
                expiration: {
                    location: {},
                    topic: {},
                    global: [],
                },
                transition: {
                    location: {},
                    topic: {
                        'dmf-v1-topic': [
                            trippedBreaker,
                        ],
                    },
                    global: []
                },
            },
            workflow: 'transition',
            location: '',
            topic: 'dmf-v1-topic',
            shouldBreak: true
        }, {
            it: 'Should check topic specific breakers enabled for expiration',
            breakers: {
                expiration: {
                    location: {},
                    topic: {
                        'dmf-v1-topic': [
                            trippedBreaker,
                        ],
                    },
                    global: [],
                },
                transition: {
                    location: {},
                    topic: {},
                    global: []
                },
            },
            workflow: 'expiration',
            location: '',
            topic: 'dmf-v1-topic',
            shouldBreak: true
        },
    ].forEach(item => {
        it(item.it, () => {
            const circuitBreakerCluster = new CircuitBreakerGroup({}, item.breakers);
            const shouldBreak = circuitBreakerCluster.tripped(
                item.workflow,
                item.location,
                item.topic,
            );
            assert.strictEqual(shouldBreak, item.shouldBreak);
        });
    });
});
