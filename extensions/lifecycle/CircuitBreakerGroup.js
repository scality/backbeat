'use-strict';
/* eslint no-template-curly-in-string: 0 */

const { CircuitBreaker, BreakerState } = require('breakbeat').CircuitBreaker;
const { startCircuitBreakerMetricsExport } = require('../../lib/CircuitBreaker');

/**
 * circuit breaker configuration adapted for the bucket processor
 * @typedef {Object} BucketProcessorCircuitBreakerConfig
 * @property {Object} global - global circuit breaker configurations (circuit breaks everything)
 * @property {CircuitBreakerGroup} circuitBreakerGroup - circuit breakers per workflow and location/topic
 */

class CircuitBreakerGroup {
    constructor(cbGlobalConf, circuitBreakers) {
        if (circuitBreakers) {
            this.circuitBreakers = circuitBreakers;
            return;
        }

        this._cbGlobalConf = cbGlobalConf;

        // breaker id used for the metrics
        this._latestBreakerId = 0;

        this.circuitBreakers = {
            transition: {
                topic: {},
                location: {},
                global: [],
            },
            expiration: {
                topic: {},
                location: {},
                global: [],
            },
        };
    }

    _append(array, element) {
        if (!array) {
            return [element];
        }
        return [...array, element];
    }

    _addWorkflowCircuitBreaker(circuitBreaker, workflowType, topic, location) {
        if (!topic && !location) {
            this.circuitBreakers[workflowType].global.push(circuitBreaker);
        }
        if (topic) {
            this.circuitBreakers[workflowType].topic[topic] =
                this._append(this.circuitBreakers[workflowType].topic[topic], circuitBreaker);
        }
        if (location) {
            this.circuitBreakers[workflowType].location[location] =
                this._append(this.circuitBreakers[workflowType].location[location], circuitBreaker);
        }
    }

    exportCircuitBreakerMetric(circuitBreaker, metricsSuffix) {
        let metricsName = `lifecycle_bucket_processor_${this._latestBreakerId}`;
        if (metricsSuffix) {
            metricsName += `_${metricsSuffix}`;
        }
        this._latestBreakerId += 1;
        startCircuitBreakerMetricsExport(
            circuitBreaker,
            metricsName
        );
    }

    addCircuitBreaker(probeConf, query, isTransition, isExpiration, topic, location, metricsSuffix) {
        const circuitBreaker = new CircuitBreaker({
            ...this._cbGlobalConf,
            probes: [{
                ...probeConf,
                query,
            }],
        });
        circuitBreaker.start();
        this.exportCircuitBreakerMetric(circuitBreaker, metricsSuffix);
        if (isTransition) {
            this._addWorkflowCircuitBreaker(circuitBreaker, 'transition', topic, location);
        }
        if (isExpiration) {
            this._addWorkflowCircuitBreaker(circuitBreaker, 'expiration', topic, location);
        }
    }

    /**
     * Checks if circuit breakers appropriate to our action are triggered
     * @param {string} worfklowType transition or expiration
     * @param {string} location location name of object
     * @param {string} topic topic we want to push to
     * @returns {bool} true if it should circuit break
     */
    tripped(worfklowType, location, topic) {
        const breakers = [];
        ['expiration', 'transition'].forEach(type => {
            if (worfklowType && type !== worfklowType) {
                return;
            }
            if (location && this.circuitBreakers[type].location[location]) {
                breakers.push(
                    ...this.circuitBreakers[type].location[location],
                );
            }
            if (topic && this.circuitBreakers[type].topic[topic]) {
                breakers.push(
                    ...this.circuitBreakers[type].topic[topic],
                );
            }
            breakers.push(
                ...this.circuitBreakers[type].global,
            );
        });
        return breakers.some(breaker => breaker.state !== BreakerState.Nominal);
    }
}

/**
 * returns a list of relevant topics to the bucket processor
 * @param {Object} lcConfig lifecycle configuration
 * @param {Object} repConfig replication configuration
 * @param {Object} locations  location configuration
 * @returns {string[]} list of topics
 */
function getAllTopics(lcConfig, repConfig, locations) {
    const topics = [
        lcConfig.objectTasksTopic,
        repConfig.dataMoverTopic,
    ];
    Object.keys(locations).forEach(locationName => {
        if (locations[locationName].isCold) {
            topics.push(lcConfig.coldStorageArchiveTopicPrefix + locationName);
        }
    });
    return topics;
}

/**
 * Adapts the templated circuit breaker configuration to the bucket processor
 * Invalid probes are ignored
 * @param {Object} cbConf templated circuit breaker configuration
 * @param {Object} lcConfig lifecycle configuration
 * @param {Object} repConfig replication configuration
 * @param {Object} locations location configuration
 * @param {Logger} logger logger instance
 * @returns {BucketProcessorCircuitBreakerConfig} circuit breaker configuration
 */
function extractBucketProcessorCircuitBreakerConfigs(cbConf, lcConfig, repConfig, locations, logger) {
    if (!cbConf || !cbConf.probes) {
        return {
            global: {},
            circuitBreakerGroup: new CircuitBreakerGroup({}, null),
        };
    }

    const { probes, ...globalCircuitBreakerConf } = cbConf;

    const circuitBreakerGroup = new CircuitBreakerGroup(globalCircuitBreakerConf, null);

    // config off global circuit breaker
    const global = {
        ...globalCircuitBreakerConf,
        probes: [],
    };

    probes.forEach(probe => {
        if (probe.type !== 'prometheusQuery') {
            global.probes.push(probe);
            return;
        }
        // when clause is optional and can be used to specify the filtering on
        // the topic and location, along with specifying if a probe should be
        // used for transition and expiration.
        // Query syntax:
        // "when({topic=topicName,location=locationName,transition=true,expiration=true}) and ANY"
        // Note: should either specify the topic or the location, not both
        // Behaviour:
        // - no when clause + templated topic: creates a circuit breaker for each topic
        // - no when clause + templated location: creates a circuit breaker for each location
        // - when clause with a specific topic + templated topic: creates a circuit breaker for the specified topic
        // - when clause with a specific location + templated location: creates a circuit breaker for the location
        // - when clause with topic and location + topic and location template: creates a circuit breaker for the
        // location and topic
        // - topic specified + location template : creates a circuit breaker for each location and references them in
        // the topic
        // - location specified + topic template : creates a circuit breaker for each topic and references them in
        // the location
        const whenClause = probe.query.match(/^when\s?\(\{(.*?)\}\)\sand\s/);

        const topic = whenClause && whenClause[1].match(/topic="(.*?)"(,|$)/);
        const location = whenClause && whenClause[1].match(/location="(.*?)"(,|$)/);

        const query = whenClause ? probe.query.replace(whenClause[0], '') : probe.query;

        const withTopicTemplate = query.includes('${topic}');
        const withLocationTemplate = query.includes('${location}');

        const transition = whenClause && whenClause[1].match(/transition="(true|false)"(,|$)/);
        const expiration = whenClause && whenClause[1].match(/expiration="(true|false)"(,|$)/);
        // By default all probes are used for both transition and expiration
        const useForTransition = (transition && transition[1] === 'true') || !transition;
        const useForExpiration = (expiration && expiration[1] === 'true') || !expiration;

        if (!useForExpiration && !useForTransition) {
            logger.error('Invalid circuit breaker probe config', {
                reason: 'both lifecycle workflows disabled',
                method: 'extractBucketProcessorCircuitBreakerConfigs',
                query: probe.query,
            });
            return;
        }

        if (withTopicTemplate && withLocationTemplate) {
            logger.error('Invalid circuit breaker probe config', {
                reason: 'using multiple unspecified templates is not supported',
                method: 'extractBucketProcessorCircuitBreakerConfigs',
                query: probe.query,
            });
            return;
        }

        if (!withTopicTemplate && !withLocationTemplate) {
            if (!whenClause) {
                global.probes.push(probe);
                return;
            }
            circuitBreakerGroup.addCircuitBreaker(
                probe,
                query,
                useForTransition,
                useForExpiration,
                topic ? topic[1] : '',
                location ? location[1] : '',
                '',
            );
            return;
        }

        const topics = topic ? [topic[1]] : getAllTopics(lcConfig, repConfig, locations);
        const locationNames = location ? [location[1]] : Object.keys(locations);

        if (withTopicTemplate) {
            topics.forEach(tp => {
                circuitBreakerGroup.addCircuitBreaker(
                    probe,
                    query.replaceAll('${topic}', tp).replaceAll('${location}', location ? location[1] : ''),
                    useForTransition,
                    useForExpiration,
                    tp,
                    location ? location[1] : '',
                    tp,
                );
            });
            return;
        }

        if (withLocationTemplate) {
            locationNames.forEach(loc => {
                circuitBreakerGroup.addCircuitBreaker(
                    probe,
                    query.replaceAll('${location}', loc).replaceAll('${topic}', topic ? topic[1] : ''),
                    useForTransition,
                    useForExpiration,
                    topic ? topic[1] : '',
                    loc,
                    loc,
                );
            });
            return;
        }
    });

    return {
        global,
        circuitBreakerGroup,
    };
}

module.exports = {
    extractBucketProcessorCircuitBreakerConfigs,
    CircuitBreakerGroup,
};
