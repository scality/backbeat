'use strict'; // eslint-disable-line
const assert = require('assert');

const ColdStorageStatusQueueEntry = require('../../../lib/models/ColdStorageStatusQueueEntry');
const LifecycleObjectProcessor = require('./LifecycleObjectProcessor');
const LifecycleUpdateTransitionTask =
      require('../tasks/LifecycleUpdateTransitionTask');
const LifecycleColdStatusArchiveTask =
      require('../tasks/LifecycleColdStatusArchiveTask');
const { LifecycleResetTransitionInProgressTask } =
      require('../tasks/LifecycleResetTransitionInProgressTask');
const { updateCircuitBreakerConfigForImplicitOutputQueue } = require('../../../lib/CircuitBreaker');
const { LifecycleRetriggerRestoreTask } = require('../tasks/LifecycleRetriggerRestoreTask');

class LifecycleObjectTransitionProcessor extends LifecycleObjectProcessor {

    /**
     * Constructor of LifecycleObjectProcessor
     *
     * @constructor
     * @param {Object} zkConfig - zookeeper configuration object
     * @param {String} zkConfig.connectionString - zookeeper connection string
     *  as "host:port[/chroot]"
     * @param {Object} kafkaConfig - kafka configuration object
     * @param {string} kafkaConfig.hosts - list of kafka brokers
     *   as "host:port[,host:port...]"
     * @param {Object} [kafkaConfig.backlogMetrics] - param object to
     * publish kafka topic metrics to zookeeper (see {@link
     * BackbeatConsumer} constructor)
     * @param {Object} lcConfig - lifecycle configuration object
     * @param {String} lcConfig.auth - authentication info
     * @param {String} lcConfig.objectTasksTopic - lifecycle object topic name
     * @param {Object} lcConfig.transitionProcessor - kafka consumer object
     * @param {String} lcConfig.transitionProcessor.groupId - kafka
     * consumer group id
     * @param {Number} [lcConfig.transitionProcessor.concurrency] - number
     *  of max allowed concurrent operations
     * @param {Object} s3Config - S3 configuration
     * @param {Object} s3Config.host - s3 endpoint host
     * @param {Number} s3Config.port - s3 endpoint port
     * @param {String} [transport="http"] - transport method ("http"
     *  or "https")
     */
    constructor(zkConfig, kafkaConfig, lcConfig, s3Config, transport = 'http') {
        super(zkConfig, kafkaConfig, lcConfig, s3Config, transport);
    }

    getProcessorType() {
        return 'transition-processor';
    }

    getConsumerParams() {
        const consumerParams = super.getConsumerParams();
        const locations = require('../../../conf/locationConfig.json') || {};

        this._lcConfig.coldStorageTopics.forEach(topic => {
            if (!topic.startsWith(this._lcConfig.coldStorageStatusTopicPrefix)) {
                return;
            }

            const coldLocation = topic.slice(this._lcConfig.coldStorageStatusTopicPrefix.length);
            assert(locations[coldLocation], `${coldLocation}: unknown location`);
            assert(locations[coldLocation].isCold, `${coldLocation} is not a valid cold storage location`);

            const circuitBreaker = updateCircuitBreakerConfigForImplicitOutputQueue(
                this._lcConfig.objectProcessor.circuitBreaker,
                null,
                topic,
            );

            consumerParams[topic] = {
                zookeeper: {
                    connectionString: this._zkConfig.connectionString,
                },
                kafka: {
                    hosts: this._kafkaConfig.hosts,
                    site: this._kafkaConfig.site,
                    backlogMetrics: this._kafkaConfig.backlogMetrics,
                },
                topic,
                groupId: this._processConfig.groupId,
                concurrency: this._processConfig.concurrency,
                queueProcessor: this.processColdStorageStatusEntry.bind(this),
                circuitBreaker,
            };
        });

        return consumerParams;
    }

    getProcessConfig(lcConfig) {
        return lcConfig.transitionProcessor;
    }

    getAuthConfig(lcConfig) {
        if (lcConfig.transitionProcessor.auth) {
            return lcConfig.transitionProcessor.auth;
        }

        return lcConfig.auth;
    }

    getTask(actionEntry) {
        const actionType = actionEntry.getActionType();

        switch (actionType) {
            case 'requeueTransition':
                return new LifecycleResetTransitionInProgressTask(this);
            case 'requeueRestore':
                return new LifecycleRetriggerRestoreTask(this);
            default:
        }

        if (actionType !== 'copyLocation' ||
            actionEntry.getContextAttribute('ruleType') !== 'transition') {
            this._log.warn(`skipped unsupported  action ${actionType}`,
                             actionEntry.getLogInfo());
            return null;
        }

        return new LifecycleUpdateTransitionTask(this);
    }

    processColdStorageStatusEntry(kafkaEntry, done) {
        const coldLocation = kafkaEntry.topic.slice(this._lcConfig.coldStorageStatusTopicPrefix.length);
        const entry = ColdStorageStatusQueueEntry.createFromKafkaEntry(kafkaEntry);
        if (entry.error) {
            this._log.error('malformed status entry', {
                error: entry.error,
                entry: kafkaEntry.value,
            });
            return process.nextTick(done);
        }
        this._log.debug('processing cold storage entry', entry.getLogInfo());

        let task = null;

        switch (entry.op) {
            case 'archive':
                task = new LifecycleColdStatusArchiveTask(this);
                break;
            default:
                return process.nextTick(done);
        }

        return this.retryWrapper.retry({
            actionDesc: 'process cold storage status entry',
            logFields: entry.getLogInfo(),
            actionFunc: done => task.processEntry(coldLocation, entry, done),
            shouldRetryFunc: err => err.retryable,
            log: this._log,
        }, done);
    }
}

module.exports = LifecycleObjectTransitionProcessor;
