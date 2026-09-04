'use strict';
const assert = require('assert');
const async = require('async');
const { errors } = require('arsenal');

const ColdStorageStatusQueueEntry = require('../../../lib/models/ColdStorageStatusQueueEntry');
const { LifecycleMetrics } = require('../LifecycleMetrics');
const LifecycleObjectProcessor = require('./LifecycleObjectProcessor');
const LifecycleUpdateExpirationTask = require('../tasks/LifecycleUpdateExpirationTask');
const LifecycleUpdateTransitionTask = require('../tasks/LifecycleUpdateTransitionTask');
const LifecycleColdStatusArchiveTask = require('../tasks/LifecycleColdStatusArchiveTask');
const { LifecycleResetTransitionInProgressTask } =
      require('../tasks/LifecycleResetTransitionInProgressTask');
const { updateCircuitBreakerConfigForImplicitOutputQueue } = require('../../../lib/CircuitBreaker');
const { LifecycleRetriggerRestoreTask } = require('../tasks/LifecycleRetriggerRestoreTask');
const BackbeatProducer = require('../../../lib/BackbeatProducer');
const GarbageCollectorProducer = require('../../gc/GarbageCollectorProducer');
const VaultClientWrapper = require('../../utils/VaultClientWrapper');
const { AccountIdCache } = require('../../utils/AccountIdCache');
const { authTypeAssumeRole } = require('../../../lib/constants');

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
     * @param {String} lcConfig.transitionTasksTopic - lifecycle transition topic name
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
     * @param {Object} [vaultAdminConfig] - vault admin endpoint, used to
     *  resolve canonical ids into account ids
     */
    constructor(zkConfig, kafkaConfig, lcConfig, s3Config, transport = 'http',
        vaultAdminConfig = undefined) {
        super(zkConfig, kafkaConfig, lcConfig, s3Config, transport);

        const authConfig = this.getAuthConfig(this._lcConfig);
        if (authConfig.type === authTypeAssumeRole) {
            this.vaultClientWrapper = new VaultClientWrapper(
                `lifecycle:${this.getProcessorType()}`,
                vaultAdminConfig,
                authConfig,
                this._log,
            );
            this._accountIdCache = new AccountIdCache(
                this._processConfig.concurrency);
        }
    }

    /**
     * Resolve the account id of a canonical id. Actions published by the
     * lifecycle conductor already carry the account id; those published by the
     * queue populator (pull replication) only know the canonical id.
     * @param {String} ownerId - canonical id of the object owner
     * @param {Logger} log - logger instance
     * @param {Function} cb - callback: cb(err, accountId)
     * @return {undefined}
     */
    getAccountId(ownerId, log, cb) {
        if (!this.vaultClientWrapper) {
            log.debug('skipping: not assume role auth type');
            return process.nextTick(cb);
        }

        // A cached miss must fail like a fresh lookup would: `isKnown()` is also
        // true for misses, and `get()` would then hand back `undefined`.
        if (this._accountIdCache.isMiss(ownerId)) {
            log.error('canonical id does not exist (cached)', { ownerId });
            return process.nextTick(cb, errors.NoSuchEntity);
        }

        if (this._accountIdCache.has(ownerId)) {
            return process.nextTick(cb, null, this._accountIdCache.get(ownerId));
        }

        return this.vaultClientWrapper.getAccountId(ownerId, (err, accountId) => {
            if (err) {
                if (err.NoSuchEntity) {
                    log.error('canonical id does not exist', { error: err, ownerId });
                    this._accountIdCache.miss(ownerId);
                } else {
                    log.error('could not get account id', { error: err, ownerId });
                }
                return cb(err);
            }

            this._accountIdCache.set(ownerId, accountId);
            this._accountIdCache.expireOldest();

            return cb(null, accountId);
        });
    }

    /**
     * Start kafka consumer. Emits a 'ready' event when
     * consumer is ready.
     * @param {function} done - callback
     * @return {undefined}
     */
    start(done) {
        this.vaultClientWrapper?.init();
        async.waterfall([
            next => super.start(next),
            next => {
                this._gcProducer = new GarbageCollectorProducer();
                this._gcProducer.setupProducer(next);
            },
            next => this.setupProducer(next),
        ], done);
    }

    /**
     * Set up Kafka producer
     * @param {function} cb callback called when producer
     * startup is complete
     * @return {undefined}
     */
    setupProducer(cb) {
        const producer = new BackbeatProducer({
            kafka: { hosts: this._kafkaConfig.hosts },
            maxRequestSize: this._kafkaConfig.maxRequestSize,
            compressionType: this._kafkaConfig.compressionType,
            requiredAcks: this._kafkaConfig.requiredAcks,
            producerParams: this._kafkaConfig.producerParams,
        });
        producer.once('error', cb);
        producer.once('ready', () => {
            producer.removeAllListeners('error');
            producer.on('error', err =>
                this._log.error('error from backbeat producer', {
                    method: 'LifecycleObjectTransitionProcessor.setupProducer',
                    error: err,
                }));
            this._coldProducer = producer;
            return cb();
        });
    }

    getProcessorType() {
        return 'transition-processor';
    }

    getConsumerParams() {
        const consumerParams = super.getConsumerParams(this._lcConfig.transitionTasksTopic);

        consumerParams[this._lcConfig.transitionTasksTopic].fromOffset = 'earliest';

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
                    compressionType: this._kafkaConfig.compressionType,
                    requiredAcks: this._kafkaConfig.requiredAcks,
                },
                topic,
                groupId: this._processConfig.groupId,
                concurrency: this._processConfig.concurrency,
                maxQueued: this._processConfig.maxQueued,
                queueProcessor: this.processColdStorageStatusEntry.bind(this),
                circuitBreaker,
                fromOffset: 'earliest',
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
            case 'gc':
                return new LifecycleUpdateExpirationTask(this);
            case 'copyLocation':
                if (actionEntry.getContextAttribute('ruleType') === 'transition') {
                    return new LifecycleUpdateTransitionTask(this);
                }
                // fall through
            default:
                this._log.warn(`skipped unsupported  action ${actionType}`,
                            actionEntry.getLogInfo());
                return null;
        }
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
        }, err => {
            if (err) {
                this._log.error('task failed permanently after retries, committing offset', {
                    method: 'LifecycleObjectTransitionProcessor.processColdStorageStatusEntry',
                    error: err.message,
                    op: entry.op,
                    coldLocation,
                    ...entry.getLogInfo(),
                });
                LifecycleMetrics.onLifecycleFailed(this._log, this.getProcessorType(),
                    entry.op, coldLocation);
            }
            return done(err);
        });
    }

    getStateVars() {
        return {
            ...super.getStateVars(),
            coldProducer: this._coldProducer,
            gcProducer: this._gcProducer,
            getAccountId: this.getAccountId.bind(this),
        };
    }

    isReady() {
        return super.isReady() && (!this.vaultClientWrapper || this.vaultClientWrapper.tempCredentialsReady());
    }
}

module.exports = LifecycleObjectTransitionProcessor;
