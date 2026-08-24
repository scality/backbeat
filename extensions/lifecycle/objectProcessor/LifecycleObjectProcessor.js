'use strict';

const { EventEmitter } = require('events');
const Logger = require('werelogs').Logger;
const { errors } = require('arsenal');

const BackbeatConsumerManager = require('../../../lib/BackbeatConsumerManager');
const ActionQueueEntry = require('../../../lib/models/ActionQueueEntry');
const ClientManager = require('../../../lib/clients/ClientManager');
const BackbeatTask = require('../../../lib/tasks/BackbeatTask');
const VaultClientWrapper = require('../../utils/VaultClientWrapper');
const { AccountIdCache } = require('../../utils/AccountIdCache');
const { authTypeAssumeRole } = require('../../../lib/constants');

const logIdFromType = {
    'object-processor': 'Backbeat:Lifecycle:ObjectProcessor',
    'transition-processor': 'Backbeat:Lifecycle:ObjectTransitionProcessor',
    'expiration-processor': 'Backbeat:Lifecycle:ObjectExpirationProcessor',
};

/**
 * @class LifecycleObjectProcessor
 *
 * @classdesc Handles consuming entries from the object tasks topic
 * and executing the expiration actions on the local CloudServer
 * endpoint using the S3 API.
 */
class LifecycleObjectProcessor extends EventEmitter {
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
     * @param {Object} s3Config - S3 configuration
     * @param {Object} s3Config.host - s3 endpoint host
     * @param {Number} s3Config.port - s3 endpoint port
     * @param {String} [transport="http"] - transport method ("http"
     *  or "https")
     */
    constructor(zkConfig, kafkaConfig, lcConfig, s3Config, transport = 'http') {
        super();
        this._log = new Logger(logIdFromType[this.getProcessorType()]);
        this._zkConfig = zkConfig;
        this._kafkaConfig = kafkaConfig;
        this._lcConfig = lcConfig;
        this._processConfig = this.getProcessConfig(this._lcConfig);
        this._consumers = null;

        this.clientManager = new ClientManager({
            id: this.getProcessorType(),
            authConfig: this.getAuthConfig(this._lcConfig),
            s3Config,
            transport,
        }, this._log);

        this.vaultClientWrapper = new VaultClientWrapper(
            `lifecycle:${this.getProcessorType()}`,
            this._processConfig.vaultAdmin,
            this.getAuthConfig(this._lcConfig),
            this._log,
        );
        this._accountIdCache = new AccountIdCache(
            this._processConfig.concurrency);

        this.retryWrapper = new BackbeatTask(this._processConfig.retry);
    }

    /**
     * Resolve the account id of a canonical id. Actions published by the
     * lifecycle conductor already carry the account id; those published by the
     * queue populator (clean room localization) only know the canonical id.
     * @param {String} ownerId - canonical id of the object owner
     * @param {Logger} log - logger instance
     * @param {Function} cb - callback: cb(err, accountId)
     * @return {undefined}
     */
    getAccountId(ownerId, log, cb) {
        if (this.getAuthConfig(this._lcConfig).type !== authTypeAssumeRole) {
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

    getProcessorType() {
        return 'object-processor';
    }

    _getTopicConsumerParams(topic) {
        return {
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
            queueProcessor: this.processObjectTaskEntry.bind(this),
            circuitBreaker: this._lcConfig.objectProcessor.circuitBreaker,
            circuitBreakerMetrics: {
                type: 'lifecycle_object_processor',
            },
        };
    }

    getConsumerParams(topic = this._lcConfig.objectTasksTopic) {
        return {
            [topic]: this._getTopicConsumerParams(topic),
        };
    }

    _setupConsumers(cb) {
        this._consumers = new BackbeatConsumerManager(
            this.getProcessorType(),
            this.getConsumerParams(),
            this._log
        );

        this._consumers.setupConsumers(err => {
            if (err) {
                 this._log.fatal('unable to start lifecycle consumers', {
                     error: err,
                     method: 'LifecycleObjectProcessor._setupConsumer',
                 });

                process.exit(1);
            }

            this._log.info('lifecycle object processor successfully started');
            this.emit('ready');
            cb();
        });
    }

    /**
     * Start kafka consumer. Emits a 'ready' event when
     * consumer is ready.
     * @param {function} done - callback
     * @return {undefined}
     */
    start(done) {
        this.clientManager.initSTSConfig();
        this.clientManager.initCredentialsManager();
        if (this.getAuthConfig(this._lcConfig).type === authTypeAssumeRole) {
            this.vaultClientWrapper.init();
        }
        this._setupConsumers(done);
    }

    /**
     * Close the lifecycle consumer
     * @param {function} cb - callback function
     * @return {undefined}
     */
    close(cb) {
        this._log.debug('closing object tasks consumer');

        if (this._deleteInactiveCredentialsInterval) {
            clearInterval(this._deleteInactiveCredentialsInterval);
        }

        if (this._consumers) {
            this._consumers.close(cb);
        } else {
            cb();
        }
    }

    /**
     * Retrieve object processor config
     * @return {object} - process config
     */
    getProcessConfig() {
        throw new Error('LifecycleObjectProcessor.getProcessConfig not implemented');
    }

    /**
     * Retrieve process auth config
     * @return {object} - auth config
     */
    getAuthConfig() {
        throw new Error('LifecycleObjectProcessor.getAuthConfig not implemented');
    }

    /**
     * Retrieve object processor task action
     * @param {ActionQueueEntry} actionEntry - lifecycle action entry
     * @return {BackbeatTask|null} - backbeat task object
     */
    // eslint-disable-next-line
    getTask(actionEntry) {
        return null;
    }

    /**
     * Proceed to the lifecycle action of an object given a kafka
     * object lifecycle queue entry
     *
     * @param {object} kafkaEntry - entry generated by the queue populator
     * @param {function} done - callback function
     * @return {undefined}
     */
    processObjectTaskEntry(kafkaEntry, done) {
        this._log.debug('processing kafka entry');

        const actionEntry = ActionQueueEntry.createFromKafkaEntry(kafkaEntry);
        if (actionEntry.error) {
            this._log.error('malformed action entry', {
                error: actionEntry.error,
                entry: kafkaEntry.value,
            });
            return process.nextTick(done);
        }
        this._log.debug('processing lifecycle object entry',
                          actionEntry.getLogInfo());
        const task = this.getTask(actionEntry);

        if (task === null) {
            return process.nextTick(done);
        }

        return this.retryWrapper.retry({
            actionDesc: 'process lifecycle object entry',
            logFields: actionEntry.getLogInfo(),
            actionFunc: done => task.processActionEntry(actionEntry, done),
            shouldRetryFunc: err => err.retryable,
            log: this._log,
        }, done);
    }

    getStateVars() {
        return {
            s3Config: this._s3Config,
            lcConfig: this._lcConfig,
            processConfig: this._processConfig,
            getS3Client:
                this.clientManager.getS3Client.bind(this.clientManager),
            getBackbeatClient:
                this.clientManager.getBackbeatClient.bind(this.clientManager),
            getBackbeatMetadataProxy:
                this.clientManager.getBackbeatMetadataProxy.bind(this.clientManager),
            getAccountId: this.getAccountId.bind(this),
            logger: this._log,
        };
    }

    isReady() {
        return this._consumers && this._consumers.isReady() &&
            this.vaultClientWrapper.tempCredentialsReady();
    }
}

module.exports = LifecycleObjectProcessor;
