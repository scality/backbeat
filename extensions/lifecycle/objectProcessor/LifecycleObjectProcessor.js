'use strict'; // eslint-disable-line

const async = require('async');
const { EventEmitter } = require('events');
const Logger = require('werelogs').Logger;

const BackbeatConsumerManager = require('../../../lib/BackbeatConsumerManager');
const ActionQueueEntry = require('../../../lib/models/ActionQueueEntry');
const GarbageCollectorProducer = require('../../gc/GarbageCollectorProducer');
const ClientManager = require('../../../lib/clients/ClientManager');
const BackbeatTask = require('../../../lib/tasks/BackbeatTask');

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
     * consumer group id
     *  of max allowed concurrent operations
     * @param {Object} s3Config - S3 configuration
     * @param {Object} s3Config.host - s3 endpoint host
     * @param {Number} s3Config.port - s3 endpoint port
     * @param {String} [transport="http"] - transport method ("http"
     *  or "https")
     */
    constructor(zkConfig, kafkaConfig, lcConfig, s3Config, transport = 'http') {
        super();
        this._log = new Logger('Backbeat:Lifecycle:ObjectProcessor');
        this._zkConfig = zkConfig;
        this._kafkaConfig = kafkaConfig;
        this._lcConfig = lcConfig;
        this._processConfig = this.getProcessConfig(this._lcConfig);
        this._consumers = null;
        this._gcProducer = null;

        this.clientManager = new ClientManager({
            id: this.getId(),
            authConfig: this.getAuthConfig(this._lcConfig),
            s3Config,
            transport,
        }, this._log);

        this.retryWrapper = new BackbeatTask();
    }

    getId() {
        return 'Backbeat:Lifecycle:ObjectProcessor';
    }

    _getObjecTaskConsumerParams() {
        return {
            zookeeper: {
                connectionString: this._zkConfig.connectionString,
            },
            kafka: {
                hosts: this._kafkaConfig.hosts,
                site: this._kafkaConfig.site,
                backlogMetrics: this._kafkaConfig.backlogMetrics,
            },
            topic: this._lcConfig.objectTasksTopic,
            groupId: this._processConfig.groupId,
            concurrency: this._processConfig.concurrency,
            queueProcessor: this.processObjectTaskEntry.bind(this),
        };
    }

    getConsumerParams() {
        return {
            [this._lcConfig.objectTasksTopic]: this._getObjecTaskConsumerParams(),
        };
    }

    _setupConsumers(cb) {
        this._consumers = new BackbeatConsumerManager(
            this.getId(),
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
        async.parallel([
            done => this._setupConsumers(done),
            done => {
                this._gcProducer = new GarbageCollectorProducer();
                this._gcProducer.setupProducer(done);
            },
        ], done);
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

        this._consumer.close(cb);
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
            this._log.error('malformed action entry', kafkaEntry.value);
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
            gcProducer: this._gcProducer,
            logger: this._log,
        };
    }

    isReady() {
        return this._consumers && this._consumers.isReady();
    }
}

module.exports = LifecycleObjectProcessor;
