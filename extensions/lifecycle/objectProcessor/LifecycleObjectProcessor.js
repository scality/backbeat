'use strict'; // eslint-disable-line

const async = require('async');
const http = require('http');
const AWS = require('aws-sdk');
const { EventEmitter } = require('events');
const Logger = require('werelogs').Logger;

const LifecycleDeleteObjectTask =
      require('../tasks/LifecycleDeleteObjectTask');
const LifecycleUpdateTransitionTask =
      require('../tasks/LifecycleUpdateTransitionTask');
const BackbeatConsumer = require('../../../lib/BackbeatConsumer');
const BackbeatMetadataProxy = require('../../../lib/BackbeatMetadataProxy');
const { getAccountCredentials } =
      require('../../../lib/credentials/AccountCredentials');
const ActionQueueEntry = require('../../../lib/models/ActionQueueEntry');
const GarbageCollectorProducer = require('../../gc/GarbageCollectorProducer');

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
     * @param {Object} lcConfig - lifecycle configuration object
     * @param {String} lcConfig.auth - authentication info
     * @param {String} lcConfig.objectTasksTopic - lifecycle object topic name
     * @param {Object} lcConfig.objectProcessor - kafka consumer object
     * @param {String} lcConfig.objectProcessor.groupId - kafka
     * consumer group id
     * @param {Number} [lcConfig.objectProcessor.concurrency] - number
     *  of max allowed concurrent operations
     * @param {Object} [lcConfig.backlogMetrics] - param object to
     * publish backlog metrics to zookeeper (see {@link
     * BackbeatConsumer} constructor)
     * @param {Object} s3Config - S3 configuration
     * @param {Object} s3Config.host - s3 endpoint host
     * @param {Number} s3Config.port - s3 endpoint port
     * @param {String} [transport="http"] - transport method ("http"
     *  or "https")
     */
    constructor(zkConfig, kafkaConfig, lcConfig, s3Config,
                transport = 'http') {
        super();
        this.zkConfig = zkConfig;
        this.kafkaConfig = kafkaConfig;
        this.lcConfig = lcConfig;
        this.authConfig = lcConfig.auth;
        this.s3Config = s3Config;
        this._transport = transport;
        this._consumer = null;
        this._gcProducer = null;

        this.logger = new Logger('Backbeat:Lifecycle:ObjectProcessor');

        // global variables
        // TODO: for SSL support, create HTTPS agents instead
        this.httpAgent = new http.Agent({ keepAlive: true });
    }


    /**
     * Start kafka consumer. Emits a 'ready' event when
     * consumer is ready.
     *
     * @return {undefined}
     */
    start() {
        this._setupClients();
        async.parallel([
            done => {
                let consumerReady = false;
                this._consumer = new BackbeatConsumer({
                    zookeeper: {
                        connectionString: this.zkConfig.connectionString,
                    },
                    kafka: { hosts: this.kafkaConfig.hosts },
                    topic: this.lcConfig.objectTasksTopic,
                    groupId: this.lcConfig.objectProcessor.groupId,
                    concurrency: this.lcConfig.objectProcessor.concurrency,
                    queueProcessor: this.processKafkaEntry.bind(this),
                    backlogMetrics: this.lcConfig.backlogMetrics,
                });
                this._consumer.on('error', () => {
                    if (!consumerReady) {
                        this.logger.fatal(
                            'error starting lifecycle object processor');
                        process.exit(1);
                    }
                });
                this._consumer.on('ready', () => {
                    consumerReady = true;
                    this._consumer.subscribe();
                    this.logger.info(
                        'lifecycle object processor successfully started');
                    this.emit('ready');
                    done();
                });
            },
            done => {
                this._gcProducer = new GarbageCollectorProducer();
                this._gcProducer.setupProducer(done);
            },
        ], () => {});
    }

    _getCredentials() {
        const credentials = getAccountCredentials(
            this.lcConfig.auth, this.logger);
        if (credentials) {
            return credentials;
        }
        this.logger.fatal('error during lifecycle object processor startup: ' +
                          `invalid auth type ${this.lcConfig.auth.type}`);
        return process.exit(1);
    }

    _setupClients() {
        const accountCreds = this._getCredentials();
        const s3 = this.s3Config;
        const transport = this._transport;
        this.logger.debug('creating s3 client', { transport, s3 });
        this.s3Client = new AWS.S3({
            endpoint: `${transport}://${s3.host}:${s3.port}`,
            credentials: accountCreds,
            sslEnabled: transport === 'https',
            s3ForcePathStyle: true,
            signatureVersion: 'v4',
            httpOptions: { agent: this.httpAgent, timeout: 0 },
            maxRetries: 0,
        });
        this.backbeatClient = new BackbeatMetadataProxy(
            `${transport}://${s3.host}:${s3.port}`,
            this.lcConfig.auth, this.httpAgent);
        this.backbeatClient.setSourceClient(this.logger);
    }

    /**
     * Close the lifecycle consumer
     * @param {function} cb - callback function
     * @return {undefined}
     */
    close(cb) {
        this.logger.debug('closing object tasks consumer');
        this._consumer.close(cb);
    }

    /**
     * Proceed to the lifecycle action of an object given a kafka
     * object lifecycle queue entry
     *
     * @param {object} kafkaEntry - entry generated by the queue populator
     * @param {function} done - callback function
     * @return {undefined}
     */
    processKafkaEntry(kafkaEntry, done) {
        this.logger.debug('processing kafka entry');

        const actionEntry = ActionQueueEntry.createFromKafkaEntry(kafkaEntry);
        if (actionEntry.error) {
            this.logger.error('malformed action entry', kafkaEntry.value);
            return process.nextTick(done);
        }
        this.logger.debug('processing lifecycle object entry',
                          actionEntry.getLogInfo());
        const actionType = actionEntry.getActionType();
        let task;
        if (actionType === 'deleteObject' ||
            actionType === 'deleteMPU') {
            task = new LifecycleDeleteObjectTask(this);
        } else if (actionType === 'copyLocation' &&
                   actionEntry.getContextAttribute('ruleType')
                   === 'transition') {
            task = new LifecycleUpdateTransitionTask(this);
        } else {
            this.logger.warn(`skipped unsupported action ${actionType}`,
                             actionEntry.getLogInfo());
            return process.nextTick(done);
        }
        return task.processActionEntry(actionEntry, done);
    }

    getStateVars() {
        return {
            s3Config: this.s3Config,
            lcConfig: this.lcConfig,
            authConfig: this.authConfig,
            s3Client: this.s3Client,
            backbeatClient: this.backbeatClient,
            gcProducer: this._gcProducer,
            logger: this.logger,
        };
    }

    isReady() {
        return this._consumer && this._consumer.isReady();
    }

}

module.exports = LifecycleObjectProcessor;
