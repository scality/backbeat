'use strict'; // eslint-disable-line

const async = require('async');
const http = require('http');
const https = require('https');
const { EventEmitter } = require('events');
const Logger = require('werelogs').Logger;

const BackbeatConsumer = require('../../../lib/BackbeatConsumer');
const BackbeatMetadataProxy = require('../../../lib/BackbeatMetadataProxy');
const ActionQueueEntry = require('../../../lib/models/ActionQueueEntry');
const GarbageCollectorProducer = require('../../gc/GarbageCollectorProducer');
const CredentialsManager = require('../../../lib/credentials/CredentialsManager');
const { createBackbeatClient, createS3Client } = require('../../../lib/clients/utils');
const { authTypeAssumeRole } = require('../../../lib/constants');
const BackbeatTask = require('../../../lib/tasks/BackbeatTask');

// TODO: test inactive credential deletion
const DELETE_INACTIVE_CREDENTIALS_INTERVAL = 1000 * 60 * 30; // 30m
const MAX_INACTIVE_DURATION = 1000 * 60 * 60 * 2; // 2hr

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
        this._authConfig = this.getAuthConfig(this._lcConfig);
        this._s3Config = s3Config;
        this._transport = transport;
        this._consumer = null;
        this._gcProducer = null;

        // global variables
        if (transport === 'https') {
            this.s3Agent = new https.Agent({ keepAlive: true });
            this.stsAgent = new https.Agent({ keepAlive: true });
        } else {
            this.s3Agent = new http.Agent({ keepAlive: true });
            this.stsAgent = new http.Agent({ keepAlive: true });
        }

        this._stsConfig = null;
        this.s3Clients = {};
        this.backbeatClients = {};
        this.credentialsManager = new CredentialsManager('lifecycle', this._log);
        this.retryWrapper = new BackbeatTask();
    }

    _setupConsumer(cb) {
        let consumerReady = false;
        this._consumer = new BackbeatConsumer({
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
            queueProcessor: this.processKafkaEntry.bind(this),
        });
        this._consumer.on('error', err => {
            if (!consumerReady) {
                this._log.fatal('unable to start lifecycle consumer', {
                    error: err,
                    method: 'LifecycleObjectProcessor._setupConsumer',
                });
                process.exit(1);
            }
        });
        this._consumer.on('ready', () => {
            consumerReady = true;
            this._consumer.subscribe();
            this._log.info(
                'lifecycle object processor successfully started');
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
        this._initSTSConfig();
        this._initCredentialsManager();
        async.parallel([
            done => this._setupConsumer(done),
            done => {
                this._gcProducer = new GarbageCollectorProducer();
                this._gcProducer.setupProducer(done);
            },
        ], done);
    }

    _initSTSConfig() {
        if (this._authConfig.type === authTypeAssumeRole) {
            const { sts } = this._authConfig;
            const stsWithCreds = this.credentialsManager.resolveExternalFileSync(sts);
            this._stsConfig = {
                endpoint: `${this._transport}://${sts.host}:${sts.port}`,
                credentials: {
                    accessKeyId: stsWithCreds.accessKey,
                    secretAccessKey: stsWithCreds.secretKey,
                },
                region: 'us-east-1',
                signatureVersion: 'v4',
                sslEnabled: this._transport === 'https',
                httpOptions: { agent: this.stsAgent, timeout: 0 },
                maxRetries: 0,
            };
        }
    }

    _initCredentialsManager() {
        this.credentialsManager.on('deleteCredentials', clientId => {
            delete this.s3Clients[clientId];
            delete this.backbeatClients[clientId];
        });

        this._deleteInactiveCredentialsInterval = setInterval(() => {
            this.credentialsManager.removeInactiveCredentials(MAX_INACTIVE_DURATION);
        }, DELETE_INACTIVE_CREDENTIALS_INTERVAL);
    }

    /**
     * Return an S3 client instance
     * @param {String} canonicalId - The canonical ID of the bucket owner.
     * @param {String} accountId - The account ID of the bucket owner .
     * @return {AWS.S3} The S3 client instance to make requests with
     */
    _getS3Client(canonicalId, accountId) {
        const credentials = this.credentialsManager.getCredentials({
            id: canonicalId,
            accountId,
            stsConfig: this._stsConfig,
            authConfig: this._authConfig,
        });

        if (credentials === null) {
            return null;
        }

        const clientId = canonicalId;
        const client = this.s3Clients[clientId];

        if (client) {
            return client;
        }

        this.s3Clients[clientId] = createS3Client({
            transport: this._transport,
            port: this._s3Config.port,
            host: this._s3Config.host,
            credentials,
            agent: this.s3Agent,
        });

        return this.s3Clients[clientId];
    }

    /**
     * Return an backbeat client instance
     * @param {String} canonicalId - The canonical ID of the bucket owner.
     * @param {String} accountId - The account ID of the bucket owner .
     * @return {BackbeatClient} The S3 client instance to make requests with
     */
    _getBackbeatClient(canonicalId, accountId) {
        console.log('this._stsConfig!!!', this._stsConfig);
        console.log('this._authConfig!!!', this._authConfig);
        const credentials = this.credentialsManager.getCredentials({
            id: canonicalId,
            accountId,
            stsConfig: this._stsConfig,
            authConfig: this._authConfig,
        });

        console.log('credentials!!!', credentials);

        if (credentials === null) {
            return null;
        }

        const clientId = canonicalId;
        const client = this.backbeatClients[clientId];

        if (client) {
            return new BackbeatMetadataProxy(
            `${this._transport}://${this._s3Config.host}:${this._s3Config.port}`, this._authConfig)
            .setBackbeatClient(client);
        }

        this.backbeatClients[clientId] = createBackbeatClient({
            transport: this._transport,
            port: this._s3Config.port,
            host: this._s3Config.host,
            credentials,
            agent: this.s3Agent,
        });

        return new BackbeatMetadataProxy(
            `${this._transport}://${this._s3Config.host}:${this._s3Config.port}`, this._authConfig)
            .setBackbeatClient(this.backbeatClients[clientId]);
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
    processKafkaEntry(kafkaEntry, done) {
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
            authConfig: this._authConfig,
            getS3Client: this._getS3Client.bind(this),
            getBackbeatClient: this._getBackbeatClient.bind(this),
            gcProducer: this._gcProducer,
            logger: this._log,
        };
    }

    isReady() {
        return this._consumer && this._consumer.isReady();
    }
}

module.exports = LifecycleObjectProcessor;
