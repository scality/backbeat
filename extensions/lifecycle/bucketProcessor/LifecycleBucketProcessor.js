'use strict'; // eslint-disable-line

const async = require('async');
const http = require('http');
const https = require('https');
const { Logger } = require('werelogs');
const { errors } = require('arsenal');

const BackbeatProducer = require('../../../lib/BackbeatProducer');
const BackbeatConsumer = require('../../../lib/BackbeatConsumer');
const KafkaBacklogMetrics = require('../../../lib/KafkaBacklogMetrics');
const BackbeatMetadataProxy = require('../../../lib/BackbeatMetadataProxy');
const LifecycleTask = require('../tasks/LifecycleTask');
const CredentialsManager = require('../../../lib/credentials/CredentialsManager');
const { createBackbeatClient, createS3Client } = require('../../../lib/clients/utils');
const safeJsonParse = require('../util/safeJsonParse');
const { authTypeAssumeRole } = require('../../../lib/constants');

const PROCESS_OBJECTS_ACTION = 'processObjects';

// TODO: test inactive credential deletion
const DELETE_INACTIVE_CREDENTIALS_INTERVAL = 1000 * 60 * 30; // 30m
const MAX_INACTIVE_DURATION = 1000 * 60 * 60 * 2; // 2hr

/**
 * @class LifecycleBucketProcessor
 *
 * @classdesc Handles consuming entries from the bucket tasks topic
 * and producing lifecycle messages to trigger actions for objects
 * that qualify to lifecycle rules.
 *
 * If another listing is required on the bucket (i.e., the listing is
 * truncated), an entry is pushed to the bucket topic for the next
 * round of processing.
 */
class LifecycleBucketProcessor {
    /**
     * Constructor of LifecycleBucketProcessor
     *
     * @constructor
     * @param {Object} zkConfig - zookeeper config
     * @param {Object} kafkaConfig - kafka configuration object
     * @param {string} kafkaConfig.hosts - list of kafka brokers
     *   as "host:port[,host:port...]"
     * @param {Object} [kafkaConfig.backlogMetrics] - param object to
     * publish kafka topic metrics to zookeeper (see {@link
     * BackbeatConsumer} constructor)
     * @param {Object} lcConfig - lifecycle config
     * @param {Object} lcConfig.lifecycle.auth - authentication info
     * @param {String} lcConfig.lifecycle.bucketTasksTopic - lifecycle bucket
     * topic name
     * @param {Object} lcConfig.lifecycle.bucketProcessor - kafka consumer
     * object
     * @param {String} lcConfig.lifecycle.bucketProcessor.groupId - kafka
     * consumer group id
     * @param {Object} repConfig - replication config
     * @param {String} repConfig.topic - kafka replication topic
     * @param {Object} repConfig.source - replication source
     * @param {Number} [lcConfig.bucketProcessor.concurrency] - number
     *  of max allowed concurrent operations
     * @param {Object} s3Config - s3 config
     * @param {String} s3Config.host - host ip
     * @param {String} s3Config.port - port
     * @param {String} transport - http or https
     */
    constructor(zkConfig, kafkaConfig, lcConfig, repConfig, s3Config, transport = 'http') {
        this._log = new Logger('Backbeat:Lifecycle:BucketProcessor');
        this._zkConfig = zkConfig;
        this._kafkaConfig = kafkaConfig;
        this._lcConfig = lcConfig;
        this._repConfig = repConfig;
        this._s3Endpoint = `${transport}://${s3Config.host}:${s3Config.port}`;
        this._s3Config = s3Config;
        this._transport = transport;
        this._producer = null;
        this._kafkaBacklogMetrics = null;

        this._producerReady = false;
        this._consumerReady = false;

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

        // The task scheduler for processing lifecycle tasks concurrently.
        this._internalTaskScheduler = async.queue((ctx, cb) => {
            const { task, rules, value, s3target, backbeatMetadataProxy } = ctx;
            return task.processBucketEntry(
                rules, value, s3target, backbeatMetadataProxy, cb);
        }, this._lcConfig.bucketProcessor.concurrency);

        // Listen for errors from any task being processed.
        this._internalTaskScheduler.drain(err => {
            if (err) {
                this._log.error('error occurred during task processing', {
                    error: err,
                });
            }
        });
    }

    /**
     * Get the state variables of the current instance.
     * @return {Object} Object containing the state variables
     */
    getStateVars() {
        return {
            producer: this._producer,
            bootstrapList: this._repConfig.destination.bootstrapList,
            enabledRules: this._lcConfig.rules,
            s3Endpoint: this._s3Endpoint,
            s3Auth: this._lcConfig.auth,
            bucketTasksTopic: this._lcConfig.bucketTasksTopic,
            objectTasksTopic: this._lcConfig.objectTasksTopic,
            kafkaBacklogMetrics: this._kafkaBacklogMetrics,
            log: this._log,
        };
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
            authConfig: this._lcConfig.auth,
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
        const credentials = this.credentialsManager.getCredentials({
            id: canonicalId,
            accountId,
            stsConfig: this._stsConfig,
            authConfig: this._lcConfig.auth,
        });

        if (credentials === null) {
            return null;
        }

        const clientId = canonicalId;
        const client = this.backbeatClients[clientId];

        if (client) {
            return client;
        }

        this.backbeatClients[clientId] = createBackbeatClient({
            transport: this._transport,
            port: this._s3Config.port,
            host: this._s3Config.host,
            credentials,
            agent: this.s3Agent,
        });

        return new BackbeatMetadataProxy(this._lcConfig)
            .setBackbeatClient(this.backbeatClients[clientId]);
    }

    /**
     * Determine whether the given config should be processed.
     * @param {Object} config - The bucket lifecycle configuration
     * @return {Boolean} Whether the config should be processed
     */
    _shouldProcessConfig(config) {
        if (config.Rules.length === 0) {
            this._log.debug('bucket lifecycle config has no rules to process', {
                config,
            });
            return false;
        }
        const { rules } = this._lcConfig;
        // Check if backbeat config has a lifecycle rule enabled for processing.
        const enabled = Object.keys(rules).some(rule => rules[rule].enabled);
        if (!enabled) {
            this._log.debug('no lifecycle rules enabled in backbeat config');
        }
        return enabled;
    }

    /**
     * Process the given bucket entry, get the bucket's lifecycle configuration,
     * and schedule a task with the lifecycle configuration rules, if
     * applicable.
     * @param {Object} entry - The kafka entry containing the information for
     * performing a listing of the bucket's objects
     * @param {Object} entry.value - The value of the entry object
     * (see format of messages in lifecycle topic for bucket tasks)
     * @param {Function} cb - The callback to call
     * @return {undefined}
     */
    _processBucketEntry(entry, cb) {
        const { error, result } = safeJsonParse(entry.value);
        if (error) {
            this._log.error('could not parse bucket entry',
                            { value: entry.value, error });
            return process.nextTick(() => cb(error));
        }
        if (result.action !== PROCESS_OBJECTS_ACTION) {
            return process.nextTick(cb);
        }
        if (typeof result.target !== 'object') {
            this._log.error('malformed kafka bucket entry', {
                method: 'LifecycleBucketProcessor._processBucketEntry',
                entry: result,
            });
            return process.nextTick(() => cb(errors.InternalError));
        }
        const { bucket, owner, accountId } = result.target;
        if (!bucket || !owner || (!accountId && this._lcConfig.auth.type === authTypeAssumeRole)) {
            this._log.error('kafka bucket entry missing required fields', {
                method: 'LifecycleBucketProcessor._processBucketEntry',
                bucket,
                owner,
                accountId,
            });
            return process.nextTick(() => cb(errors.InternalError));
        }
        this._log.debug('processing bucket entry', {
            method: 'LifecycleBucketProcessor._processBucketEntry',
            bucket,
            owner,
            accountId,
        });

        const s3 = this._getS3Client(owner, accountId);
        if (!s3) {
            return cb(errors.InternalError
                .customizeDescription('failed to obtain a s3 client'));
        }

        const backbeatMetadataProxy = this._getBackbeatClient(owner, accountId);
        if (!backbeatMetadataProxy) {
            return cb(errors.InternalError
                .customizeDescription('failed to obtain a backbeat client'));
        }

        const params = { Bucket: bucket };
        return s3.getBucketLifecycleConfiguration(params, (err, config) => {
            if (err) {
                this._log.error('error getting bucket lifecycle config', {
                    method: 'LifecycleBucketProcessor._processBucketEntry',
                    bucket,
                    owner,
                    error: err,
                });
                return cb(err);
            }
            if (!this._shouldProcessConfig(config)) {
                return cb();
            }
            this._log.info('scheduling new task for bucket lifecycle', {
                method: 'LifecycleBucketProcessor._processBucketEntry',
                bucket,
                owner,
                details: result.details,
            });
            return this._internalTaskScheduler.push({
                task: new LifecycleTask(this),
                rules: config.Rules,
                value: result,
                s3target: s3,
                backbeatMetadataProxy,
            }, cb);
        });
    }

    /**
     * Set up the backbeat producer with the given topic.
     * @param {Function} cb - The callback to call
     * @return {undefined}
     */
    _setupProducer(cb) {
        const producer = new BackbeatProducer({
            kafka: { hosts: this._kafkaConfig.hosts },
            topic: this._lcConfig.objectTasksTopic,
        });
        producer.once('error', err => {
            this._log.error('error setting up kafka producer', {
                error: err,
                method: 'LifecycleBucketProcesso::_setupProducer',
            });
            process.exit(1);
        });
        producer.once('ready', () => {
            this._log.debug('producer is ready',
                { kafkaConfig: this.kafkaConfig });
            producer.removeAllListeners('error');
            producer.on('error', err => {
                this._log.error('error from backbeat producer', {
                    error: err,
                });
            });
            this._producerReady = true;
            this._producer = producer;
            return cb();
        });
    }

    /**
     * Set up the lifecycle consumer.
     * @param {function} cb - callback
     * @return {undefined}
     */
    _setupConsumer(cb) {
        this._consumer = new BackbeatConsumer({
            zookeeper: {
                connectionString: this._zkConfig.connectionString,
            },
            kafka: {
                hosts: this._kafkaConfig.hosts,
                // backlogMetrics: this._kafkaConfig.backlogMetrics,
            },
            topic: this._lcConfig.bucketTasksTopic,
            groupId: this._lcConfig.bucketProcessor.groupId,
            concurrency: this._lcConfig.bucketProcessor.concurrency,
            queueProcessor: this._processBucketEntry.bind(this),
        });
        this._consumer.on('error', err => {
            if (!this._consumerReady) {
                this._log.fatal('unable to start lifecycle consumer', {
                    error: err,
                    method: 'LifecycleBucketProcessor._setupConsumer',
                });
                process.exit(1);
            }
        });
        this._consumer.on('ready', () => {
            this._consumerReady = true;
            this._consumer.subscribe();
            cb();
        });
    }

    /**
     * Set up the producers and consumers needed for lifecycle.
     * @param {function} done - callback
     * @return {undefined}
     */
    start(done) {
        this._initSTSConfig();
        this._initCredentialsManager();
        async.series([
            done => this._setupProducer(done),
            done => this._initKafkaBacklogMetrics(done),
            done => this._setupConsumer(done),
        ], done);
    }

    _initKafkaBacklogMetrics(cb) {
        this._kafkaBacklogMetrics = new KafkaBacklogMetrics(
            this._zkConfig.connectionString, this._kafkaConfig.backlogMetrics);
        this._kafkaBacklogMetrics.init();
        this._kafkaBacklogMetrics.once('ready', () => {
            this._kafkaBacklogMetrics.removeAllListeners('error');
            cb();
        });
        this._kafkaBacklogMetrics.once('error', err => {
            this._log.error('error setting up kafka topic metrics', {
                error: err,
                method: 'LifecycleBucketProcessor._initKafkaBacklogMetrics',
            });
            process.exit(1);
        });
    }

    _initSTSConfig() {
        if (this._lcConfig.auth.type === authTypeAssumeRole) {
            const { sts } = this._lcConfig.auth;
            this._stsConfig = {
                endpoint: `${this._transport}://${sts.host}:${sts.port}`,
                credentials: {
                    accessKeyId: sts.accessKey,
                    secretAccessKey: sts.secretKey,
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
     * Close the lifecycle bucket processor
     * @param {function} cb - callback function
     * @return {undefined}
     */
    close(cb) {
        if (this._deleteInactiveCredentialsInterval) {
            clearInterval(this._deleteInactiveCredentialsInterval);
        }

        async.parallel([
            done => {
                this._log.debug('closing bucket tasks consumer');
                this._consumer.close(done);
            },
            done => {
                this._log.debug('closing producer');
                this._producer.close(done);
            },
        ], () => cb());
    }

    isReady() {
        return this._producer && this._producer.isReady() &&
               this._consumer && this._consumer.isReady();
    }
}

module.exports = LifecycleBucketProcessor;
