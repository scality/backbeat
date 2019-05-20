'use strict'; // eslint-disable-line

const fs = require('fs');
const async = require('async');
const AWS = require('aws-sdk');
const http = require('http');
const { Logger } = require('werelogs');
const { errors } = require('arsenal');

const BackbeatProducer = require('../../../lib/BackbeatProducer');
const BackbeatConsumer = require('../../../lib/BackbeatConsumer');
const BackbeatMetadataProxy = require('../../../lib/BackbeatMetadataProxy');
const LifecycleTask = require('../tasks/LifecycleTask');
const { getAccountCredentials } =
      require('../../../lib/credentials/AccountCredentials');
const VaultClientCache = require('../../../lib/clients/VaultClientCache');
const safeJsonParse = require('../util/safeJsonParse');

const PROCESS_OBJECTS_ACTION = 'processObjects';

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
     * @param {Object} extensions - backbeat config extensions object
     * @param {Object} extensions.lifecycle - lifecycle config
     * @param {Object} extensions.lifecycle.auth - authentication info
     * @param {String} extensions.lifecycle.bucketTasksTopic - lifecycle bucket
     * topic name
     * @param {Object} extensions.lifecycle.bucketProcessor - kafka consumer
     * object
     * @param {String} extensions.lifecycle.bucketProcessor.groupId - kafka
     * consumer group id
     * @param {Object} extensions.replication - replication config
     * @param {String} extensions.replication.topic - kafka replication topic
     * @param {Object} extensions.replication.source - replication source
     * @param {Number} [lcConfig.bucketProcessor.concurrency] - number
     *  of max allowed concurrent operations
     * @param {Object} [lcConfig.backlogMetrics] - param object to
     * publish backlog metrics to zookeeper (see {@link
     * BackbeatConsumer} constructor)
     * @param {Object} s3Config - s3 config
     * @param {String} s3Config.host - host ip
     * @param {String} s3Config.port - port
     * @param {String} transport - http or https
     */
    constructor(zkConfig, kafkaConfig, extensions, s3Config, transport) {
        this._log = new Logger('Backbeat:Lifecycle:BucketProcessor');
        this._zkConfig = zkConfig;
        this._kafkaConfig = kafkaConfig;
        this._lcConfig = extensions.lifecycle;
        this._repConfig = extensions.replication;
        this._s3Endpoint = `${transport}://${s3Config.host}:${s3Config.port}`;
        this._transport = transport;
        this._producer = null;
        this.accountCredsCache = {};

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
            log: this._log,
        };
    }

    /**
     * Return an S3 client instance using the given account credentials.
     * @param {Object} accountCreds - Object containing account credentials
     * @param {String} accountCreds.accessKeyId - The account access key
     * @param {String} accountCreds.secretAccessKey - The account secret key
     * @return {AWS.S3} The S3 client instance to make requests with
     */
    _getS3Client(accountCreds) {
        return new AWS.S3({
            endpoint: this._s3Endpoint,
            credentials: {
                accessKeyId: accountCreds.accessKeyId,
                secretAccessKey: accountCreds.secretAccessKey,
            },
            sslEnabled: this._transport === 'https',
            s3ForcePathStyle: true,
            signatureVersion: 'v4',
            httpOptions: {
                agent: new http.Agent({ keepAlive: true }),
                timeout: 0,
            },
            maxRetries: 0,
        });
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
        const { bucket, owner } = result.target;
        if (!bucket || !owner) {
            this._log.error('kafka bucket entry missing required fields', {
                method: 'LifecycleBucketProcessor._processBucketEntry',
                bucket,
                owner,
            });
            return process.nextTick(() => cb(errors.InternalError));
        }
        this._log.debug('processing bucket entry', {
            method: 'LifecycleBucketProcessor._processBucketEntry',
            bucket,
            owner,
        });
        return async.waterfall([
            next => this._getAccountCredentials(owner, next),
            (accountCreds, next) => {
                const s3 = this._getS3Client(accountCreds);
                const params = { Bucket: bucket };
                return s3.getBucketLifecycleConfiguration(params,
                (err, data) => {
                    next(err, data, s3);
                });
            },
        ], (err, config, s3) => {
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
            const backbeatMetadataProxy =
                new BackbeatMetadataProxy(this._s3Endpoint, this._lcConfig.auth)
                    .setSourceClient(this._log);
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
        });
        producer.once('error', cb);
        producer.once('ready', () => {
            producer.removeAllListeners('error');
            producer.on('error', err => {
                this._log.error('error from backbeat producer', {
                    error: err,
                });
            });
            return cb(null, producer);
        });
    }

    /**
     * Set up the lifecycle consumer.
     * @return {undefined}
     */
    _setupConsumer() {
        let consumerReady = false;
        this._consumer = new BackbeatConsumer({
            zookeeper: {
                connectionString: this._zkConfig.connectionString,
            },
            kafka: { hosts: this._kafkaConfig.hosts },
            topic: this._lcConfig.bucketTasksTopic,
            groupId: this._lcConfig.bucketProcessor.groupId,
            concurrency: this._lcConfig.bucketProcessor.concurrency,
            queueProcessor: this._processBucketEntry.bind(this),
            backlogMetrics: this._lcConfig.backlogMetrics,
        });
        this._consumer.on('error', err => {
            if (!consumerReady) {
                this._log.fatal('unable to start lifecycle consumer', {
                    error: err,
                    method: 'LifecycleBucketProcessor._setupConsumer',
                });
                process.exit(1);
            }
        });
        this._consumer.on('ready', () => {
            consumerReady = true;
            this._consumer.subscribe();
        });
    }

    /**
     * Set up the credentials (service account credentials or provided
     * by vault depending on config)
     * @return {undefined}
     */
    _setupCredentials() {
        const { type } = this._lcConfig.auth;
        if (type === 'vault') {
            return this._setupVaultClientCache();
        }
        return undefined;
    }

    /**
     * Set up the vault client cache for making requests to vault.
     * @return {undefined}
     */
    _setupVaultClientCache() {
        const { vault } = this._lcConfig.auth;
        const { host, port, adminPort, adminCredentialsFile } = vault;
        const adminCredsJSON = fs.readFileSync(adminCredentialsFile);
        const adminCredsObj = JSON.parse(adminCredsJSON);
        const accessKey = Object.keys(adminCredsObj)[0];
        const secretKey = adminCredsObj[accessKey];
        this._vaultClientCache = new VaultClientCache();
        if (accessKey && secretKey) {
            this._vaultClientCache
                .setHost('lifecycle:admin', host)
                .setPort('lifecycle:admin', adminPort)
                .loadAdminCredentials('lifecycle:admin', accessKey, secretKey);
        } else {
            throw new Error('Lifecycle bucket processor not properly ' +
                'configured: missing credentials for Vault admin client');
        }
        this._vaultClientCache
            .setHost('lifecycle:s3', host)
            .setPort('lifecycle:s3', port);
    }

    /**
     * Get the account's credentials for making a request with S3.
     * @param {String} canonicalId - The canonical ID of the bucket owner.
     * @param {Function} cb - The callback to call with the account credentials.
     * @return {undefined}
     */
    _getAccountCredentials(canonicalId, cb) {
        const cachedAccountCreds = this.accountCredsCache[canonicalId];
        if (cachedAccountCreds) {
            return process.nextTick(() => cb(null, cachedAccountCreds));
        }
        const credentials = getAccountCredentials(this._lcConfig.auth,
                                                  this._log);
        if (credentials) {
            this.accountCredsCache[canonicalId] = credentials;
            return process.nextTick(() => cb(null, credentials));
        }
        const { type } = this._lcConfig.auth;
        if (type === 'vault') {
            return this._generateVaultAdminCredentials(canonicalId, cb);
        }
        return cb(errors.InternalError.customizeDescription(
            `invalid auth type ${type}`));
    }

    _generateVaultAdminCredentials(canonicalId, cb) {
        const vaultClient = this._vaultClientCache.getClient('lifecycle:s3');
        const vaultAdmin = this._vaultClientCache.getClient('lifecycle:admin');
        return async.waterfall([
            // Get the account's display name for generating a new access key.
            next =>
                vaultClient.getAccounts(undefined, undefined, [canonicalId], {},
                (err, data) => {
                    if (err) {
                        return next(err);
                    }
                    if (data.length !== 1) {
                        return next(errors.InternalError);
                    }
                    return next(null, data[0].name);
                }),
            // Generate a new account access key beacuse it has not been cached.
            (name, next) =>
                vaultAdmin.generateAccountAccessKey(name, (err, data) => {
                    if (err) {
                        return next(err);
                    }
                    const accountCreds = {
                        accessKeyId: data.id,
                        secretAccessKey: data.value,
                    };
                    this.accountCredsCache[canonicalId] = accountCreds;
                    return next(null, accountCreds);
                }),
        ], (err, accountCreds) => {
            if (err) {
                this._log.error('error generating new access key', {
                    error: err.message,
                    method: 'LifecycleBucketProcessor._getAccountCredentials',
                });
                return cb(err);
            }
            return cb(null, accountCreds);
        });
    }

    /**
     * Set up the producers and consumers needed for lifecycle.
     * @return {undefined}
     */
    start() {
        this._setupCredentials();
        this._setupProducer((err, producer) => {
            if (err) {
                this._log.error('error setting up kafka producer', {
                    error: err,
                    method: 'LifecycleBucketProcessor.start',
                });
                process.exit(1);
            }
            this._setupConsumer();
            this._producer = producer;
            this._log.info('lifecycle bucket processor successfully started');
        });
    }

    /**
     * Close the lifecycle bucket processor
     * @param {function} cb - callback function
     * @return {undefined}
     */
    close(cb) {
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
