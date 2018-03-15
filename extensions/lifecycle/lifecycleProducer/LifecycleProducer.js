'use strict'; // eslint-disable-line

const fs = require('fs');
const async = require('async');
const AWS = require('aws-sdk');
const http = require('http');
const { Logger } = require('werelogs');
const { errors } = require('arsenal');

const BackbeatProducer = require('../../../lib/BackbeatProducer');
const BackbeatConsumer = require('../../../lib/BackbeatConsumer');
const LifecycleTask = require('../tasks/LifecycleTask');
const VaultClientCache = require('../../../lib/clients/VaultClientCache');
const safeJsonParse = require('../util/safeJsonParse');

const PROCESS_OBJECTS_ACTION = 'processObjects';

/**
 * @class LifecycleProducer
 *
 * @classdesc Handles consuming entries from the bucket topic and sending those
 * entries to be processed by the lifecycle task. Once processed, the entry is
 * sent to the object topic for another consumer to perform the lifecycle
 * operation (e.g., expiration) on the object. If another listing is required on
 * the bucket (i.e., the listing is truncated), an entry is pushed to the bucket
 * topic for the next round of processing.
 */
class LifecycleProducer {
    /**
     * Constructor of LifecycleProducer
     * @param {Object} zkConfig - zookeeper config
     * @param {Object} kafkaConfig - kafka configuration object
     * @param {string} kafkaConfig.hosts - list of kafka brokers
     *   as "host:port[,host:port...]"
     * @param {Object} lcConfig - lifecycle config
     * @param {Object} [lcConfig.backlogMetrics] - param object to
     * publish backlog metrics to zookeeper (see {@link
     * BackbeatConsumer} constructor)
     * @param {Object} s3Config - s3 config
     * @param {String} s3Config.host - host ip
     * @param {String} s3Config.port - port
     * @param {Object} authConfig - auth config
     * @param {String} [authConfig.account] - account name
     * @param {Object} [authConfig.vault] - vault details
     * @param {String} authConfig.vault.host - vault host ip
     * @param {number} authConfig.vault.port - vault port
     * @param {number} authConfig.vault.adminPort - vault admin port
     * @param {String} transport - http or https
     */
    constructor(zkConfig, kafkaConfig, lcConfig, s3Config, authConfig,
                transport) {
        this._log = new Logger('Backbeat:LifecycleProducer');
        this._zkConfig = zkConfig;
        this._kafkaConfig = kafkaConfig;
        this._lcConfig = lcConfig;
        this._authConfig = authConfig;
        this._s3Endpoint = `${transport}://${s3Config.host}:${s3Config.port}`;
        this._transport = transport;
        this._bucketProducer = null;
        this._objectProducer = null;
        this.accountCredsCache = {};

        // The task scheduler for processing lifecycle tasks concurrently.
        this._internalTaskScheduler = async.queue((ctx, cb) => {
            const { task, rules, value, s3target } = ctx;
            return task.processBucketEntry(rules, value, s3target, cb);
        }, this._lcConfig.producer.concurrency);

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
     * Send entry to the object task topic
     * @param {Object} entry - The Kafka entry to send to the topic
     * @param {Function} cb - The callback to call
     * @return {undefined}
     */
    sendObjectEntry(entry, cb) {
        const entries = [{ message: JSON.stringify(entry) }];
        this._objectProducer.send(entries, cb);
    }

    /**
     * Send entry back to bucket task topic
     * @param {Object} entry - The Kafka entry to send to the topic
     * @param {Function} cb - The callback to call
     * @return {undefined}
     */
    sendBucketEntry(entry, cb) {
        const entries = [{ message: JSON.stringify(entry) }];
        this._bucketProducer.send(entries, cb);
    }

    getQueuedBucketsZkPath() {
        return `${this._lcConfig.zookeeperPath}/run/queuedBuckets`;
    }

    /**
     * Get the state variables of the current instance.
     * @return {Object} Object containing the state variables
     */
    getStateVars() {
        return {
            sendBucketEntry: this.sendBucketEntry.bind(this),
            sendObjectEntry: this.sendObjectEntry.bind(this),
            enabledRules: this._lcConfig.rules,
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
                method: 'LifecycleProducer._processBucketEntry',
                entry: result,
            });
            return process.nextTick(() => cb(errors.InternalError));
        }
        const { bucket, owner } = result.target;
        if (!bucket || !owner) {
            this._log.error('kafka bucket entry missing required fields', {
                method: 'LifecycleProducer._processBucketEntry',
                bucket,
                owner,
            });
            return process.nextTick(() => cb(errors.InternalError));
        }
        this._log.debug('processing bucket entry', {
            method: 'LifecycleProducer._processBucketEntry',
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
                    method: 'LifecycleProducer._processBucketEntry',
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
                method: 'LifecycleProducer._processBucketEntry',
                bucket,
                owner,
                details: result.details,
            });
            return this._internalTaskScheduler.push({
                task: new LifecycleTask(this),
                rules: config.Rules,
                value: result,
                s3target: s3,
            }, cb);
        });
    }

    /**
     * Set up the backbeat producer with the given topic.
     * @param {String} topic - - Kafka topic to write to
     * @param {Function} cb - The callback to call
     * @return {undefined}
     */
    _setupProducer(topic, cb) {
        const producer = new BackbeatProducer({
            kafka: { hosts: this._kafkaConfig.hosts },
            topic,
        });
        producer.once('error', cb);
        producer.once('ready', () => {
            producer.removeAllListeners('error');
            producer.on('error', err => {
                this._log.error('error from backbeat producer', {
                    topic,
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
        const consumer = new BackbeatConsumer({
            zookeeper: {
                connectionString: this._zkConfig.connectionString,
            },
            kafka: { hosts: this._kafkaConfig.hosts },
            topic: this._lcConfig.bucketTasksTopic,
            groupId: this._lcConfig.producer.groupId,
            concurrency: this._lcConfig.producer.concurrency,
            queueProcessor: this._processBucketEntry.bind(this),
            autoCommit: true,
            backlogMetrics: this._lcConfig.backlogMetrics,
        });
        consumer.on('error', () => {});
        consumer.on('ready', () => {
            consumer.subscribe();
        });
    }

    /**
     * Set up the vault client cache for making requests to vault.
     * @return {undefined}
     */
    _setupVaultClientCache() {
        const { vault } = this._authConfig;
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
            throw new Error('Lifecycle producer not properly configured: ' +
                'missing credentials for Vault admin client');
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
                    method: 'LifecycleProducer._getAccountCredentials',
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
        this._setupVaultClientCache();
        this._setupConsumer();
        return async.parallel([
            // Set up producer to populate the lifecycle bucket task topic.
            next => this._setupProducer(this._lcConfig.bucketTasksTopic,
                (err, producer) => {
                    if (err) {
                        this._log.error('error setting up kafka producer for ' +
                        'bucket task', {
                            error: err.message,
                            method: 'LifecycleProducer.start',
                        });
                        return next(err);
                    }
                    this._bucketProducer = producer;
                    return next();
                }),
            // Set up producer to populate the lifecycle object task topic.
            next => this._setupProducer(this._lcConfig.objectTasksTopic,
                (err, producer) => {
                    if (err) {
                        this._log.error('error setting up kafka producer for ' +
                        'object task', {
                            error: err.message,
                            method: 'LifecycleProducer.start',
                        });
                        return next(err);
                    }
                    this._objectProducer = producer;
                    return next();
                }),
        ], err => {
            if (err) {
                this._log.error('error setting up kafka clients', {
                    error: err,
                    method: 'LifecycleProducer.start',
                });
            }
            this._log.info('lifecycle producer successfully started');
            return undefined;
        });
    }
}

module.exports = LifecycleProducer;
