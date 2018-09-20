'use strict'; // eslint-disable-line

const async = require('async');
const http = require('http');
const { EventEmitter } = require('events');
const Redis = require('ioredis');
const schedule = require('node-schedule');

const Logger = require('werelogs').Logger;

const errors = require('arsenal').errors;
const RoundRobin = require('arsenal').network.RoundRobin;

const BackbeatProducer = require('../../../lib/BackbeatProducer');
const BackbeatConsumer = require('../../../lib/BackbeatConsumer');
const VaultClientCache = require('../../../lib/clients/VaultClientCache');
const QueueEntry = require('../../../lib/models/QueueEntry');
const ReplicationTaskScheduler = require('../utils/ReplicationTaskScheduler');
const getLocationsFromStorageClass =
    require('../utils/getLocationsFromStorageClass');
const ReplicateObject = require('../tasks/ReplicateObject');
const MultipleBackendTask = require('../tasks/MultipleBackendTask');
const EchoBucket = require('../tasks/EchoBucket');

const ObjectQueueEntry = require('../../../lib/models/ObjectQueueEntry');
const BucketQueueEntry = require('../../../lib/models/BucketQueueEntry');
const MetricsProducer = require('../../../lib/MetricsProducer');

const {
    zookeeperReplicationNamespace,
    zkCRRStatePath,
    zkCRRStateProperties,
    proxyVaultPath,
    proxyIAMPath,
    replicationBackends,
} = require('../constants');

class QueueProcessor extends EventEmitter {

    /**
     * Create a queue processor object to activate Cross-Region
     * Replication from a kafka topic dedicated to store replication
     * entries to a target S3 endpoint.
     *
     * @constructor
     * @param {node-zookeeper-client.Client} zkClient - zookeeper client
     * @param {Object} kafkaConfig - kafka configuration object
     * @param {String} kafkaConfig.hosts - list of kafka brokers
     *   as "host:port[,host:port...]"
     * @param {Object} sourceConfig - source S3 configuration
     * @param {Object} sourceConfig.s3 - s3 endpoint configuration object
     * @param {Object} sourceConfig.auth - authentication info on source
     * @param {Object} destConfig - target S3 configuration
     * @param {Object} destConfig.auth - authentication info on target
     * @param {Object} repConfig - replication configuration object
     * @param {String} repConfig.topic - replication topic name
     * @param {String} repConfig.queueProcessor - config object
     *   specific to queue processor
     * @param {String} repConfig.queueProcessor.groupId - kafka
     *   consumer group ID
     * @param {String} repConfig.queueProcessor.retryTimeoutS -
     *   number of seconds before giving up retries of an entry
     *   replication
     * @param {Object} redisConfig - redis configuration
     * @param {Object} mConfig - metrics config
     * @param {String} mConfig.topic - metrics config kafka topic
     * @param {String} site - site name
     */
    constructor(zkClient, kafkaConfig, sourceConfig, destConfig, repConfig,
        redisConfig, mConfig, site) {
        super();
        this.zkClient = zkClient;
        this.kafkaConfig = kafkaConfig;
        this.sourceConfig = sourceConfig;
        this.destConfig = destConfig;
        this.repConfig = repConfig;
        this.destHosts = null;
        this.sourceAdminVaultConfigured = false;
        this.destAdminVaultConfigured = false;
        this.replicationStatusProducer = null;
        this._consumer = null;
        this._mProducer = null;
        this.site = site;
        this.mConfig = mConfig;

        this.echoMode = false;
        this.scheduledResume = null;

        this.logger = new Logger(
            `Backbeat:Replication:QueueProcessor:${this.site}`);

        // global variables
        // TODO: for SSL support, create HTTPS agents instead
        this.sourceHTTPAgent = new http.Agent({ keepAlive: true });
        this.destHTTPAgent = new http.Agent({ keepAlive: true });

        this._setupVaultclientCache();
        this._setupRedis(redisConfig);

        // FIXME support multiple scality destination sites
        if (Array.isArray(destConfig.bootstrapList)) {
            destConfig.bootstrapList.forEach(dest => {
                if (Array.isArray(dest.servers)) {
                    this.destHosts =
                        new RoundRobin(dest.servers, { defaultPort: 80 });
                    if (dest.echo) {
                        this._setupEcho();
                    }
                }
            });
        }

        this.taskScheduler = new ReplicationTaskScheduler(
            (ctx, done) => ctx.task.processQueueEntry(ctx.entry, done));
    }

    _setupVaultclientCache() {
        this.vaultclientCache = new VaultClientCache();

        if (this.sourceConfig.auth.type === 'role') {
            const { host, port, adminPort, adminCredentials }
                      = this.sourceConfig.auth.vault;
            this.vaultclientCache
                .setHost('source:s3', host)
                .setPort('source:s3', port);
            if (adminCredentials) {
                this.vaultclientCache
                    .setHost('source:admin', host)
                    .setPort('source:admin', adminPort)
                    .loadAdminCredentials('source:admin',
                                          adminCredentials.accessKey,
                                          adminCredentials.secretKey);
                this.sourceAdminVaultConfigured = true;
            }
        }
        if (this.destConfig.auth.type === 'role') {
            if (this.destConfig.auth.vault) {
                const { host, port, adminPort, adminCredentials }
                          = this.destConfig.auth.vault;
                if (host) {
                    this.vaultclientCache.setHost('dest:s3', host);
                }
                if (port) {
                    this.vaultclientCache.setPort('dest:s3', port);
                }
                if (adminCredentials) {
                    if (host) {
                        this.vaultclientCache.setHost('dest:admin', host);
                    }
                    if (adminPort) {
                        this.vaultclientCache.setPort('dest:admin', adminPort);
                    } else {
                        // if dest vault admin port not configured, go
                        // through nginx proxy
                        this.vaultclientCache.setProxyPath('dest:admin',
                                                           proxyIAMPath);
                    }
                    this.vaultclientCache.loadAdminCredentials(
                        'dest:admin',
                        adminCredentials.accessKey,
                        adminCredentials.secretKey);
                    this.destAdminVaultConfigured = true;
                }
            }
            if (!this.destConfig.auth.vault ||
                !this.destConfig.auth.vault.port) {
                // if dest vault port not configured, go through nginx
                // proxy
                this.vaultclientCache.setProxyPath('dest:s3',
                                                   proxyVaultPath);
            }
        }
    }

    _setupProducer(done) {
        const producer = new BackbeatProducer({
            kafka: { hosts: this.kafkaConfig.hosts },
            topic: this.repConfig.replicationStatusTopic,
        });
        producer.once('error', done);
        producer.once('ready', () => {
            producer.removeAllListeners('error');
            producer.on('error', err => {
                this.logger.error('error from backbeat producer', {
                    topic: this.repConfig.replicationStatusTopic,
                    error: err,
                });
            });
            this.replicationStatusProducer = producer;
            done();
        });
    }

    _setupEcho() {
        if (!this.sourceAdminVaultConfigured) {
            throw new Error('echo mode not properly configured: missing ' +
                            'credentials for source Vault admin client');
        }
        if (!this.destAdminVaultConfigured) {
            throw new Error('echo mode not properly configured: missing ' +
                            'credentials for destination Vault ' +
                            'admin client');
        }
        if (process.env.BACKBEAT_ECHO_TEST_MODE === '1') {
            this.logger.info('starting in echo mode',
                             { method: 'QueueProcessor.constructor',
                               testMode: true });
        } else {
            this.logger.info('starting in echo mode',
                             { method: 'QueueProcessor.constructor' });
        }
        this.echoMode = true;
        this.accountCredsCache = {};
    }

    /**
     * Setup the Redis Subscriber which listens for actions from other processes
     * (i.e. BackbeatAPI for pause/resume)
     * @param {object} redisConfig - redis ha config
     * @return {undefined}
     */
    _setupRedis(redisConfig) {
        // redis pub/sub for pause/resume
        const redis = new Redis(redisConfig);
        // redis subscribe to site specific channel
        const channelName = `${this.repConfig.topic}-${this.site}`;
        redis.subscribe(channelName, err => {
            if (err) {
                this.logger.fatal('queue processor failed to subscribe to ' +
                                  `crr redis channel for location ${this.site}`,
                                  { method: 'QueueProcessor.constructor',
                                    error: err });
                process.exit(1);
            }
            redis.on('message', (channel, message) => {
                const validActions = {
                    pauseService: this._pauseService.bind(this),
                    resumeService: this._resumeService.bind(this),
                    deleteScheduledResumeService:
                        this._deleteScheduledResumeService.bind(this),
                };
                try {
                    const { action, date } = JSON.parse(message);
                    const cmd = validActions[action];
                    if (channel === channelName && typeof cmd === 'function') {
                        cmd(date);
                    }
                } catch (e) {
                    this.logger.error('error parsing redis sub message', {
                        method: 'QueueProcessor._setupRedis',
                        error: e,
                    });
                }
            });
        });
    }

    /**
     * Pause replication consumers
     * @return {undefined}
     */
    _pauseService() {
        const enabled = this._consumer.getServiceStatus();
        if (enabled) {
            // if currently resumed/active, attempt to pause
            this._updateZkStateNode('paused', true, err => {
                if (err) {
                    this.logger.trace('error occurred saving state to ' +
                    'zookeeper', {
                        method: 'QueueProcessor._pauseService',
                    });
                } else {
                    this._consumer.pause(this.site);
                    this.logger.info('paused replication for location: ' +
                        `${this.site}`);
                    this._deleteScheduledResumeService();
                }
            });
        }
    }

    /**
     * Resume replication consumers
     * @param {Date} [date] - optional date object for scheduling resume
     * @return {undefined}
     */
    _resumeService(date) {
        const enabled = this._consumer.getServiceStatus();
        const now = new Date();
        if (date && now < new Date(date)) {
            // if date is in the future, attempt to schedule job
            this.scheduleResume(date);
        } else if (!enabled) {
            // if currently paused, attempt to resume
            this._updateZkStateNode('paused', false, err => {
                if (err) {
                    this.logger.trace('error occurred saving state to ' +
                    'zookeeper', {
                        method: 'QueueProcessor._resumeService',
                    });
                } else {
                    this._consumer.resume(this.site);
                    this.logger.info('resumed replication for location: ' +
                        `${this.site}`);
                    this._deleteScheduledResumeService();
                }
            });
        }
    }

    /**
     * Delete scheduled resume (if any)
     * @return {undefined}
     */
    _deleteScheduledResumeService() {
        this._updateZkStateNode('scheduledResume', null, err => {
            if (err) {
                this.logger.trace('error occurred saving state to zookeeper', {
                    method: 'QueueProcessor._deleteScheduledResumeService',
                });
            } else if (this.scheduledResume) {
                this.scheduledResume.cancel();
                this.scheduledResume = null;
                this.logger.info('deleted scheduled CRR resume for location:' +
                    ` ${this.site}`);
            }
        });
    }

    _getZkSiteNode() {
        return `${zookeeperReplicationNamespace}${zkCRRStatePath}/` +
            `${this.site}`;
    }

    /**
     * Update zookeeper state node for this site-defined QueueProcessor
     * @param {String} key - key name to store in zk state node
     * @param {String|Boolean} value - value
     * @param {Function} cb - callback(error)
     * @return {undefined}
     */
    _updateZkStateNode(key, value, cb) {
        if (!zkCRRStateProperties.includes(key)) {
            const errorMsg = 'incorrect zookeeper state property given';
            this.logger.error(errorMsg, {
                method: 'QueueProcessor._updateZkStateNode',
            });
            return cb(new Error('incorrect zookeeper state property given'));
        }
        const path = this._getZkSiteNode();
        return async.waterfall([
            next => this.zkClient.getData(path, (err, data) => {
                if (err) {
                    this.logger.error('could not get state from zookeeper', {
                        method: 'QueueProcessor._updateZkStateNode',
                        zookeeperPath: path,
                        error: err.message,
                    });
                    return next(err);
                }
                try {
                    const state = JSON.parse(data.toString());
                    // set revised status
                    state[key] = value;
                    const bufferedData = Buffer.from(JSON.stringify(state));
                    return next(null, bufferedData);
                } catch (err) {
                    this.logger.error('could not parse state data from ' +
                    'zookeeper', {
                        method: 'QueueProcessor._updateZkStateNode',
                        zookeeperPath: path,
                        error: err,
                    });
                    return next(err);
                }
            }),
            (data, next) => this.zkClient.setData(path, data, err => {
                if (err) {
                    this.logger.error('could not save state data in ' +
                    'zookeeper', {
                        method: 'QueueProcessor._updateZkStateNode',
                        zookeeperPath: path,
                        error: err,
                    });
                    return next(err);
                }
                return next();
            }),
        ], cb);
    }

    scheduleResume(date) {
        function triggerResume() {
            this._updateZkStateNode('scheduledResume', null, err => {
                if (err) {
                    this.logger.error('error occurred saving state ' +
                    'to zookeeper for resuming a scheduled resume. Retry ' +
                    'again in 1 minute', {
                        method: 'QueueProcessor.scheduleResume',
                        error: err,
                    });
                    // if an error occurs, need to retry
                    // for now, schedule minute from now
                    const date = new Date();
                    date.setMinutes(date.getMinutes() + 1);
                    this.scheduleResume = schedule.scheduleJob(date,
                        triggerResume.bind(this));
                } else {
                    this.scheduledResume.cancel();
                    this.scheduledResume = null;
                    this._resumeService();
                }
            });
        }

        this._updateZkStateNode('scheduledResume', date, err => {
            if (err) {
                this.logger.trace('error occurred saving state to zookeeper', {
                    method: 'QueueProcessor.scheduleResume',
                });
            } else {
                this.scheduledResume = schedule.scheduleJob(date,
                    triggerResume.bind(this));
                this.logger.info('scheduled CRR resume', {
                    scheduleTime: date.toString(),
                });
            }
        });
    }

    getStateVars() {
        return {
            sourceConfig: this.sourceConfig,
            destConfig: this.destConfig,
            repConfig: this.repConfig,
            destHosts: this.destHosts,
            sourceHTTPAgent: this.sourceHTTPAgent,
            destHTTPAgent: this.destHTTPAgent,
            vaultclientCache: this.vaultclientCache,
            accountCredsCache: this.accountCredsCache,
            replicationStatusProducer: this.replicationStatusProducer,
            mProducer: this._mProducer,
            logger: this.logger,
            site: this.site,
        };
    }

    /**
     * Start kafka consumer and producer. Emits a 'ready' even when
     * producer and consumer are ready.
     *
     * Note: for tests, with auto.create.topics.enable option set on
     * kafka container, this will also pre-create the topic.
     *
     * @param {object} [options] options object
     * @param {boolean} [options.disableConsumer] - true to disable
     *   startup of consumer (for testing: one has to call
     *   processQueueEntry() explicitly)
     * @param {boolean} [options.paused] - if true, kafka consumer is paused
     * @return {undefined}
     */
    start(options) {
        this._mProducer = new MetricsProducer(this.kafkaConfig, this.mConfig);
        return this._mProducer.setupProducer(err => {
            if (err) {
                this.logger.info('error setting up metrics producer',
                                 { error: err.message });
                process.exit(1);
            }
            return this._setupProducer(err => {
                let consumerReady = false;
                if (err) {
                    this.logger.info('error setting up kafka producer',
                                     { error: err.message });
                    process.exit(1);
                }
                if (options && options.disableConsumer) {
                    this.emit('ready');
                    return undefined;
                }
                const groupId =
                    `${this.repConfig.queueProcessor.groupId}-${this.site}`;
                this._consumer = new BackbeatConsumer({
                    kafka: { hosts: this.kafkaConfig.hosts },
                    topic: this.repConfig.topic,
                    groupId,
                    concurrency: this.repConfig.queueProcessor.concurrency,
                    queueProcessor: this.processKafkaEntry.bind(this),
                });
                this._consumer.on('error', () => {
                    if (!consumerReady) {
                        this.logger.fatal('queue processor failed to start a ' +
                                       'backbeat consumer');
                        process.exit(1);
                    }
                });
                this._consumer.on('ready', () => {
                    consumerReady = true;
                    const paused = options && options.paused;
                    this._consumer.subscribe(paused);
                });
                this._consumer.on('canary', () => {
                    this.logger.info('queue processor is ready to consume ' +
                                     'replication entries');
                    this.emit('ready');
                });
                return undefined;
            });
        });
    }

    /**
     * Cleanup zookeeper node if the site has been removed as a location
     * @param {function} cb - callback(error)
     * @return {undefined}
     */
    removeZkState(cb) {
        const path = this._getZkSiteNode();
        this.zkClient.remove(path, err => {
            if (err && err.name !== 'NO_NODE') {
                this.logger.error('failed removing zookeeper state node', {
                    method: 'QueueProcessor.removeZkState',
                    zookeeperPath: path,
                    error: err,
                });
                return cb(err);
            }
            return cb();
        });
    }

    /**
     * Stop kafka producer and consumer, commit current consumer offset, and
     * remove any zookeeper state for this instance
     *
     * @param {function} done - callback
     * @return {undefined}
     */
    stop(done) {
        if (!this.replicationStatusProducer) {
            return setImmediate(done);
        }
        return this.replicationStatusProducer.close(() => {
            if (this._consumer) {
                this._consumer.close(done);
            } else {
                done();
            }
        });
    }

    /**
     * Proceed to the replication of an object given a kafka
     * replication queue entry
     *
     * @param {object} kafkaEntry - entry generated by the queue populator
     * @param {string} kafkaEntry.key - kafka entry key
     * @param {string} kafkaEntry.value - kafka entry value
     * @param {function} done - callback function
     * @return {undefined}
     */
    processKafkaEntry(kafkaEntry, done) {
        const sourceEntry = QueueEntry.createFromKafkaEntry(kafkaEntry);
        if (sourceEntry.error) {
            this.logger.error('error processing source entry',
                              { error: sourceEntry.error });
            return process.nextTick(() => done(errors.InternalError));
        }
        if (sourceEntry.skip) {
            // skip message, noop
            return process.nextTick(done);
        }
        let task;
        if (sourceEntry instanceof BucketQueueEntry) {
            if (this.echoMode) {
                task = new EchoBucket(this);
            }
            // ignore bucket entry if echo mode disabled
        } else if (sourceEntry instanceof ObjectQueueEntry) {
            const replicationStorageClass =
                sourceEntry.getReplicationStorageClass();
            const sites = getLocationsFromStorageClass(replicationStorageClass);
            if (sites.includes(this.site)) {
                const replicationEndpoint = this.destConfig.bootstrapList
                    .find(endpoint => endpoint.site === this.site);
                if (replicationEndpoint &&
                replicationBackends.includes(replicationEndpoint.type)) {
                    task = new MultipleBackendTask(this);
                } else {
                    task = new ReplicateObject(this);
                }
            }
        }
        if (task) {
            this.logger.debug('source entry is being pushed',
              { entry: sourceEntry.getLogInfo() });
            return this.taskScheduler.push({ task, entry: sourceEntry },
                                           sourceEntry.getCanonicalKey(),
                                           done);
        }
        this.logger.debug('skip source entry',
                          { entry: sourceEntry.getLogInfo() });
        return process.nextTick(done);
    }

    isReady() {
        return this.replicationStatusProducer && this._consumer &&
            this.replicationStatusProducer.isReady() &&
            this._consumer.isReady();
    }
}

module.exports = QueueProcessor;
