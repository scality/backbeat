'use strict'; // eslint-disable-line

const async = require('async');
const schedule = require('node-schedule');
const zookeeper = require('node-zookeeper-client');

const { constants, errors } = require('arsenal');
const Logger = require('werelogs').Logger;
const BucketClient = require('bucketclient').RESTClient;

const BackbeatProducer = require('../../../lib/BackbeatProducer');
const BackbeatTask = require('../../../lib/tasks/BackbeatTask');
const zookeeperHelper = require('../../../lib/clients/zookeeper');
const KafkaBacklogMetrics = require('../../../lib/KafkaBacklogMetrics');
const { authTypeAssumeRole } = require('../../../lib/constants');
const VaultClientCache = require('../../../lib/clients/VaultClientCache');
const safeJsonParse = require('../util/safeJsonParse');
const { AccountIdCache } = require('../util/AccountIdCache');

const DEFAULT_CRON_RULE = '* * * * *';
const DEFAULT_CONCURRENCY = 10;

const LIFEYCLE_CONDUCTOR_CLIENT_ID = 'lifecycle:conductor';

/**
 * @class LifecycleConductor
 *
 * @classdesc Background task that periodically reads the lifecycled
 * buckets list on Zookeeper and creates bucket listing tasks on
 * Kafka.
 */
class LifecycleConductor {

    /**
     * @constructor
     * @param {Object} zkConfig - zookeeper configuration object
     * @param {String} zkConfig.connectionString - zookeeper connection string
     *  as "host:port[/chroot]"
     * @param {Object} kafkaConfig - kafka configuration object
     * @param {string} kafkaConfig.hosts - list of kafka brokers
     *   as "host:port[,host:port...]"
     * @param {String} [kafkaConfig.backlogMetrics] - kafka topic
     * metrics config object (see {@link BackbeatConsumer} constructor
     * for params)
     * @param {Object} lcConfig - lifecycle configuration object
     * @param {String} lcConfig.bucketSource - whether to fetch buckets
     * from zookeeper or bucketd
     * @param {Object} lcConfig.bucketd - host:port bucketd configuration
     * @param {String} lcConfig.zookeeperPath - base path for
     * lifecycle nodes in zookeeper
     * @param {String} lcConfig.bucketTasksTopic - lifecycle
     *   bucket tasks topic name
     * @param {Object} lcConfig.backlogControl - lifecycle backlog
     * control params
     * @param {Boolean} [lcConfig.backlogControl.enabled] - enable
     * lifecycle backlog control
     * @param {String} lcConfig.conductor - config object specific to
     *   lifecycle conductor
     * @param {String} [lcConfig.conductor.cronRule="* * * * *"] -
     *   cron rule for bucket processing periodic task
     * @param {Number} [lcConfig.conductor.concurrency=10] - maximum
     *   number of concurrent bucket-to-kafka operations allowed
     * @param {Object} repConfig - replication configuration object
     */
    constructor(zkConfig, kafkaConfig, lcConfig, repConfig) {
        this.zkConfig = zkConfig;
        this.kafkaConfig = kafkaConfig;
        this.lcConfig = lcConfig;
        this.repConfig = repConfig;
        this._cronRule =
            this.lcConfig.conductor.cronRule || DEFAULT_CRON_RULE;
        this._concurrency =
            this.lcConfig.conductor.concurrency || DEFAULT_CONCURRENCY;
        this._maxInFlightBatchSize = this._concurrency * 2;
        this._bucketSource = this.lcConfig.conductor.bucketSource;
        this._bucketdConfig = this.lcConfig.conductor.bucketd;
        this._producer = null;
        this._zkClient = null;
        this._bucketClient = null;
        this._kafkaBacklogMetrics = null;
        this._started = false;
        this._cronJob = null;
        this._vaultClientCache = null;
        this._initialized = false;
        this._batchInProgress = false;

        // this cache only needs to be the size of one listing.
        // worst case scenario is 1 account per bucket:
        // - max size is this._concurrency, rotated entirely at each listing
        // best case scenario is only a few accounts for all buckets
        // - the cache never reaches max size and only a few calls to vault are issued
        // middle case scenario is:
        // - the last entry of the last listing is reused for this listing, the others are pushed out
        //   as necessary, because listings are ordered by canonical id
        // - some entries are expired for each listing
        this._accountIdCache = new AccountIdCache(this._concurrency);

        this.logger = new Logger('Backbeat:Lifecycle:Conductor');
    }

    getBucketsZkPath() {
        return `${this.lcConfig.zookeeperPath}/data/buckets`;
    }

    initZkPaths(cb) {
        async.each([this.getBucketsZkPath()],
                   (path, done) => this._zkClient.mkdirp(path, done), cb);
    }

    _getAccountIds(canonicalIds, cb) {
        // if auth is not of type `assumeRole`, then
        // the accountId can be omitted from work queue messages
        if (this.lcConfig.auth.type !== authTypeAssumeRole) {
            return process.nextTick(cb, null, {});
        }

        const client = this._vaultClientCache.getClient(LIFEYCLE_CONDUCTOR_CLIENT_ID);
        const opts = {};
        return client.getAccountIds(canonicalIds, opts, (err, res) => {
            if (err) {
                return cb(err);
            }
            return cb(null, res.message.body);
        });
    }

    processBuckets(cb) {
        const log = this.logger.newRequestLogger();
        const start = new Date();
        let nBucketsQueued = 0;
        let messageDeliveryReports = 0;

        const messageSendQueue = async.cargo((tasks, done) => {
            if (tasks.length === 0) {
                return done();
            }

            nBucketsQueued += tasks.length;

            const unknownCanonicalIds = new Set(
                tasks
                    .map(t => t.canonicalId)
                    .filter(c => !this._accountIdCache.isKnown(c))
            );

            return async.eachLimit(
                unknownCanonicalIds,
                10,
                (canonicalId, done) => {
                    this._getAccountIds([canonicalId], (err, accountIds) => {
                        if (err) {
                            if (err.NoSuchEntity) {
                                log.error('canonical id does not exist', { error: err, canonicalId });
                                this._accountIdCache.miss(canonicalId);
                            } else {
                                log.error('could not get account id', { error: err, canonicalId });
                            }

                            // don't propagate the error, to avoid interrupting the whole cargo
                            return done();
                        }

                        this._accountIdCache.set(canonicalId, accountIds[canonicalId]);

                        return done();
                    });
                },
                () => {
                    // ignore error, it has been reported before and should not stop the whole cargo
                    const messages = tasks
                        .filter(t => {
                            if (!this._accountIdCache.has(t.canonicalId)) {
                                log.warn('skipping bucket, unknown canonical id', {
                                    bucketName: t.bucketName,
                                    canonicalId: t.canonicalId,
                                });
                                return false;
                            }
                            return true;
                        })
                        .map(t => ({
                            message: JSON.stringify({
                                action: 'processObjects',
                                target: {
                                    bucket: t.bucketName,
                                    owner: t.canonicalId,
                                    accountId: this._accountIdCache.get(t.canonicalId),
                                },
                                details: {},
                            }),
                        }));

                    log.info('bucket push progress', {
                        nBucketsQueued,
                        bucketsInCargo: tasks.length,
                        kafkaBucketMessagesDeliveryReports: messageDeliveryReports,
                        kafkaEnqueueRateHz: Math.round(nBucketsQueued * 1000 / (new Date() - start)),
                    });

                    this._accountIdCache.expireOldest();

                    return this._producer.send(messages, (err, res) => {
                        messageDeliveryReports += messages.length;
                        done(err, res);
                    });
                }
            );
        });

        async.waterfall([
            next => this._controlBacklog(next),
            next => {
                this._batchInProgress = true;
                log.info('starting new lifecycle batch', { bucketSource: this._bucketSource });
                this.listBuckets(messageSendQueue, log, next);
            },
            (nBucketsListed, next) => {
                async.until(
                    () => nBucketsQueued === nBucketsListed,
                    unext => setTimeout(unext, 1000),
                    next);
            },
        ], err => {
            if (err && err.Throttling) {
                log.info('not starting new lifecycle batch', { reason: err });
                if (cb) {
                    cb(err);
                }
                return;
            }

            this._batchInProgress = false;
            const unknownCanonicalIds = this._accountIdCache.getMisses();

            if (err) {
                log.error('lifecycle batch failed', { error: err, unknownCanonicalIds });
                if (cb) {
                    cb(err);
                }
                return;
            }

            log.info('finished pushing lifecycle batch', { nBucketsQueued, unknownCanonicalIds });
            if (cb) {
                cb();
            }
        });
    }

    listBuckets(queue, log, cb) {
        if (this._bucketSource === 'zookeeper') {
            return this.listZookeeperBuckets(queue, log, cb);
        }

        return this.listBucketdBuckets(queue, log, cb);
    }

    listZookeeperBuckets(queue, log, cb) {
        const zkBucketsPath = this.getBucketsZkPath();
        this._zkClient.getChildren(
            zkBucketsPath,
            null,
            (err, buckets) => {
                if (err) {
                    log.error(
                        'error getting list of buckets from zookeeper',
                        { zkPath: zkBucketsPath, error: err.message });
                    return cb(err);
                }

                const batch = buckets.map(bucket => {
                    const [canonicalId, bucketUID, bucketName] =
                              bucket.split(':');
                    if (!canonicalId || !bucketUID || !bucketName) {
                        log.error(
                            'malformed zookeeper bucket entry, skipping',
                            { zkPath: zkBucketsPath, bucket });
                        return null;
                    }

                    return { canonicalId, bucketName };
                });

                queue.push(batch);
                return process.nextTick(cb, null, batch.length);
            });
    }

    listBucketdBuckets(queue, log, cb) {
        let isTruncated = false;
        let marker = null;
        let nEnqueued = 0;
        const start = new Date();
        const retryWrapper = new BackbeatTask();

        async.doWhilst(
            next => {
                if (queue.length() > this._maxInFlightBatchSize) {
                    log.info('delaying bucket pull', {
                        nEnqueuedToDownstream: nEnqueued,
                        inFlight: queue.length(),
                        maxInFlight: this._maxInFlightBatchSize,
                        bucketListingPushRateHz: Math.round(nEnqueued * 1000 / (new Date() - start)),
                    });

                    return setTimeout(next, 10000);
                }

                return retryWrapper.retry({
                    actionDesc: 'list accounts+buckets',
                    logFields: { marker },
                    actionFunc: done =>
                        this._bucketClient.listObject(
                            constants.usersBucket,
                            log.getSerializedUids(),
                            { marker, prefix: '', maxKeys: this._concurrency },
                            (err, resp) => {
                                if (err) {
                                    return done(err);
                                }

                                const { error, result } = safeJsonParse(resp);
                                if (error) {
                                    return done(error);
                                }

                                isTruncated = result.IsTruncated;
                                nEnqueued += result.Contents.length;

                                result.Contents.forEach(o => {
                                    marker = o.key;
                                    const [canonicalId, bucketName] = marker.split(constants.splitter);
                                    queue.push({ canonicalId, bucketName });
                                });

                                return done();
                            }
                        ),
                    shouldRetryFunc: () => true,
                    log,
                }, next);
            },
            () => isTruncated,
            err => cb(err, nEnqueued));
    }

    _controlBacklog(done) {
        // skip backlog control step in the following cases:
        // - disabled in config
        // - backlog metrics not configured
        // - on first processing cycle (to guarantee progress in case
        //   of restarts)
        if (!this.lcConfig.conductor.backlogControl.enabled ||
            !this.kafkaConfig.backlogMetrics) {
            return process.nextTick(done);
        }

        if (this._batchInProgress) {
            return process.nextTick(done, errors.Throttling.customizeDescription('Batch in progress'));
        }

        // check that previous lifecycle batch has completely been
        // processed from all topics before starting a new one
        return async.series({
            // check that bucket tasks topic consumer lag is 0 (no
            // need to allow any lag because the conductor expects
            // everything to be processed in a single cycle before
            // starting another one, so pass maxLag=0)
            bucketTasksCheck:
            next => this._kafkaBacklogMetrics.checkConsumerLag(
                this.lcConfig.bucketTasksTopic,
                this.lcConfig.bucketProcessor.groupId, 0, next),

            // check that object tasks topic consumer lag is 0
            objectTasksCheck:
            next => this._kafkaBacklogMetrics.checkConsumerLag(
                this.lcConfig.objectTasksTopic,
                this.lcConfig.objectProcessor.groupId, 0, next),

            // check that data mover topic consumer has progressed
            // beyond the latest lifecycle snapshot of topic offsets,
            // which means everything from the latest lifecycle batch
            // has been consumed
            // NOTE: Transition not implemented
            // dataMoverCheck:
            // next => this._kafkaBacklogMetrics.checkConsumerProgress(
            //     this.repConfig.dataMoverTopic, null, 'lifecycle', next),
        }, (err, checkResults) => {
            if (err) {
                return done(err);
            }
            const doSkip = Object.keys(checkResults).some(
                checkType => checkResults[checkType] !== undefined);
            if (doSkip) {
                this.logger.info('skipping lifecycle batch due to ' +
                                 'previous operation still in progress',
                                 checkResults);
                return done(errors.Throttling);
            }
            return done();
        });
    }

    _setupProducer(cb) {
        const producer = new BackbeatProducer({
            kafka: { hosts: this.kafkaConfig.hosts },
            topic: this.lcConfig.bucketTasksTopic,
        });
        producer.once('error', cb);
        producer.once('ready', () => {
            this.logger.debug(
                'producer is ready',
                { kafkaConfig: this.kafkaConfig,
                    topic: this.lcConfig.bucketTasksTopic });
            producer.removeAllListeners('error');
            producer.on('error', err => {
                this.logger.error('error from backbeat producer', {
                    topic: this.lcConfig.bucketTasksTopic,
                    error: err,
                });
            });
            this._producer = producer;
            cb();
        });
    }

    _setupBucketdClient(cb) {
        if (this._bucketSource === 'bucketd') {
            const { host, port } = this._bucketdConfig;
            // TODO https support S3C-4659
            this._bucketClient = new BucketClient(`${host}:${port}`);
        }

        process.nextTick(cb);
    }

    _setupZookeeperClient(cb) {
        if (this._bucketSource !== 'zookeeper') {
            process.nextTick(cb);
            return;
        }
        this._zkClient = zookeeperHelper.createClient(
            this.zkConfig.connectionString);
        this._zkClient.connect();
        this._zkClient.once('error', cb);
        this._zkClient.once('ready', () => {
            // just in case there would be more 'error' events
            // emitted
            this._zkClient.removeAllListeners('error');
            this._zkClient.on('error', err => {
                this.logger.error(
                    'error from lifecycle conductor zookeeper client',
                    { error: err });
            });
            cb();
        });
    }

    /**
     * Initialize kafka producer and clients
     *
     * @param {function} done - callback
     * @return {undefined}
     */
    init(done) {
        if (this._initialized) {
            // already initialized
            return process.nextTick(done);
        }

        this._setupVaultClientCache();
        return async.series([
            next => this._setupProducer(next),
            next => this._setupZookeeperClient(next),
            next => this._setupBucketdClient(next),
            next => this._initKafkaBacklogMetrics(next),
        ], done);
    }

    _setupVaultClientCache() {
        if (this.lcConfig.auth.type !== authTypeAssumeRole) {
            return;
        }

        this._vaultClientCache = new VaultClientCache();
        const { host, port } = this.lcConfig.auth.vault;
        this._vaultClientCache
            .setHost(LIFEYCLE_CONDUCTOR_CLIENT_ID, host)
            .setPort(LIFEYCLE_CONDUCTOR_CLIENT_ID, port);
    }

    _initKafkaBacklogMetrics(cb) {
        this._kafkaBacklogMetrics = new KafkaBacklogMetrics(
            this.zkConfig.connectionString, this.kafkaConfig.backlogMetrics);
        this._kafkaBacklogMetrics.init();
        this._kafkaBacklogMetrics.once('error', cb);
        this._kafkaBacklogMetrics.once('ready', () => {
            // just in case there would be more 'error' events emitted
            this._kafkaBacklogMetrics.removeAllListeners('error');
            this._kafkaBacklogMetrics.on('error', err => {
                this._log.error('error from kafka topic metrics', {
                    error: err.message,
                    method: 'LifecycleConductor._initKafkaBacklogMetrics',
                });
            });
            cb();
        });
    }

    _startCronJob() {
        if (!this._cronJob) {
            this.logger.info('starting bucket queueing cron job',
                             { cronRule: this._cronRule });
            this._cronJob = schedule.scheduleJob(
                this._cronRule,
                () => this.processBuckets(null));
        }
    }

    _stopCronJob() {
        if (this._cronJob) {
            this.logger.info('stopping bucket queueing cron job');
            this._cronJob.cancel();
            this._cronJob = null;
        }
    }

    /**
     * Start the cron job kafka producer
     *
     * @param {function} done - callback
     * @return {undefined}
     */
    start(done) {
        if (this._started) {
            // already started
            return process.nextTick(done);
        }
        this._started = true;
        return this.init(err => {
            if (err) {
                return done(err);
            }
            this._initialized = true;
            this._startCronJob();
            return done();
        });
    }

    /**
     * Stop cron task (if started), stop kafka producer
     *
     * @param {function} done - callback
     * @return {undefined}
     */
    stop(done) {
        this._stopCronJob();
        async.series([
            next => {
                if (!this._producer) {
                    return process.nextTick(next);
                }
                this.logger.debug('closing producer');
                return this._producer.close(() => {
                    this._producer = null;
                    this._zkClient = null;
                    next();
                });
            },
        ], err => {
            this._started = false;
            return done(err);
        });
    }

    isReady() {
        const state = this._zkClient && this._zkClient.getState();
        return this._producer && this._producer.isReady() && state &&
            state.code === zookeeper.State.SYNC_CONNECTED.code &&
            (!this._kafkaBacklogMetrics || this._kafkaBacklogMetrics.isReady());
    }
}

module.exports = LifecycleConductor;
