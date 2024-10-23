'use strict'; // eslint-disable-line

const async = require('async');
const schedule = require('node-schedule');
const zookeeper = require('node-zookeeper-client');

const { constants, errors } = require('arsenal');
const Logger = require('werelogs').Logger;
const BucketClient = require('bucketclient').RESTClient;
const MongoClient = require('arsenal').storage
    .metadata.mongoclient.MongoClientInterface;

const BackbeatProducer = require('../../../lib/BackbeatProducer');
const BackbeatTask = require('../../../lib/tasks/BackbeatTask');
const ZookeeperManager = require('../../../lib/clients/ZookeeperManager');
const KafkaBacklogMetrics = require('../../../lib/KafkaBacklogMetrics');
const safeJsonParse = require('../util/safeJsonParse');
const { AccountIdCache } = require('../../utils/AccountIdCache');
const VaultClientWrapper = require('../../../extensions/utils/VaultClientWrapper');
const ClientManager = require('../../../lib/clients/ClientManager');
const {
    indexesForFeature,
    lifecycleTaskVersions,
} = require('../../../lib/constants');

const { LifecycleMetrics } = require('../LifecycleMetrics');
const { BreakerState, CircuitBreaker } = require('breakbeat').CircuitBreaker;
const {
    startCircuitBreakerMetricsExport,
    updateCircuitBreakerConfigForImplicitOutputQueue,
} = require('../../../lib/CircuitBreaker');

const DEFAULT_CRON_RULE = '* * * * *';
const DEFAULT_CONCURRENCY = 10;
const BUCKET_CHECKPOINT_PUSH_NUMBER = 50;
const BUCKET_CHECKPOINT_PUSH_NUMBER_BUCKETD = 50;
const ACCOUNT_SPLITTER = ':';

const LIFEYCLE_CONDUCTOR_CLIENT_ID = 'lifecycle-conductor';

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
     * @param {Object} s3Config - s3 configuration object
     * @param {String} [transport="http"] - transport method ("http"
     */
    constructor(zkConfig, kafkaConfig, lcConfig, repConfig, s3Config, transport = 'http') {
        this.zkConfig = zkConfig;
        this.kafkaConfig = kafkaConfig;
        this.lcConfig = lcConfig;
        this._authConfig = lcConfig.conductor.auth || lcConfig.auth;
        this._transport = transport;
        this.repConfig = repConfig;
        this.s3Config = s3Config;
        this._cronRule = this.lcConfig.conductor.cronRule || DEFAULT_CRON_RULE;
        this._concurrency =
            this.lcConfig.conductor.concurrency || DEFAULT_CONCURRENCY;
        this._maxInFlightBatchSize = this._concurrency * 2;
        this._bucketSource = this.lcConfig.conductor.bucketSource;
        this._bucketdConfig = this.lcConfig.conductor.bucketd;

        this._producer = null;
        this._zkClient = null;
        this._bucketClient = null;
        this._mongodbClient = null;
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

        const blacklist = (this.lcConfig.conductor.filter && this.lcConfig.conductor.filter.deny) || {};
        this.bucketsBlacklisted = new Set(blacklist.buckets);
        const accountCanonicalIds = this._getAccountCanonicalIds(blacklist.accounts);
        this.accountsBlacklisted = new Set(accountCanonicalIds);
        this.onlyBlacklistAccounts = this.bucketsBlacklisted.size === 0 && this.accountsBlacklisted.size > 0;
        this._vaultAdmin = this.lcConfig.conductor.vaultAdmin || this.lcConfig.auth.vault; 

        this.logger = new Logger('Backbeat:Lifecycle:Conductor');
        this.vaultClientWrapper = new VaultClientWrapper(
            LIFEYCLE_CONDUCTOR_CLIENT_ID,
            this._vaultAdmin,
            this._authConfig,
            this.logger,
        );

        this.clientManager = new ClientManager({
            id: 'lifecycle',
            authConfig: this._authConfig,
            s3Config: this.s3Config,
            transport,
        }, this.logger);

        this.activeIndexingJobsRetrieved = false;
        this.activeIndexingJobs = [];

        const circuitBreakerConfig = updateCircuitBreakerConfigForImplicitOutputQueue(
            lcConfig.conductor.circuitBreaker,
            lcConfig.bucketProcessor.groupId,
            lcConfig.bucketTasksTopic,
        );

        this._circuitBreaker = this.buildCircuitBreaker(circuitBreakerConfig);
    }

    buildCircuitBreaker(conf) {
        try {
            return new CircuitBreaker(conf);
        } catch (e) {
            this.logger.error('invalid circuit breaker configuration');
            throw e;
        }
    }

    /**
     * Extract account canonical ids from configuration filter accounts
     *
     * @param {array} accounts from filter config -
     * format: [account1:eb288756448dc58f61482903131e7ae533553d20b52b0e2ef80235599a1b9143]
     * @return {array} account canonical ids
     */
    _getAccountCanonicalIds(accounts) {
        if (!accounts) {
            return [];
        }
        return accounts.reduce((store, account) => {
            const split = account.split(ACCOUNT_SPLITTER);
            if (split.length === 2) {
                store.push(split[1]);
            }
            return store;
        }, []);
    }

    _isBlacklisted(canonicalId, bucketName) {
        return this.bucketsBlacklisted.has(bucketName) || this.accountsBlacklisted.has(canonicalId);
    }

    getBucketsZkPath() {
        return `${this.lcConfig.zookeeperPath}/data/buckets`;
    }

    getBucketProgressZkPath() {
        return `${this.lcConfig.zookeeperPath}/data/bucket-send-progress`;
    }

    initZkPaths(cb) {
        async.each(
            [
                this.getBucketsZkPath(),
                this.getBucketProgressZkPath(),
            ],
            (path, done) => this._zkClient.mkdirp(path, done),
            cb,
        );
    }

    _indexesGetInProgressJobs(log, cb) {
        if (this._bucketSource !== 'mongodb' || this.lcConfig.forceLegacyListing) {
            return process.nextTick(cb, null);
        }

        log.debug('retrieving mongodb in progress indexing jobs');
        return this._mongodbClient.getIndexingJobs(log, (err, jobs) => {
            if (err) {
                log.debug('failed to retrive mongodb in progress indexing jobs', {
                    method: '_indexesGetInProgressJobs',
                    error: err,
                });
                this.activeIndexingJobsRetrieved = false;
                LifecycleMetrics.onActiveIndexingJobsFailed(log);
                return cb(err);
            }
            this.activeIndexingJobsRetrieved = true;
            this.activeIndexingJobs = jobs;
            LifecycleMetrics.onActiveIndexingJobs(log, jobs.length);
            return cb(null);
        });
    }

    _indexesGetOrCreate(task, log, cb) {
        if (this._bucketSource !== 'mongodb' || !task.isLifecycled) {
            return process.nextTick(cb, null, lifecycleTaskVersions.v1);
        }

        const backbeatMetadataProxy = this.clientManager.getBackbeatMetadataProxy(
            this._accountIdCache.get(task.canonicalId));
        if (!backbeatMetadataProxy) {
            log.error('failed to obtain a backbeat client; skipping index check');
            LifecycleMetrics.onLegacyTask(log, 'getMetadataProxyFailed');
            return process.nextTick(cb, null, lifecycleTaskVersions.v1);
        }

        return backbeatMetadataProxy.getBucketIndexes(task.bucketName, log, (err, bucketIndexes) => {
            if (err) {
                log.warn('unable to retrieve indexing info', {
                    bucket: task.bucketName,
                    error: err,
                });

                LifecycleMetrics.onLegacyTask(log, 'getBucketIndexesFailed');
                return cb(null, lifecycleTaskVersions.v1);
            }

            const isV2 = indexesForFeature.lifecycle.v2.every(
                idx => bucketIndexes.some(bidx => bidx.name === idx.name)
            );

            if (isV2) {
                return cb(null, lifecycleTaskVersions.v2);
            }

            if (!this.lcConfig.autoCreateIndexes) {
                log.trace('skipping index creation: auto creation of indexes disabled');
                LifecycleMetrics.onLegacyTask(log, 'noIndex');
                return cb(null, lifecycleTaskVersions.v1);
            }

            if (!this.activeIndexingJobsRetrieved) {
                log.debug('skipping index creation: unable to retrieve in progress indexing jobs');
                LifecycleMetrics.onLegacyTask(log, 'getIndexingJobsFailed');
                return cb(null, lifecycleTaskVersions.v1);
            }

            if (this.activeIndexingJobs.some(j => (j.bucket === task.bucketName))) {
                log.debug(`skipping index creation: indexing job for ${task.bucketName} in progress`);
                LifecycleMetrics.onLegacyTask(log, 'indexingInProgress');
                return cb(null, lifecycleTaskVersions.v1);
            }

            if (this.activeIndexingJobs.length >= this.lcConfig.conductor.concurrentIndexesBuildLimit) {
                log.debug('skipping index creation: at maximum number of concurrent indexing jobs');
                LifecycleMetrics.onLegacyTask(log, 'maxConcurrentIndexingJobs');
                return cb(null, lifecycleTaskVersions.v1);
            }

            LifecycleMetrics.onLegacyTask(log, 'putBucketIndexes');

            this.activeIndexingJobs.push({
                bucket: task.bucketName,
                indexes: indexesForFeature.lifecycle.v2,
            });

            return backbeatMetadataProxy.putBucketIndexes(
                task.bucketName,
                indexesForFeature.lifecycle.v2,
                log,
                err => {
                    if (err) {
                        log.warn('unable to create lifecycle indexes', {
                            bucket: task.bucketName,
                            error: err,
                        });
                    }
                    return cb(null, lifecycleTaskVersions.v1);
                });
        });
    }

    _taskToMessage(task, taskVersion, log) {
        return {
            message: JSON.stringify({
                action: 'processObjects',
                contextInfo: {
                    reqId: log.getSerializedUids(),
                },
                target: {
                    bucket: task.bucketName,
                    owner: task.canonicalId,
                    accountId: this._accountIdCache.get(task.canonicalId),
                    taskVersion,
                },
                details: {},
            }),
        };
    }

    _getAccountIds(unknownCanonicalIds, log, cb) {
        async.eachLimit(
            unknownCanonicalIds,
            10,
            (canonicalId, done) => {
                this.vaultClientWrapper.getAccountIds([canonicalId], (err, accountIds) => {
                    // TODO: BB-344 fixes me
                    // LifecycleMetrics.onVaultRequest(this.logger, 'getAccountIds', err);
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
            cb);
    }

    _createBucketTaskMessages(tasks, log, cb) {
        if (this.lcConfig.forceLegacyListing) {
            return process.nextTick(cb, null, tasks.map(t => this._taskToMessage(t, lifecycleTaskVersions.v1, log)));
        }

        return async.mapLimit(tasks, 10, (t, taskDone) =>
            this._indexesGetOrCreate(t, log, (err, taskVersion) => {
                if (err) {
                    // should not happen as indexes methods would
                    // ignore the errors and fallback to v1 listing
                    return taskDone(null, this._taskToMessage(t, lifecycleTaskVersions.v1, log));
                }
                return taskDone(null, this._taskToMessage(t, taskVersion, log));
            }), cb);
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

            const unknownCanonicalIds = new Set(
                tasks
                    .map(t => t.canonicalId)
                    .filter(c => !this._accountIdCache.isKnown(c))
            );

            return async.waterfall([
                // ignore error, it has been reported before and should not stop the whole cargo
                next => this._getAccountIds(unknownCanonicalIds, log,
                    () => next(null, tasks.filter(t => {
                        if (!this._accountIdCache.has(t.canonicalId)) {
                            log.warn('skipping bucket, unknown canonical id', {
                                bucketName: t.bucketName,
                                canonicalId: t.canonicalId,
                            });
                            return false;
                        }
                        return true;
                    }))),
                (tasksWithAccountId, next) => this._createBucketTaskMessages(tasksWithAccountId, log, next),
            ],
            (err, messages) => {
                nBucketsQueued += tasks.length;

                log.info('bucket push progress', {
                    nBucketsQueued,
                    bucketsInCargo: tasks.length,
                    kafkaBucketMessagesDeliveryReports: messageDeliveryReports,
                    kafkaEnqueueRateHz: Math.round(nBucketsQueued * 1000 / (new Date() - start)),
                });

                this._accountIdCache.expireOldest();

                return this._producer.send(messages, (err, res) => {
                    messageDeliveryReports += messages.length;
                    LifecycleMetrics.onKafkaPublish(log, 'BucketTopic', 'conductor', err, messages.length);
                    done(err, res);
                });
            });
        });

        async.waterfall([
            next => this._controlBacklog(next),
            // error retrieving in progress jobs should not stop the current batch
            // fallback to V1 listings
            next => this._indexesGetInProgressJobs(log, () => next(null)),
            next => {
                this._batchInProgress = true;
                log.info('starting new lifecycle batch', { bucketSource: this._bucketSource });
                this.listBuckets(messageSendQueue, log, (err, nBucketsListed) => {
                    LifecycleMetrics.onBucketListing(log, err);
                    return next(err, nBucketsListed);
                });
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

            this.activeIndexingJobsRetrieved = false;
            this.activeIndexingJobs = [];
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
            LifecycleMetrics.onProcessBuckets(log);
            if (cb) {
                cb(null, nBucketsQueued);
            }
        });
    }

    listBuckets(queue, log, cb) {
        if (this._bucketSource === 'zookeeper') {
            return this.listZookeeperBuckets(queue, log, cb);
        }

        if (this._bucketSource === 'mongodb') {
            return this.listMongodbBuckets(queue, log, cb);
        }

        return this.restoreBucketCheckpoint((err, marker) => {
            if (err) {
                return cb(err);
            }

            return this.listBucketdBuckets(queue, marker || null, log, cb);
        });
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

                const batch = buckets
                    .filter(bucket => {
                        const [canonicalId, bucketUID, bucketName] =
                                bucket.split(':');
                        if (!canonicalId || !bucketUID || !bucketName) {
                            log.error(
                                'malformed zookeeper bucket entry, skipping',
                                { zkPath: zkBucketsPath, bucket });
                            return false;
                        }

                        if (this._isBlacklisted(canonicalId, bucketName)) {
                            return false;
                        }

                        return true;
                    })
                    .map(bucket => {
                        const split = bucket.split(':');
                        const canonicalId = split[0];
                        const bucketName = split[2];

                        return { canonicalId, bucketName };
                    });

                queue.push(batch);
                return process.nextTick(cb, null, batch.length);
            });
    }

    checkpointBucket(bucketEntry, cb) {
        if (bucketEntry === null) {
            return process.nextTick(cb);
        }

        return this._zkClient.setData(
            this.getBucketProgressZkPath(),
            Buffer.from(bucketEntry),
            this.lastSentVersion,
            (err, stat) => {
                if (err) {
                    return cb(err);
                }

                if (stat) {
                    this.lastSentVersion = stat.version;
                }

                this.lastSentId = null;

                return cb();
            },
        );
    }

    restoreBucketCheckpoint(cb) {
        this._zkClient.getData(this.getBucketProgressZkPath(), (err, data, stat) => {
            if (err) {
                return cb(err);
            }

            const entry = data ? data.toString('ascii') : null;
            if (stat) {
                this.lastSentVersion = stat.version;
            }

            return cb(null, entry);
        });
    }

    listBucketdBuckets(queue, initMarker, log, cb) {
        let isTruncated = true;
        let marker = initMarker;
        let nEnqueued = 0;
        const start = new Date();
        const retryWrapper = new BackbeatTask();

        this.lastSentId = null;
        this.lastSentVersion = -1;

        async.doWhilst(
            next => {
                const breakerState = this._circuitBreaker.state;
                const queueInfo = {
                    nEnqueuedToDownstream: nEnqueued,
                    inFlight: queue.length(),
                    maxInFlight: this._maxInFlightBatchSize,
                    bucketListingPushRateHz: Math.round(nEnqueued * 1000 / (new Date() - start)),
                    breakerState,
                };

                if (queue.length() > this._maxInFlightBatchSize ||
                    breakerState !== BreakerState.Nominal) {
                    log.info('delaying bucket pull', queueInfo);
                    return this.checkpointBucket(this.lastSentId, () => {
                        setTimeout(next, 10000);
                    });
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
                                let needCheckpoint = false;

                                result.Contents.forEach(o => {
                                    marker = o.key;
                                    const [canonicalId, bucketName] = marker.split(constants.splitter);
                                    if (!this._isBlacklisted(canonicalId, bucketName)) {
                                        nEnqueued += 1;
                                        queue.push({ canonicalId, bucketName });
                                        this.lastSentId = o.key;
                                    // Optimization:
                                    // If we only blacklist by accounts, and the last bucket is blacklisted
                                    // we can skip listing buckets until the next account.
                                    // To start the next listing after the blacklisted account, we construct
                                    // a marker by appending the blacklisted account with a semicolon character.
                                    // 'canonicalid1;' > 'canonicalid1..|..bucketname1'
                                    } else if (this.onlyBlacklistAccounts) {
                                        marker = `${canonicalId};`;
                                    }
                                    if (nEnqueued % BUCKET_CHECKPOINT_PUSH_NUMBER_BUCKETD === 0) {
                                        needCheckpoint = true;
                                    }
                                });

                                if (needCheckpoint) {
                                    return this.checkpointBucket(marker, done);
                                }

                                return done();
                            }
                        ),
                    shouldRetryFunc: () => true,
                    log,
                }, next);
            },
            () => isTruncated,
            err => {
                if (err) {
                    return cb(err, nEnqueued);
                }

                // clear last seen bucket from zk
                return this.checkpointBucket('', err => {
                    if (err) {
                        return cb(err);
                    }

                    return cb(null, nEnqueued);
                });
            }
        );
    }

    listMongodbBuckets(queue, log, cb) {
        let nEnqueued = 0;
        const start = new Date();

        const lastEntryPath = this.getBucketProgressZkPath();
        let lastSentId = null;
        let lastSentVersion = -1;
        const checkpointBucket = (bucketEntry, cb) => {
            if (bucketEntry === null) {
                return process.nextTick(cb);
            }

            return this._zkClient.setData(
                lastEntryPath,
                Buffer.from(bucketEntry),
                lastSentVersion,
                (err, stat) => {
                    if (err) {
                        return cb(err);
                    }

                    lastSentVersion = stat.version;
                    lastSentId = null;

                    return cb();
                },
            );
        };

        async.waterfall([
            done => this._zkClient.getData(lastEntryPath, done),
            (data, stat, done) => {
                const entry = data?.toString('ascii');
                if (stat) {
                    lastSentVersion = stat.version;
                }

                const filter = {};
                if (entry) {
                    filter._id = { $gt: entry };
                }

                if (this.accountsBlacklisted.size > 0) {
                    filter['value.owner'] = { $nin: Array.from(this.accountsBlacklisted) };
                }

                if (this.bucketsBlacklisted.size > 0) {
                    filter._id = {
                        ...filter._id,
                        $nin: Array.from(this.bucketsBlacklisted),
                   };
                }

                const cursor = this._mongodbClient
                    .getCollection('__metastore')
                    .find(filter)
                    .project({ '_id': 1, 'value.owner': 1, 'value.lifecycleConfiguration': 1 });

                return done(null, cursor);
            },
            (cursor, done) => {
                async.during(
                    test => process.nextTick(
                        () => cursor.hasNext()
                        .then(res => test(null, res))
                        .catch(test)),
                    next => {
                        const breakerState = this._circuitBreaker.state;
                        const queueInfo = {
                            nEnqueuedToDownstream: nEnqueued,
                            inFlight: queue.length(),
                            maxInFlight: this._maxInFlightBatchSize,
                            enqueueRateHz: Math.round(nEnqueued * 1000 / (new Date() - start)),
                            breakerState,
                        };

                        if (queue.length() > this._maxInFlightBatchSize ||
                            breakerState !== BreakerState.Nominal) {
                            log.info('delaying bucket pull', queueInfo);
                            return checkpointBucket(lastSentId, () => {
                                setTimeout(next, 10000);
                            });
                        }

                        return cursor.next()
                            .then(doc => {
                                // reverse-lookup the name in case it is special and has been
                                // rewritten by the client
                                const name = this._mongodbClient.getCollection(doc._id).collectionName;
                                if (!this._mongodbClient._isSpecialCollection(name)) {
                                    queue.push({
                                        bucketName: name,
                                        canonicalId: doc.value.owner,
                                        isLifecycled: !!doc.value.lifecycleConfiguration,
                                    });
                                    nEnqueued += 1;
                                    lastSentId = doc._id;

                                    if (nEnqueued % BUCKET_CHECKPOINT_PUSH_NUMBER === 0) {
                                        return checkpointBucket(doc._id, next);
                                    }
                                    // fallthrough
                                }
                                return next();
                            })
                            .catch(err => {
                                log.info('error listing next bucket', {
                                    ...queueInfo,
                                    error: err,
                                });

                                return setTimeout(next, 500);
                            });
                    },
                    err => {
                        if (!err) {
                            // clear last seen bucket from zk
                            return checkpointBucket('', err => {
                                if (err) {
                                    return done(err);
                                }

                                return done(null, nEnqueued);
                            });
                        }

                        return done(err);
                    }
                );
            }
        ], cb);
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

        // Check that previous lifecycle batch has completely been
        // processed from all topics before starting a new one.
        //
        // We want to ensure that the _listing_ is complete, and that
        // all object states have been updated accordingly, to avoid
        // duplicated work - even though the triggered transition or
        // expiration may not have terminated.
        return async.series({
            // check that bucket tasks topic consumer lag is 0 (no
            // need to allow any lag because the conductor expects
            // everything to be processed in a single cycle before
            // starting another one, so pass maxLag=0)
            bucketTasksCheck:
            next => this._kafkaBacklogMetrics.checkConsumerLag(
                this.lcConfig.bucketTasksTopic,
                this.lcConfig.bucketProcessor.groupId, 0, next),

            // check that object tasks topic consumer lag for
            // expirations is 0. We don't need to check the
            // transition processor, which is used to process
            // events which are not related to the listing.
            objectTasksExpirationCheck:
            next => this._kafkaBacklogMetrics.checkConsumerLag(
                this.lcConfig.objectTasksTopic,
                this.lcConfig.objectProcessor.groupId, 0, next),

            // Check that data mover topic consumer has progressed
            // beyond the latest lifecycle snapshot of topic offsets,
            // which means everything from the latest lifecycle batch
            // has been consumed.
            // We don't need to check the cold-archive topics, since
            // the bucket-processor marks the objects as transition
            // in-progress immediately.
            dataMoverCheck:
            next => this._kafkaBacklogMetrics.checkConsumerProgress(
                this.repConfig.dataMoverTopic, null, 'lifecycle', next),
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
            maxRequestSize: this.kafkaConfig.maxRequestSize,
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

    _setupMongodbClient(cb) {
        if (this._bucketSource === 'mongodb') {
            const conf = this.lcConfig.conductor.mongodb;
            conf.logger = this.logger;
            this._mongodbClient = new MongoClient(conf);
            return this._mongodbClient.setup(cb);
        }

        return process.nextTick(cb);
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
        if (!this.needsZookeeper()) {
            process.nextTick(cb);
            return;
        }
        this._zkClient = new ZookeeperManager(this.zkConfig.connectionString, this.zkConfig, this.logger);
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
            this.initZkPaths(cb);
        });
    }

    needsZookeeper() {
        return this._bucketSource === 'zookeeper' || // bucket list stored in zk
            this._bucketSource === 'mongodb' || // bucket stream checkpoints in zk
            this._bucketSource === 'bucketd'; // bucket stream checkpoints in zk
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

        this._circuitBreaker.start();
        startCircuitBreakerMetricsExport(this._circuitBreaker, 'lifecycle_conductor');

        this.vaultClientWrapper.init();
        this.clientManager.initSTSConfig();
        this.clientManager.initCredentialsManager();
        return async.series([
            next => this._setupProducer(next),
            next => this._setupZookeeperClient(next),
            next => this._setupBucketdClient(next),
            next => this._setupMongodbClient(next),
            next => this._initKafkaBacklogMetrics(next),
        ], done);
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
                this.logger.error('error from kafka topic metrics', {
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
            this._circuitBreaker.stop();
            this._started = false;
            return done(err);
        });
    }

    isReady() {
        const zkState = !this.needsZookeeper() ||
            !!(this._zkClient && this._zkClient.getState());

        const zkConnectState = !this.needsZookeeper() ||
            (zkState && this._zkClient.getState().code === zookeeper.State.SYNC_CONNECTED.code);

        const components = {
            zkState,
            zkConnectState,
            producer: !!(this._producer && this._producer.isReady()),
            kafkaBacklogMetrics: (!this._kafkaBacklogMetrics || this._kafkaBacklogMetrics.isReady()),
            tempCreds: this.vaultClientWrapper.tempCredentialsReady(),
        };

        const allReady = Object.values(components).every(v => v);

        if (!allReady) {
            this.logger.error('ready state', components);
        }

        return allReady;
    }
}

module.exports = LifecycleConductor;
