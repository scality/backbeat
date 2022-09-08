'use strict'; // eslint-disable-line

const async = require('async');
const http = require('http');
const https = require('https');

const Logger = require('werelogs').Logger;
const errors = require('arsenal').errors;
const { StatsModel } = require('arsenal').metrics;
const { sendSuccess } = require('arsenal').network.probe.Utils;

const BackbeatProducer = require('../../../lib/BackbeatProducer');
const BackbeatConsumer = require('../../../lib/BackbeatConsumer');
const GarbageCollectorProducer = require('../../gc/GarbageCollectorProducer');
const VaultClientCache = require('../../../lib/clients/VaultClientCache');
const TaskScheduler = require('../../../lib/tasks/TaskScheduler');
const UpdateReplicationStatus = require('../tasks/UpdateReplicationStatus');
const QueueEntry = require('../../../lib/models/QueueEntry');
const ObjectQueueEntry = require('../../../lib/models/ObjectQueueEntry');
const FailedCRRProducer = require('../failedCRR/FailedCRRProducer');
const ReplayProducer = require('../replay/ReplayProducer');
const MetricsProducer = require('../../../lib/MetricsProducer');

const NotificationConfigManager = require('../../notification/NotificationConfigManager');

// StatsClient constant default for site metrics
const INTERVAL = 300; // 5 minutes;
const promClient = require('prom-client');
const constants = require('../../../lib/constants');
const {
    wrapCounterInc,
    wrapHistogramObserve,
} = require('../../../lib/util/metrics');

promClient.register.setDefaultLabels({
    origin: 'replication',
});

/**
 * Labels used for Prometheus metrics
 * @typedef {Object} MetricLabels
 * @property {string} origin - Method that began the replication
 * @property {string} [replicationStatus] - Result of the replications status
 * @property {string} [partition] - What kafka partition relates to the metric
 */

/**
 * Contains methods to incrememt different metrics
 * @typedef {Object} ReplicationStatusMetricsHandler
 * @property {CounterInc} status - Increments the replication status metric
 * @property {CounterInc} replayAttempts - Increments the replay attempts metric
 * @property {CounterInc} replaySuccess - Increments the replay success metric
 * @property {CounterInc} replayQueuedObjects - Increments the replay queued objects metric
 * @property {CounterInc} replayQueuedBytes - Increments the replay queued bytes metric
 * @property {CounterInc} replayQueuedFileSizes - Increments the replay queued file sizes metric
 * @property {CounterInc} replayCompletedObjects - Increments the replay completed objects metric
 * @property {CounterInc} replayCompletedBytes - Increments the replay completed bytes metric
 * @property {CounterInc} replayCompletedFileSizes - Increments the replay completed file sizes metric
 */

/**
 * @param {Object} repConfig - Replication configuration
 * @returns {ReplicationStatusMetricsHandler} Metric handlers
 */
function loadMetricHandlers(repConfig) {
    const replicationStatusMetric = new promClient.Counter({
        name: 'replication_status_changed_total',
        help: 'Number of objects updated',
        labelNames: ['origin', 'replicationStatus'],
    });

    const replayAttempts = new promClient.Counter({
        name: 'replication_replay_attempts_total',
        help: 'Number of total attempts made to replay replication',
        labelNames: ['origin', 'location', 'replayCount'],
    });

    const replaySuccess = new promClient.Counter({
        name: 'replication_replay_success_total',
        help: 'Number of times an object was replicated during a replay',
        labelNames: ['origin', 'location', 'replayCount'],
    });

    const replayQueuedObjects = new promClient.Counter({
        name: 'replication_replay_objects_queued_total',
        help: 'Number of objects added to replay queues',
        labelNames: ['origin', 'location', 'replayCount'],
    });

    const replayQueuedBytes = new promClient.Counter({
        name: 'replication_replay_bytes_queued_total',
        help: 'Number of bytes added to replay queues',
        labelNames: ['origin', 'location', 'replayCount'],
    });

    const replayQueuedFileSizes = new promClient.Histogram({
        name: 'replication_replay_file_sizes_queued',
        help: 'Number of objects queued for replay by file size',
        labelNames: ['origin', 'location', 'replayCount'],
        buckets: repConfig.objectSizeMetrics,
    });

    const replayCompletedObjects = new promClient.Counter({
        name: 'replication_replay_objects_completed_total',
        help: 'Number of objects completed from replay queues',
        labelNames: ['origin', 'location', 'replayCount', 'replicationStatus'],
    });

    const replayCompletedBytes = new promClient.Counter({
        name: 'replication_replay_bytes_completed_total',
        help: 'Number of bytes completed from replay queues',
        labelNames: ['origin', 'location', 'replayCount', 'replicationStatus'],
    });

    const replayCompletedFileSizes = new promClient.Histogram({
        name: 'replication_replay_file_sizes_completed',
        help: 'Number of objects completed from replay by file size',
        labelNames: ['origin', 'location', 'replayCount', 'replicationStatus'],
        buckets: repConfig.objectSizeMetrics,
    });

    return {
        status: wrapCounterInc(replicationStatusMetric),
        replayAttempts: wrapCounterInc(replayAttempts),
        replaySuccess: wrapCounterInc(replaySuccess),
        replayQueuedObjects: wrapCounterInc(replayQueuedObjects),
        replayQueuedBytes: wrapCounterInc(replayQueuedBytes),
        replayQueuedFileSizes: wrapHistogramObserve(replayQueuedFileSizes),
        replayCompletedObjects: wrapCounterInc(replayCompletedObjects),
        replayCompletedBytes: wrapCounterInc(replayCompletedBytes),
        replayCompletedFileSizes: wrapHistogramObserve(replayCompletedFileSizes),
    };
}

/**
 * @class ReplicationStatusProcessor
 *
 * @classdesc Background task that processes entries from the
 * replication status kafka queue and updates replication status on
 * source objects.
 */
class ReplicationStatusProcessor {

    /**
     * @constructor
     * @param {Object} kafkaConfig - kafka configuration object
     * @param {string} kafkaConfig.hosts - list of kafka brokers
     *   as "host:port[,host:port...]"
     * @param {Object} sourceConfig - source S3 configuration
     * @param {Object} sourceConfig.s3 - s3 endpoint configuration object
     * @param {Object} sourceConfig.auth - authentication info on source
     * @param {Object} repConfig - replication configuration object
     * @param {String} repConfig.replicationStatusTopic - replication
     *   status topic name
     * @param {Object} repConfig.replicationStatusProcessor - config object
     *   specific to replication status processor
     * @param {String} repConfig.replicationStatusProcessor.groupId - kafka
     *   consumer group ID
     * @param {number} [repConfig.retry.timeoutS] -
     *  retry timeout in secs.
     * @param {number} [repConfig.retry.maxRetries] -
     *  max retries before giving up
     * @param {Object} [repConfig.retry.backoff] -
     *  backoff params
     * @param {number} [repConfig.retry.backoff.min] -
     *  min. backoff in ms.
     * @param {number} [repConfig.retry.backoff.max] -
     *  max. backoff in ms.
     * @param {number} [repConfig.retry.backoff.jitter] -
     *  randomness
     * @param {number} [repConfig.retry.backoff.factor] -
     *  backoff factor
     * @param {Array.<{topicName: String, retries: Number}>}
     * repConfig.replayTopics - array of replay topics
     * @param {Object} [internalHttpsConfig] - internal HTTPS
     *   configuration object
     * @param {String} [internalHttpsConfig.key] - client private key
     *   in PEM format
     * @param {String} [internalHttpsConfig.cert] - client certificate
     *   in PEM format
     * @param {String} [internalHttpsConfig.ca] - alternate CA bundle
     *   in PEM format
     * @param {Object} mConfig - metrics config
     * @param {String} mConfig.topic - metrics config kafka topic
     * @param {Object} bucketNotificationConfig - bucket notification config
     * @param {Object} mongoConfig - mongodb connection config
     */
    constructor(kafkaConfig, sourceConfig, repConfig,
                internalHttpsConfig, mConfig, bucketNotificationConfig,
                mongoConfig) {
        this.kafkaConfig = kafkaConfig;
        this.sourceConfig = sourceConfig;
        this.repConfig = repConfig;
        this.internalHttpsConfig = internalHttpsConfig;
        this.mConfig = mConfig;
        this._consumer = null;
        this._gcProducer = null;
        this._mProducer = null;
        // bucket notification related
        this.bucketNotificationConfig = bucketNotificationConfig;
        this.mongoConfig = mongoConfig;
        this.notificationConfigManager = null;
        this.notificationProducers = {};

        this.logger =
            new Logger('Backbeat:Replication:ReplicationStatusProcessor');

        // global variables
        if (sourceConfig.transport === 'https') {
            this.sourceHTTPAgent = new https.Agent({
                key: internalHttpsConfig.key,
                cert: internalHttpsConfig.cert,
                ca: internalHttpsConfig.ca,
                keepAlive: true,
            });
        } else {
            this.sourceHTTPAgent = new http.Agent({ keepAlive: true });
        }

        this._setupVaultclientCache();
        this.metricsHandlers = loadMetricHandlers(repConfig);

        const { monitorReplicationFailureExpiryTimeS } = this.repConfig;
        this._statsClient = new StatsModel(undefined, INTERVAL,
            (monitorReplicationFailureExpiryTimeS + INTERVAL));
        // Serialize updates to the same master key
        this.taskScheduler = new TaskScheduler(
            (ctx, done) => ctx.task.processQueueEntry(ctx.entry, done),
            ctx => ctx.entry.getCanonicalKey());

        this._ReplayProducers = [];

        this._replayTopics = this._reshapeReplayTopics(repConfig.replayTopics);
        this._replayTopicNames = this._makeTopicNames(repConfig.replayTopics);
    }

    /**
     * Return an array of replay topic's names. Array is reverse sorted
     * because the counter is decremented.
     *
     * @param {Array.<{topicName: String, retries: Number}>} [replayTopics] -
     * array of replay topics
     * @return {array | undefined} [names] - replay topic's names
     * @param {String} names[0] - name of the topic
     * name.length equals topic's names retries
     */
    _reshapeReplayTopics(replayTopics) {
        if (!replayTopics || replayTopics.length === 0) {
            return undefined;
        }
        // duplicate replayTopics array because
        // reverse() is destructive -- it changes the original array
        const r = [...replayTopics];
        return r.reverse().reduce((prev, curr) => {
            for (let i = 0; i < curr.retries; i++) {
                prev.push(curr.topicName);
            }
            return prev;
        }, []);
    }

    /**
     * Return an array of unique replay topic's name
     *
     * @param {array} [replayTopics] - array of replay topics' objects from config
     * @return {array} - array of unique replay topic's name.
     */
    _makeTopicNames(replayTopics) {
        if (!replayTopics || replayTopics.length === 0) {
            return [];
        }
        return replayTopics
            .map(t => t.topicName)
            .filter((item, pos, self) => self.indexOf(item) === pos);
    }

    _setupVaultclientCache() {
        this.vaultclientCache = new VaultClientCache();

        if (this.sourceConfig.auth.type === 'role') {
            const { host, port } = this.sourceConfig.auth.vault;
            this.vaultclientCache
                .setHost('source:s3', host)
                .setPort('source:s3', port);
            if (this.sourceConfig.transport === 'https') {
                // provision HTTPS credentials for local Vault S3 route
                this.vaultclientCache.setHttps(
                    'source:s3', this.internalHttpsConfig.key,
                    this.internalHttpsConfig.cert,
                    this.internalHttpsConfig.ca);
            }
        }
    }

    _producerLiveness(p, componentName, responses) {
        if (p === undefined || p === null ||
            p._producer === undefined ||
            p._producer === null) {
            responses.push({
                component: componentName,
                status: constants.statusUndefined,
            });
            return constants.statusUndefined;
        }
        if (!p._producer.isReady()) {
            responses.push({
                component: componentName,
                status: constants.statusNotReady,
            });
            return constants.statusNotReady;
        }
        return constants.statusReady;
    }

    getStateVars() {
        return {
            sourceConfig: this.sourceConfig,
            repConfig: this.repConfig,
            internalHttpsConfig: this.internalHttpsConfig,
            sourceHTTPAgent: this.sourceHTTPAgent,
            vaultclientCache: this.vaultclientCache,
            gcProducer: this._gcProducer,
            mProducer: this._mProducer,
            statsClient: this._statsClient,
            failedCRRProducer: this._failedCRRProducer,
            replayProducers: this._ReplayProducers,
            logger: this.logger,
            replayTopics: this._replayTopics,
            replayTopicNames: this._replayTopicNames,
            bucketNotificationConfig: this.bucketNotificationConfig,
            notificationConfigManager: this.notificationConfigManager,
            notificationProducers: this.notificationProducers,
        };
    }

    _setupNotificationConfigManager(done) {
        // setup notification configuration manager only if notification
        // extension is available
        if (this.bucketNotificationConfig) {
            try {
                this.notificationConfigManager = new NotificationConfigManager({
                    mongoConfig: this.mongoConfig,
                    logger: this.logger,
                });
                return this.notificationConfigManager.setup(done);
            } catch (err) {
                return done(err);
            }
        }
        return done();
    }

    _setupNotificationProducers(done) {
        // setup notification producers only when
        // extension is available
        if (this.bucketNotificationConfig) {
            // we set one producer per notification target
            return async.each(this.bucketNotificationConfig.destinations,
            (destination, cb) => {
                const internalTopic = destination.internalTopic ||
                    this.bucketNotificationConfig.topic;
                const producer = new BackbeatProducer({
                    kafka: { hosts: this.kafkaConfig.hosts },
                    topic: internalTopic,
                });
                producer.once('error', done);
                producer.once('ready', () => {
                    producer.removeAllListeners('error');
                    producer.on('error', err => {
                        this.logger.error('error setting replication notification producer', {
                            destination: destination.resource,
                            topic: internalTopic,
                            error: err,
                        });
                    });
                    this.notificationProducers[destination.resource] = producer;
                    return cb();
                });
            },
            err => {
                if (err) {
                    this.logger.error('error setting replication notification producers', {
                        error: err,
                    });
                    return done(err);
                }
                return done();
            });
        }
        return done();
    }

    /**
     * Start kafka consumer
     *
     * @param {object} [options] - options object (only used for tests
     * for now)
     * @param {function} [cb] - optional callback called when startup
     * is complete
     * @return {undefined}
     */
    start(options, cb) {
        async.parallel([
            done => {
                this._failedCRRProducer = new FailedCRRProducer(this.kafkaConfig);
                this._replayTopicNames.forEach(t => {
                    this._ReplayProducers[t] = new ReplayProducer(this.kafkaConfig, t);
                });
                this._failedCRRProducer.setupProducer(err => {
                    if (err) {
                        return done(err);
                    }
                    return async.each(this._replayTopicNames, (topicName, next) =>
                        this._ReplayProducers[topicName].setupProducer(next),
                    done);
                });
            },
            done => {
                this._gcProducer = new GarbageCollectorProducer();
                this._gcProducer.setupProducer(done);
            },
            done => {
                this._mProducer = new MetricsProducer(this.kafkaConfig,
                    this.mConfig);
                this._mProducer.setupProducer(done);
            },
            done => this._setupNotificationConfigManager(err => {
                if (err) {
                    this.logger.info('error setting up notification config manager',
                                     { error: err.message });
                    process.exit(1);
                }
                return done();
            }),
            done => this._setupNotificationProducers(err => {
                if (err) {
                    this.logger.info('error setting up kafka notification producers',
                                     { error: err.message });
                    process.exit(1);
                }
                return done();
            }),
            done => {
                let consumerReady = false;
                this._consumer = new BackbeatConsumer({
                    kafka: {
                        hosts: this.kafkaConfig.hosts,
                        site: this.kafkaConfig.site,
                    },
                    topic: this.repConfig.replicationStatusTopic,
                    groupId: this.repConfig.replicationStatusProcessor.groupId,
                    concurrency:
                    this.repConfig.replicationStatusProcessor.concurrency,
                    queueProcessor: this.processKafkaEntry.bind(this),
                    bootstrap: (options && options.bootstrap) || false,
                });
                this._consumer.on('error', () => {
                    if (!consumerReady) {
                        this.logger.fatal('error starting a backbeat consumer');
                        process.exit(1);
                    }
                });
                this._consumer.on('ready', () => {
                    consumerReady = true;
                    this.logger.info('replication status processor is ready ' +
                                     'to consume replication status entries');
                    this._consumer.subscribe();
                    done();
                });
            },
        ], cb);
    }

    /**
     * Stop kafka consumer and commit current offset
     *
     * @param {function} done - callback
     * @return {undefined}
     */
    stop(done) {
        async.series([
            next => {
                if (this._consumer) {
                    this.logger.debug('closing kafka consumer', {
                        method: 'ReplicationStatusProcessor.stop',
                    });
                    return this._consumer.close(next);
                }
                this.logger.debug('no kafka consumer to close', {
                    method: 'ReplicationStatusProcessor.stop',
                });
                return next();
            },
            next => {
                if (this._mProducer) {
                    this.logger.debug('closing metrics producer', {
                        method: 'ReplicationStatusProcessor.stop',
                    });
                    return this._mProducer.close(next);
                }
                this.logger.debug('no metrics producer to close', {
                    method: 'ReplicationStatusProcessor.stop',
                });
                return next();
            }
        ], done);
    }

    /**
     * Proceed with updating the replication status of an object given
     * a kafka replication status queue entry
     *
     * @param {object} kafkaEntry - entry generated by the replication
     *   queue processor
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
        let task;
        if (sourceEntry instanceof ObjectQueueEntry) {
            task = new UpdateReplicationStatus(this, this.metricsHandlers);
        }
        if (task) {
            return this.taskScheduler.push({ task, entry: sourceEntry },
                done);
        }
        this.logger.warn('skipping unknown source entry',
            { entry: sourceEntry.getLogInfo() });
        return process.nextTick(done);
    }

    isReady() {
        return this._consumer && this._consumer.isReady() &&
            this._failedCRRProducer && this._failedCRRProducer.isReady();
    }

    /**
     * Handle ProbeServer liveness check
     *
     * @param {http.HTTPServerResponse} res - HTTP Response to respond with
     * @param {Logger} log - Logger
     * @returns {undefined}
     */
    handleLiveness(res, log) {
        const verboseLiveness = {};
        // track and return all errors in one response
        const responses = [];

        if (this._consumer === undefined || this._consumer === null) {
            verboseLiveness.consumer = constants.statusUndefined;
            responses.push({
                component: 'Consumer',
                status: constants.statusUndefined,
            });
        } else if (!this._consumer._consumerReady) {
            verboseLiveness.consumer = constants.statusNotReady;
            responses.push({
                component: 'Consumer',
                status: constants.statusNotReady,
            });
        } else {
            verboseLiveness.consumer = constants.statusReady;
        }

        verboseLiveness.failedCRRProducer =
            this._producerLiveness(this._failedCRRProducer, 'Failed CRR Producer', responses);

        this._replayTopicNames.forEach(topicName => {
            const componentName = `Replay CRR Producer to ${topicName}`;
            verboseLiveness[`replayCRRProducer-${topicName}`] =
                this._producerLiveness(this._ReplayProducers[topicName], componentName, responses);
        });

        log.debug('verbose liveness', verboseLiveness);

        if (responses.length > 0) {
            const message = JSON.stringify(responses);
            log.debug('service unavailable',
                {
                    httpCode: 500,
                    error: message,
                }
            );
            res.writeHead(500);
            res.end(message);
            return undefined;
        }

        sendSuccess(res, log);
        return undefined;
    }

    /**
     * Handle ProbeServer metrics
     *
     * @param {http.HTTPServerResponse} res - HTTP Response to respond with
     * @param {Logger} log - Logger
     * @returns {undefined}
     */
    handleMetrics(res, log) {
        log.debug('metrics requested');

        res.writeHead(200, {
            'Content-Type': promClient.register.contentType,
        });
        res.end(promClient.register.metrics());
    }

    /**
     * Static trampoline, for use from tests.
     * @param {Object} repConfig - Replication configuration
     * @returns {ReplicationStatusMetricsHandler} Metric handlers
     */
    static loadMetricHandlers(repConfig) {
        return loadMetricHandlers(repConfig);
    }
}

module.exports = ReplicationStatusProcessor;
