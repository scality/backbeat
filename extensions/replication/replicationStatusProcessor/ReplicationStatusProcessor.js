'use strict'; // eslint-disable-line

const http = require('http');
const async = require('async');
const https = require('https');

const Logger = require('werelogs').Logger;
const errors = require('arsenal').errors;
const { StatsModel } = require('arsenal').metrics;

const BackbeatConsumer = require('../../../lib/BackbeatConsumer');
const GarbageCollectorProducer = require('../../gc/GarbageCollectorProducer');
const VaultClientCache = require('../../../lib/clients/VaultClientCache');
const TaskScheduler = require('../../../lib/tasks/TaskScheduler');
const UpdateReplicationStatus = require('../tasks/UpdateReplicationStatus');
const QueueEntry = require('../../../lib/models/QueueEntry');
const ObjectQueueEntry = require('../../../lib/models/ObjectQueueEntry');
const FailedCRRProducer = require('../failedCRR/FailedCRRProducer');
const {
    getSortedSetMember,
    getSortedSetKey,
} = require('../../../lib/util/sortedSetHelper');
const MetricsProducer = require('../../../lib/MetricsProducer');

// StatsClient constant default for site metrics
const INTERVAL = 300; // 5 minutes;

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
     * @param {String} repConfig.replicationStatusProcessor - config object
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
     */
    constructor(kafkaConfig, sourceConfig, repConfig,
                internalHttpsConfig, mConfig) {
        this.kafkaConfig = kafkaConfig;
        this.sourceConfig = sourceConfig;
        this.repConfig = repConfig;
        this.internalHttpsConfig = internalHttpsConfig;
        this.mConfig = mConfig;
        this._consumer = null;
        this._gcProducer = null;
        this._mProducer = null;

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

        const { monitorReplicationFailureExpiryTimeS } = this.repConfig;
        this._statsClient = new StatsModel(undefined, INTERVAL,
            (monitorReplicationFailureExpiryTimeS + INTERVAL));
        // Serialize updates to the same master key
        this.taskScheduler = new TaskScheduler(
            (ctx, done) => ctx.task.processQueueEntry(ctx.entry, done),
            ctx => ctx.entry.getCanonicalKey());
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

    getStateVars() {
        return {
            sourceConfig: this.sourceConfig,
            repConfig: this.repConfig,
            internalHttpsConfig: this.internalHttpsConfig,
            sourceHTTPAgent: this.sourceHTTPAgent,
            vaultclientCache: this.vaultclientCache,
            gcProducer: this._gcProducer,
            mProducer: this._mProducer,
            logger: this.logger,
        };
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
                this._failedCRRProducer = new FailedCRRProducer();
                this._failedCRRProducer.setupProducer(done);
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
            done => {
                let consumerReady = false;
                this._consumer = new BackbeatConsumer({
                    kafka: { hosts: this.kafkaConfig.hosts },
                    topic: this.repConfig.replicationStatusTopic,
                    groupId: this.repConfig.replicationStatusProcessor.groupId,
                    concurrency:
                    this.repConfig.replicationStatusProcessor.concurrency,
                    queueProcessor: this.processKafkaEntry.bind(this),
                    bootstrap: options && options.bootstrap,
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
        if (!this._consumer) {
            return setImmediate(done);
        }
        return this._consumer.close(done);
    }

    /**
     * Push any failed entry to the "failed" topic.
     * @param {QueueEntry} queueEntry - The queue entry with the failed status.
     * @param {Function} cb - The callback function
     * @return {undefined}
     */
    _pushFailedEntry(queueEntry, cb) {
        const { status, backends } = queueEntry.getReplicationInfo();
        if (status !== 'FAILED') {
            return process.nextTick(cb);
        }
        const backend = backends.find(b =>
            b.status === 'FAILED' && b.site === queueEntry.getSite());
        if (backend) {
            const bucket = queueEntry.getBucket();
            const objectKey = queueEntry.getObjectKey();
            const versionId = queueEntry.getEncodedVersionId();
            const score = Date.now();
            const { site } = backend;
            const latestHour = this._statsClient.getSortedSetCurrentHour(score);
            const message = {
                key: getSortedSetKey(site, latestHour),
                member: getSortedSetMember(bucket, objectKey, versionId),
                score,
            };
            return this._failedCRRProducer
                .publishFailedCRREntry(JSON.stringify(message), cb);
        }
        return cb();
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
            task = new UpdateReplicationStatus(this);
        }
        if (task && this.repConfig.monitorReplicationFailures) {
            return async.parallel([
                next => this._pushFailedEntry(sourceEntry, next),
                next => this.taskScheduler.push({ task, entry: sourceEntry },
                    next),
            ], done);
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
}

module.exports = ReplicationStatusProcessor;
