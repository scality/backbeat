'use strict'; // eslint-disable-line

const http = require('http');
const https = require('https');
const async = require('async');

const Logger = require('werelogs').Logger;
const errors = require('arsenal').errors;
const { StatsModel } = require('arsenal').metrics;

const BackbeatConsumer = require('../../../lib/BackbeatConsumer');
const VaultClientCache = require('../../../lib/clients/VaultClientCache');
const ReplicationTaskScheduler = require('../utils/ReplicationTaskScheduler');
const UpdateReplicationStatus = require('../tasks/UpdateReplicationStatus');
const QueueEntry = require('../../../lib/models/QueueEntry');
const ObjectQueueEntry = require('../utils/ObjectQueueEntry');
const FailedCRRProducer = require('../failedCRR/FailedCRRProducer');
const ReplayProducer = require('../replay/ReplayProducer');
const promClient = require('prom-client');
const constants = require('../../../lib/constants');
const { wrapCounterInc, wrapGaugeSet } = require('../../../lib/util/metrics');

promClient.register.setDefaultLabels({
    origin: 'replication',
    containerName: process.env.CONTAINER_NAME || '',
});

/**
 * Labels used for Prometheus metrics
 * @typedef {Object} MetricLabels
 * @property {string} origin - Method that began the replication
 * @property {string} containerName - Name of the container running our process
 * @property {string} [replicationStatus] - Result of the replications status
 * @property {string} [partition] - What kafka partition relates to the metric
 * @property {string} [serviceName] - Name of our service to match generic metrics
 */

const replicationStatusMetric = new promClient.Counter({
    name: 'replication_status_changed_total',
    help: 'Number of objects updated',
    labelNames: ['origin', 'containerName', 'replicationStatus'],
});

const kafkaLagMetric = new promClient.Gauge({
    name: 'kafka_lag',
    help: 'Number of update entries waiting to be consumed from the Kafka topic',
    labelNames: ['origin', 'containerName', 'partition', 'serviceName'],
});

/**
 * Contains methods to incrememt different metrics
 * @typedef {Object} MetricsHandler
 * @property {CounterInc} status - Increments the replication status metric
 * @property {GaugeSet} lag - Set the kafka lag metric
 */
const metricsHandler = {
    status: wrapCounterInc(replicationStatusMetric),
    lag: wrapGaugeSet(kafkaLagMetric),
};

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
     * @param {String} repConfig.replicationStatusProcessor.retryTimeoutS -
     *   number of seconds before giving up retries of an entry status
     *   update
     * @param {Object} [internalHttpsConfig] - internal HTTPS
     *   configuration object
     * @param {String} [internalHttpsConfig.key] - client private key
     *   in PEM format
     * @param {String} [internalHttpsConfig.cert] - client certificate
     *   in PEM format
     * @param {String} [internalHttpsConfig.ca] - alternate CA bundle
     *   in PEM format
     */
    constructor(kafkaConfig, sourceConfig, repConfig, internalHttpsConfig) {
        this.kafkaConfig = kafkaConfig;
        this.sourceConfig = sourceConfig;
        this.repConfig = repConfig;
        this.internalHttpsConfig = internalHttpsConfig;
        this._consumer = null;

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

        this._statsClient = new StatsModel(undefined);
        this.taskScheduler = new ReplicationTaskScheduler(
            (ctx, done) => ctx.task.processQueueEntry(ctx.entry, done));

        this._ReplayProducers = [];

        this._replayTopics = this._reshapeReplayTopics(repConfig.replayTopics);
        this._replayTopicNames = this._makeTopicNames(repConfig.replayTopics);
    }

    _reshapeReplayTopics(replayTopics) {
        if (!replayTopics || replayTopics.length === 0) {
            return undefined;
        }
        return replayTopics.reverse().reduce((prev, curr) => {
            for (let i = 0; i < curr.retries; i++) {
                prev.push(curr.topicName);
            }
            return prev;
        }, []);
    }

    _makeTopicNames(replayTopics) {
        return replayTopics
            .map(t => t.topicName)
            .filter((item, pos, self) => self.indexOf(item) === pos) || [];
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
            statsClient: this._statsClient,
            failedCRRProducer: this._FailedCRRProducer,
            replayProducers: this._ReplayProducers,
            logger: this.logger,
            replayTopics: this._replayTopics,
            replayTopicNames: this._replayTopicNames,
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
        this._FailedCRRProducer = new FailedCRRProducer(this.kafkaConfig);
        this._replayTopicNames.forEach(t => {
            this._ReplayProducers[t] = new ReplayProducer(this.kafkaConfig, t);
        });
        this._consumer = new BackbeatConsumer({
            kafka: { hosts: this.kafkaConfig.hosts },
            topic: this.repConfig.replicationStatusTopic,
            groupId: this.repConfig.replicationStatusProcessor.groupId,
            concurrency:
                this.repConfig.replicationStatusProcessor.concurrency,
            queueProcessor: this.processKafkaEntry.bind(this),
            bootstrap: options && options.bootstrap,
            logConsumerMetricsIntervalS: this.repConfig.replicationStatusProcessor.logConsumerMetricsIntervalS,
        });
        this._consumer.on('error', () => { });
        this._consumer.on('ready', () => {
            this.logger.info('replication status processor is ready to ' +
                'consume replication status entries');
            this._consumer.subscribe();
            this._FailedCRRProducer.setupProducer(err => {
                if (err) {
                    return cb(err);
                }
                return async.each(this._replayTopicNames, (topicName, next) =>
                    this._ReplayProducers[topicName].setupProducer(next), cb);
            });
        });
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
            task = new UpdateReplicationStatus(this, metricsHandler);
        }
        if (task) {
            return this.taskScheduler.push({ task, entry: sourceEntry },
                sourceEntry.getCanonicalKey(), done);
        }
        this.logger.warn('skipping unknown source entry',
            { entry: sourceEntry.getLogInfo() });
        return process.nextTick(done);
    }

    /**
     * Handle ProbeServer liveness check
     *
     * @param {http.HTTPServerResponse} res - HTTP Response to respond with
     * @param {Logger} log - Logger
     * @returns {string} Error response string or undefined
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
            this._producerLiveness(this._FailedCRRProducer, 'Failed CRR Producer', responses);

        this._replayTopicNames.forEach(topicName => {
            const componentName = `Replay CRR Producer to ${topicName}`;
            verboseLiveness[`replayCRRProducer-${topicName}`] =
                this._producerLiveness(this._ReplayProducers[topicName], componentName, responses);
        });

        log.debug('verbose liveness', verboseLiveness);

        if (responses.length > 0) {
            return JSON.stringify(responses);
        }

        res.writeHead(200);
        res.end();
        return undefined;
    }

    /**
     * Handle ProbeServer metrics
     *
     * @param {http.HTTPServerResponse} res - HTTP Response to respond with
     * @param {Logger} log - Logger
     * @returns {string} Error response string or undefined
     */
    handleMetrics(res, log) {
        log.debug('metrics requested');

        const serviceName = constants.services.replicationStatusProcessor;

        // consumer stats lag is on a different update cycle so we need to
        // update the metrics when requested
        const lagStats = this._consumer.consumerStats.lag;
        Object.keys(lagStats).forEach(partition => {
            metricsHandler.lag({ partition, serviceName }, lagStats[partition]);
        });

        res.writeHead(200, {
            'Content-Type': promClient.register.contentType,
        });
        res.end(promClient.register.metrics());
    }
}

module.exports = ReplicationStatusProcessor;
