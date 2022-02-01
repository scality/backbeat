'use strict'; // eslint-disable-line

const http = require('http');
const https = require('https');
const { EventEmitter } = require('events');
const promClient = require('prom-client');

const Logger = require('werelogs').Logger;

const errors = require('arsenal').errors;
const RoundRobin = require('arsenal').network.RoundRobin;

const BackbeatProducer = require('../../../lib/BackbeatProducer');
const BackbeatConsumer = require('../../../lib/BackbeatConsumer');
const VaultClientCache = require('../../../lib/clients/VaultClientCache');
const QueueEntry = require('../../../lib/models/QueueEntry');
const ReplicationTaskScheduler = require('../utils/ReplicationTaskScheduler');
const ReplicateObject = require('../tasks/ReplicateObject');
const MultipleBackendTask = require('../tasks/MultipleBackendTask');
const EchoBucket = require('../tasks/EchoBucket');

const ObjectQueueEntry = require('../utils/ObjectQueueEntry');
const BucketQueueEntry = require('../utils/BucketQueueEntry');
const constants = require('../../../lib/constants');
const { wrapCounterInc, wrapGaugeSet, wrapHistogramObserve } = require('../../../lib/util/metrics');

const {
    proxyVaultPath,
    proxyIAMPath,
} = require('../constants');

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
 * @property {string} [replicationContent] - Data or Metadata
 * @property {string} [replicationStage] - Name of the replication stage
 */

const dataReplicationStatusMetric = new promClient.Counter({
    name: 'replication_data_status_changed_total',
    help: 'Number of status updates',
    labelNames: ['origin', 'containerName', 'replicationStatus'],
});

const metadataReplicationStatusMetric = new promClient.Counter({
    name: 'replication_metadata_status_changed_total',
    help: 'Number of status updates',
    labelNames: ['origin', 'containerName', 'replicationStatus'],
});

const kafkaLagMetric = new promClient.Gauge({
    name: 'kafka_lag',
    help: 'Number of update entries waiting to be consumed from the Kafka topic',
    labelNames: ['origin', 'containerName', 'partition', 'serviceName'],
});

const dataReplicationBytesMetric = new promClient.Counter({
    name: 'replication_data_bytes',
    help: 'Total number of bytes replicated for data operation',
    labelNames: ['origin', 'containerName', 'serviceName'],
});

const metadataReplicationBytesMetric = new promClient.Counter({
    name: 'replication_metadata_bytes',
    help: 'Total number of bytes replicated for metadata operation',
    labelNames: ['origin', 'containerName', 'serviceName'],
});

const sourceDataBytesMetric = new promClient.Counter({
    name: 'replication_source_data_bytes',
    help: 'Total number of data bytes read from replication source',
    labelNames: ['origin', 'containerName', 'serviceName'],
});

const readMetric = new promClient.Counter({
    name: 'replication_data_read',
    help: 'Number of read operations',
    labelNames: ['origin', 'containerName', 'serviceName'],
});

const writeMetric = new promClient.Counter({
    name: 'replication_data_write',
    help: 'Number of write operations',
    labelNames: ['origin', 'containerName', 'serviceName', 'replicationContent'],
});

const timeElapsedMetric = new promClient.Histogram({
    name: 'replication_stage_time_elapsed',
    help: 'Elapsed time of a specific stage in replication',
    labelNames: ['origin', 'containerName', 'serviceName', 'replicationStage'],
});

/**
 * Contains methods to incrememt different metrics
 * @typedef {Object} MetricsHandler
 * @property {CounterInc} dataReplicationStatus - Increments the replication status metric for data operation
 * @property {CounterInc} metadataReplicationStatus - Increments the replication status metric for metadata operation
 * @property {CounterInc} dataReplicationBytes - Increments the replication bytes metric for data operation
 * @property {CounterInc} metadataReplicationBytes - Increments the replication bytes metric for metadata operation
 * @property {CounterInc} sourceDataBytes - Increments the source data bytes metric
 * @property {GaugeSet} lag - Set the kafka lag metric
 * @property {CounterInc} reads - Increments the read metric
 * @property {CounterInc} writes - Increments the write metric
 * @property {HistogramObserve} timeElapsed - Observes the time elapsed metric
 */
const metricsHandler = {
    dataReplicationStatus: wrapCounterInc(dataReplicationStatusMetric),
    metadataReplicationStatus: wrapCounterInc(metadataReplicationStatusMetric),
    dataReplicationBytes: wrapCounterInc(dataReplicationBytesMetric),
    metadataReplicationBytes: wrapCounterInc(metadataReplicationBytesMetric),
    sourceDataBytes: wrapCounterInc(sourceDataBytesMetric),
    lag: wrapGaugeSet(kafkaLagMetric),
    reads: wrapCounterInc(readMetric),
    writes: wrapCounterInc(writeMetric),
    timeElapsed: wrapHistogramObserve(timeElapsedMetric),
};

class QueueProcessor extends EventEmitter {

    /**
     * Create a queue processor object to activate Cross-Region
     * Replication from a kafka topic dedicated to store replication
     * entries to a target S3 endpoint.
     *
     * @constructor
     * @param {string} topic - topic name
     * @param {Object} kafkaConfig - kafka configuration object
     * @param {string} kafkaConfig.hosts - list of kafka brokers
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
     * @param {Object} [httpsConfig] - destination SSL termination
     *   HTTPS configuration object
     * @param {String} [httpsConfig.key] - client private key in PEM format
     * @param {String} [httpsConfig.cert] - client certificate in PEM format
     * @param {String} [httpsConfig.ca] - alternate CA bundle in PEM format
     * @param {Object} [internalHttpsConfig] - internal source HTTPS
     *   configuration object
     * @param {String} [internalHttpsConfig.key] - client private key
     *   in PEM format
     * @param {String} [internalHttpsConfig.cert] - client certificate
     *   in PEM format
     * @param {String} [internalHttpsConfig.ca] - alternate CA bundle
     *   in PEM format
     * @param {String} repConfig.queueProcessor.groupId - kafka
     *   consumer group ID
     * @param {String} repConfig.queueProcessor.retryTimeoutS -
     *   number of seconds before giving up retries of an entry
     *   replication
     * @param {String} site - site name
     * @param {MetricsProducer} mProducer - instance of metrics producer
     */
    constructor(topic, kafkaConfig, sourceConfig, destConfig, repConfig,
        httpsConfig, internalHttpsConfig, site, mProducer) {
        super();
        this.topic = topic;
        this.kafkaConfig = kafkaConfig;
        this.sourceConfig = sourceConfig;
        this.destConfig = destConfig;
        this.repConfig = repConfig;
        this.httpsConfig = httpsConfig;
        this.internalHttpsConfig = internalHttpsConfig;
        this.destHosts = null;
        this.sourceAdminVaultConfigured = false;
        this.destAdminVaultConfigured = false;
        this.replicationStatusProducer = null;
        this._consumer = null;
        this.site = site;
        this._mProducer = mProducer;
        this.serviceName = constants.services.replicationQueueProcessor;

        this.echoMode = false;

        this.logger = new Logger('Backbeat:Replication:QueueProcessor');

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
        if (destConfig.transport === 'https') {
            this.destHTTPAgent = new https.Agent({
                key: httpsConfig.key,
                cert: httpsConfig.cert,
                ca: httpsConfig.ca,
                keepAlive: true,
            });
        } else {
            this.destHTTPAgent = new http.Agent({ keepAlive: true });
        }

        this._setupVaultclientCache();

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
            (ctx, done) => ctx.task.processQueueEntry(
                ctx.entry, ctx.kafkaEntry, done));
    }

    _setupVaultclientCache() {
        this.vaultclientCache = new VaultClientCache();

        if (this.sourceConfig.auth.type === 'role') {
            const { host, port, adminPort, adminCredentials }
                = this.sourceConfig.auth.vault;
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
            if (adminCredentials) {
                this.vaultclientCache
                    .setHost('source:admin', host)
                    .setPort('source:admin', adminPort)
                    .loadAdminCredentials('source:admin',
                        adminCredentials.accessKey,
                        adminCredentials.secretKey);
                if (this.sourceConfig.transport === 'https') {
                    // provision HTTPS credentials for local Vault admin route
                    this.vaultclientCache.setHttps(
                        'source:admin', this.internalHttpsConfig.key,
                        this.internalHttpsConfig.cert,
                        this.internalHttpsConfig.ca);
                }
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
                    if (this.destConfig.transport === 'https') {
                        // provision HTTPS credentials for admin route
                        this.vaultclientCache.setHttps(
                            'dest:admin', this.httpsConfig.key,
                            this.httpsConfig.cert,
                            this.httpsConfig.ca);
                    }
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
            if (this.destConfig.transport === 'https') {
                // provision HTTPS credentials for IAM route
                this.vaultclientCache.setHttps(
                    'dest:s3', this.httpsConfig.key,
                    this.httpsConfig.cert,
                    this.httpsConfig.ca);
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
                {
                    method: 'QueueProcessor.constructor',
                    testMode: true,
                });
        } else {
            this.logger.info('starting in echo mode',
                { method: 'QueueProcessor.constructor' });
        }
        this.echoMode = true;
        this.accountCredsCache = {};
    }

    getStateVars() {
        return {
            sourceConfig: this.sourceConfig,
            destConfig: this.destConfig,
            repConfig: this.repConfig,
            httpsConfig: this.httpsConfig,
            internalHttpsConfig: this.internalHttpsConfig,
            destHosts: this.destHosts,
            sourceHTTPAgent: this.sourceHTTPAgent,
            destHTTPAgent: this.destHTTPAgent,
            vaultclientCache: this.vaultclientCache,
            accountCredsCache: this.accountCredsCache,
            replicationStatusProducer: this.replicationStatusProducer,
            mProducer: this._mProducer,
            site: this.site,
            consumer: this._consumer,
            logger: this.logger,
            metricsHandler,
            serviceName: this.serviceName,
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
     * @return {undefined}
     */
    start(options) {
        this._setupProducer(err => {
            if (err) {
                this.logger.info('error setting up kafka producer',
                    { error: err.message });
                return undefined;
            }
            if (options && options.disableConsumer) {
                this.emit('ready');
                return undefined;
            }
            const groupId =
                `${this.repConfig.queueProcessor.groupId}-${this.site}`;
            this._consumer = new BackbeatConsumer({
                kafka: { hosts: this.kafkaConfig.hosts },
                topic: this.topic,
                groupId,
                concurrency: this.repConfig.queueProcessor.concurrency,
                queueProcessor: this.processKafkaEntry.bind(this),
                logConsumerMetricsIntervalS: this.repConfig.queueProcessor.logConsumerMetricsIntervalS,
            });
            this._consumer.on('error', () => { });
            this._consumer.on('ready', () => {
                this._consumer.subscribe();
                this.logger.info('queue processor is ready to consume ' +
                    `replication entries from ${this.topic}`);
                this.emit('ready');
            });
            return undefined;
        });
    }

    /**
     * Stop kafka producer and consumer and commit current consumer
     * offset
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
        let task;
        if (sourceEntry instanceof BucketQueueEntry) {
            if (this.echoMode) {
                task = new EchoBucket(this);
            }
            // ignore bucket entry if echo mode disabled
        } else if (sourceEntry instanceof ObjectQueueEntry &&
            sourceEntry.getReplicationStorageClass().includes(this.site)) {
            const replicationEndpoint = this.destConfig.bootstrapList
                .find(endpoint => endpoint.site === this.site);
            if (['aws_s3', 'azure'].includes(replicationEndpoint.type)) {
                task = new MultipleBackendTask(this);
            } else {
                task = new ReplicateObject(this);
            }
        }
        if (task) {
            return this.taskScheduler.push({
                task,
                entry: sourceEntry,
                kafkaEntry,
            }, sourceEntry.getCanonicalKey(), done);
        }
        this.logger.debug('skip source entry',
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
        if (this.replicationStatusProducer === undefined ||
            this.replicationStatusProducer === null) {
            verboseLiveness.replicationStatusProducer = constants.statusUndefined;
            responses.push({
                component: 'Replication Status Producer',
                status: constants.statusUndefined,
                site: this.site,
            });
        } else if (!this.replicationStatusProducer.isReady()) {
            verboseLiveness.replicationStatusProducer = constants.statusNotReady;
            responses.push({
                component: 'Replication Status Producer',
                status: constants.statusNotReady,
                site: this.site,
            });
        } else {
            verboseLiveness.replicationStatusProducer = constants.statusReady;
        }

        if (this._consumer === undefined || this._consumer === null) {
            verboseLiveness.consumer = constants.statusUndefined;
            responses.push({
                component: 'Consumer',
                status: constants.statusUndefined,
                site: this.site,
            });
        } else if (!this._consumer.isReady()) {
            verboseLiveness.consumer = constants.statusNotReady;
            responses.push({
                component: 'Consumer',
                status: constants.statusNotReady,
                site: this.site,
            });
        } else {
            verboseLiveness.consumer = constants.statusReady;
        }

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

        // consumer stats lag is on a different update cycle so we need to
        // update the metrics when requested
        const lagStats = this._consumer.consumerStats.lag;
        Object.keys(lagStats).forEach(partition => {
            metricsHandler.lag({
                partition,
                serviceName: this.serviceName,
            }, lagStats[partition]);
        });

        res.writeHead(200, {
            'Content-Type': promClient.register.contentType,
        });
        res.end(promClient.register.metrics());
    }
}

module.exports = QueueProcessor;
