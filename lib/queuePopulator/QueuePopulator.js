const async = require('async');
const Logger = require('werelogs').Logger;
const zookeeper = require('../clients/zookeeper');
const { State: ZKState } = require('node-zookeeper-client');
const ProvisionDispatcher = require('../provisioning/ProvisionDispatcher');
const RaftLogReader = require('./RaftLogReader');
const BucketFileLogReader = require('./BucketFileLogReader');
const MetricsProducer = require('../MetricsProducer');
const MetricsConsumer = require('../MetricsConsumer');
const FailedCRRConsumer =
    require('../../extensions/replication/failedCRR/FailedCRRConsumer');
const NotificationConfigManager
    = require('../../extensions/notification/NotificationConfigManager');
const promClient = require('prom-client');
const metricLabels = ['origin', 'logName', 'logId', 'containerName'];
promClient.register.setDefaultLabels({
    origin: 'replication',
    containerName: process.env.CONTAINER_NAME || '',
});

/**
 * Labels used for Prometheus metrics
 * @typedef {Object} MetricLabels
 * @property {string} logName - Name of the log we are using
 * @property {string} logId - Id of the log we are reading
 * @property {string} containerName - Name of the container running our process
 * @property {string} [publishStatus] - Result of the publishing to kafka to the topic
 */

/**
 * Increment a Prometheus counter metric
 * @typedef { (labels: MetricLabels, value?: number) => void } CounterInc
 */

/**
 * Set a Prometheus gauge metric
 * @typedef { (labels: MetricLabels, value: number) => void } GaugeSet
 */

/**
 * WrapCounterInc wraps the Counters Inc method adding the metric labels used.
 *
 * @param {promClient.Counter} metric - Prom counter metric
 * @returns {CounterInc} - Function used to increment counter
 */
function wrapCounterInc(metric) {
    return (labels, value) => {
        metric.inc(labels, value);
    };
}

/**
 * Wrap Gauge Set wraps a Prometheus Guage's set method
 *
 * @param {promClient.Gauge} metric - Prom gauge metric
 * @returns {GaugeSet} - Function used to set gauge
 */
function wrapGaugeSet(metric) {
    return (labels, value) => {
        metric.set(labels, value);
    };
}

const logReadOffsetMetric = new promClient.Gauge({
    name: 'replication_read_offset',
    help: 'Current read offset of metadata journal',
    labelNames: metricLabels,
});

const logSizeMetric = new promClient.Gauge({
    name: 'replication_log_size',
    help: 'Current size of metadata journal',
    labelNames: metricLabels,
});

const messageMetrics = new promClient.Counter({
    name: 'replication_populator_messages',
    help: 'Total number of Kafka messages produced by the queue populator',
    labelNames: ['origin', 'logName', 'logId', 'containerName', 'publishStatus'],
});

const objectMetrics = new promClient.Counter({
    name: 'replication_populator_objects',
    help: 'Total objects queued for replication',
    labelNames: metricLabels,
});

const byteMetrics = new promClient.Counter({
    name: 'replication_populator_bytes',
    help: 'Total number of bytes queued for replication not including metadata',
    labelNames: metricLabels,
});

/**
 * Contains methods to incrememt different metrics
 * @typedef {Object} MetricsHandler
 * @property {CounterInc} messages - Increments the message metric
 * @property {CounterInc} objects - Increments the objects metric
 * @property {CounterInc} bytes - Increments the bytes metric
 * @property {GaugeSet} logReadOffset - Set the log read offset metric
 * @property {GaugeSet} logSize - Set the log size metric
 */
const metricsHandler = {
    messages: wrapCounterInc(messageMetrics),
    objects: wrapCounterInc(objectMetrics),
    bytes: wrapCounterInc(byteMetrics),
    logReadOffset: wrapGaugeSet(logReadOffsetMetric),
    logSize: wrapGaugeSet(logSizeMetric),
};

class QueuePopulator {
    /**
     * Create a queue populator object to populate various kafka
     * queues from the metadata log
     *
     * @constructor
     * @param {Object} zkConfig - zookeeper configuration object
     * @param {String} zkConfig.connectionString - zookeeper
     *   connection string as "host:port[/chroot]"
     * @param {Object} kafkaConfig - kafka configuration object
     * @param {string} kafkaConfig.hosts - kafka hosts list
     * as "host:port[,host:port...]"
     * @param {Object} qpConfig - queue populator configuration
     * @param {String} qpConfig.zookeeperPath - sub-path to use for
     *   storing populator state in zookeeper
     * @param {String} qpConfig.logSource - type of source
     *   log: "bucketd" (raft log) or "dmd" (bucketfile)
     * @param {Object} [qpConfig.bucketd] - bucketd source
     *   configuration (mandatory if logSource is "bucket")
     * @param {Object} [qpConfig.dmd] - dmd source
     *   configuration (mandatory if logSource is "dmd")
     * @param {Object} [httpsConfig] - HTTPS configuration object
     * @param {String} [httpsConfig.key] - private key file path
     * @param {String} [httpsConfig.cert] - certificate file path
     * @param {String} [httpsConfig.ca] - CA file path
     * @param {Object} mConfig - metrics configuration object
     * @param {string} mConfig.topic - metrics topic
     * @param {Object} rConfig - redis configuration object
     * @param {Object} extConfigs - configuration of extensions: keys
     *   are extension names and values are extension's config object.
     */
    constructor(zkConfig, kafkaConfig, qpConfig, httpsConfig,
        mConfig, rConfig, extConfigs) {
        this.zkConfig = zkConfig;
        this.kafkaConfig = kafkaConfig;
        this.qpConfig = qpConfig;
        this.httpsConfig = httpsConfig;
        this.mConfig = mConfig;
        this.rConfig = rConfig;
        this.extConfigs = extConfigs;

        this.log = new Logger('Backbeat:QueuePopulator');

        // list of active log readers
        this.logReaders = [];

        // list of updated log readers, if any
        this.logReadersUpdate = null;
        // metrics clients
        this._mProducer = null;
        this._mConsumer = null;
        // bucket notification configuration manager
        this.bnConfigManager = null;
    }

    /**
     * Open the queue populator
     *
     * @param {function} cb - callback function
     * @return {undefined}
     */
    open(cb) {
        async.series([
            next => this._setupMetricsClients(next),
            next => this._setupFailedCRRClients(next),
            next => this._setupZookeeper(next),
            next => this._setupNotificationConfigManager(err => {
                if (err) {
                    return next(err);
                }
                this._loadExtensions();
                this._setupLogSources();
                return next();
            }),
        ], err => {
            if (err) {
                this.log.error('error starting up queue populator',
                    {
                        method: 'QueuePopulator.open',
                        error: err,
                    });
                return cb(err);
            }
            return cb();
        });
    }

    _setupMetricsClients(cb) {
        // TODO: Handle metrics for bucket notification
        if (this.extConfigs.replication) {
            // Metrics Consumer
            this._mConsumer = new MetricsConsumer(this.rConfig, this.mConfig,
                this.kafkaConfig);
            this._mConsumer.start();

            // Metrics Producer
            this._mProducer = new MetricsProducer(this.kafkaConfig,
                this.mConfig);
            return this._mProducer.setupProducer(cb);
        }
        return cb();
    }

    /**
     * Set up and start the consumer for retrying failed CRR operations.
     * @param {Function} cb - The callback function
     * @return {undefined}
     */
    _setupFailedCRRClients(cb) {
        // TODO: Handle this in a better way, notifications extension need not
        // handle failed crr
        if (this.extConfigs.replication) {
            this._failedCRRConsumer = new FailedCRRConsumer(this.kafkaConfig);
            return this._failedCRRConsumer.start(cb);
        }
        return cb();
    }

    /**
     * Close the queue populator
     * @param {function} cb - callback function
     * @return {undefined}
     */
    close(cb) {
        return this._closeLogState(cb);
    }

    _setupLogSources() {
        switch (this.qpConfig.logSource) {
        case 'bucketd':
            // initialization of log source is deferred until the
            // dispatcher notifies us of which raft sessions we're
            // responsible for
            this._subscribeToRaftSessionDispatcher();
            break;
        case 'dmd':
            this.logReadersUpdate = [
                new BucketFileLogReader({
                    zkClient: this.zkClient,
                    kafkaConfig: this.kafkaConfig,
                    dmdConfig: this.qpConfig.dmd,
                    logger: this.log,
                    extensions: this._extensions,
                    metricsProducer: this._mProducer,
                    metricsHandler,
                }),
            ];
            break;
        default:
            throw new Error("bad 'logSource' config value: expect 'bucketd'" +
                `or 'dmd', got '${this.qpConfig.logSource}'`);
        }
    }

    _subscribeToRaftSessionDispatcher() {
        const zookeeperUrl =
            this.zkConfig.connectionString +
            this.qpConfig.zookeeperPath;
        const zkEndpoint = `${zookeeperUrl}/raft-id-dispatcher`;
        this.raftIdDispatcher =
            new ProvisionDispatcher({ connectionString: zkEndpoint });
        this.raftIdDispatcher.subscribe((err, items) => {
            if (err) {
                this.log.error('error when receiving raft ID provision list',
                    { zkEndpoint, error: err });
                return undefined;
            }
            if (items.length === 0) {
                this.log.info('no raft ID provisioned, idling',
                    { zkEndpoint });
            }
            this.logReadersUpdate = items.map(
                raftId => new RaftLogReader({
                    zkClient: this.zkClient,
                    kafkaConfig: this.kafkaConfig,
                    bucketdConfig: this.qpConfig.bucketd,
                    httpsConfig: this.httpsConfig,
                    raftId,
                    logger: this.log,
                    extensions: this._extensions,
                    metricsProducer: this._mProducer,
                    metricsHandler,
                }));
            return undefined;
        });
        this.log.info('waiting to be provisioned a raft ID',
            { zkEndpoint });
    }

    _setupZookeeper(done) {
        const populatorZkPath = this.qpConfig.zookeeperPath;
        const zookeeperUrl =
            `${this.zkConfig.connectionString}${populatorZkPath}`;
        this.log.info('opening zookeeper connection for persisting ' +
            'populator state', { zookeeperUrl });
        this.zkClient = zookeeper.createClient(zookeeperUrl, {
            autoCreateNamespace: this.zkConfig.autoCreateNamespace,
        });
        this.zkClient.connect();
        this.zkClient.once('error', done);
        this.zkClient.once('ready', () => {
            // just in case there would be more 'error' events emitted
            this.zkClient.removeAllListeners('error');
            done();
        });
    }

    _setupNotificationConfigManager(done) {
        // setup notification configuration manager only if notification
        // extension is available
        if (this.extConfigs.notification) {
            try {
                this.bnConfigManager = new NotificationConfigManager({
                    zkClient: this.zkClient,
                    logger: this.log,
                });
                return this.bnConfigManager.init(done);
            } catch (err) {
                return done(err);
            }
        }
        return done();
    }

    _loadExtensions() {
        this._extensions = [];
        Object.keys(this.extConfigs).forEach(extName => {
            const extConfig = this.extConfigs[extName];
            const index = require(`../../extensions/${extName}/index.js`);
            if (index.queuePopulatorExtension) {
                // eslint-disable-next-line new-cap
                const ext = new index.queuePopulatorExtension({
                    config: extConfig,
                    zkClient: this.zkClient,
                    logger: this.log,
                    bnConfigManager: this.bnConfigManager,
                    metricsHandler,
                });
                this.log.info(`${index.name} extension is active`);
                this._extensions.push(ext);
            }
        });
    }

    _setupUpdatedReaders(done) {
        const newReaders = this.logReadersUpdate;
        this.logReadersUpdate = null;
        async.each(
            newReaders,
            (logReader, cb) => logReader.setup(cb),
            err => {
                if (err) {
                    return done(err);
                }
                this.logReaders = newReaders;
                return done();
            }
        );
    }

    _closeLogState(done) {
        if (this.raftIdDispatcher !== undefined) {
            return this.raftIdDispatcher.unsubscribe(done);
        }
        return process.nextTick(done);
    }

    _processAllLogEntries(params, done) {
        return async.each(
            this.logReaders,
            (logReader, readerDone) => {
                const batchCb = (err, processedAll) => {
                    if (err) {
                        return readerDone(err);
                    }
                    if (processedAll || this.logReadersUpdate) {
                        return readerDone();
                    }
                    return logReader.processLogEntries(params, batchCb);
                };
                logReader.processLogEntries(params, batchCb);
            },
            done);
    }

    processAllLogEntries(params, done) {
        if (this.logReadersUpdate !== null) {
            return this._setupUpdatedReaders(err => {
                if (err) {
                    return done(err);
                }
                return this._processAllLogEntries(params, done);
            });
        }
        return this._processAllLogEntries(params, done);
    }

    /**
     * Returns current zookeeper client status.
     * Possible values are under zookeeper.State.
     * @returns {string} zookeeper client status
     */
    zkStatus() {
        if (!this.zkClient) {
            return ZKState.DISCONNECTED;
        }
        return this.zkClient.getState();
    }

    /**
     * Handle ProbeServer liveness check
     *
     * @param {http.HTTPServerResponse} res - HTTP Response to respond with
     * @param {Logger} log - Logger
     * @returns {string} Error response string or undefined
     */
    handleLiveness(res, log) {
        // log verbose status for all checks
        const verboseLiveness = {};
        // track and return all errors in one response
        const responses = [];
        const zkState = this.zkStatus();
        verboseLiveness.zookeeper = zkState.code;
        if (zkState.code !== ZKState.SYNC_CONNECTED.code) {
            responses.push({
                component: 'Zookeeper',
                status: 'not connected',
                code: zkState.code,
            });
        }

        for (const reader of this.logReaders) {
            const prodStatuses = reader.getProducerStatus();
            for (const [topic, status] of Object.entries(prodStatuses)) {
                verboseLiveness[`producer-${topic}`] = status;
                if (!status) {
                    responses.push({
                        component: 'log reader',
                        status: 'not ready',
                        topic,
                    });
                }
            }
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
        log.debug('prom metrics fetched');
        res.writeHead(200, {
            'Content-Type': promClient.register.contentType,
        });
        res.end(promClient.register.metrics());
    }
}

module.exports = QueuePopulator;
