const async = require('async');

const Logger = require('werelogs').Logger;

const zookeeper = require('../clients/zookeeper');
const ProvisionDispatcher = require('../provisioning/ProvisionDispatcher');
const IngestionReader = require('./IngestionReader');
const BucketFileLogReader = require('./BucketFileLogReader');
const MetricsProducer = require('../MetricsProducer');
const MetricsConsumer = require('../MetricsConsumer');
const MongoLogReader = require('./MongoLogReader');
const IngestionQueuePopulator =
    require('../../extensions/ingestion/IngestionQueuePopulator');

class IngestionPopulator {
    /**
     * Create an ingestion populator object to populate various kafka
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
     * @param {Object} [qpConfig.mongo] - mongo source
     *   configuration (mandatory if logSource is "mongo")
     * @param {Object} mConfig - metrics configuration object
     * @param {string} mConfig.topic - metrics topic
     * @param {Object} rConfig - redis configuration object
     * @param {Object} extConfigs - configuration of extensions: keys
     *   are extension names and values are extension's config object.
     * @param {Object} s3Config - configuration to connect to S3 Client
     */
    constructor(zkConfig, kafkaConfig, qpConfig, mConfig, rConfig,
                extConfigs, s3Config) {
        this.zkConfig = zkConfig;
        this.kafkaConfig = kafkaConfig;
        this.qpConfig = qpConfig;
        this.mConfig = mConfig;
        this.rConfig = rConfig;
        this.extConfigs = extConfigs;
        this.s3Config = s3Config;

        this.log = new Logger('Backbeat:IngestionPopulator');

        // list of active log readers
        this.logReaders = [];

        // list of updated log readers, if any
        this.logReadersUpdate = [];
        // metrics clients
        this._mProducer = null;
        this._mConsumer = null;
    }

    /**
     * Open the ingestion populator
     *
     * @param {function} cb - callback function
     * @return {undefined}
     */
    open(cb) {
        this._loadExtensions();
        async.series([
            next => this._setupMetricsClients(err => {
                if (err) {
                    this.log.error('error setting up metrics client', {
                        method: 'IngestionPopulator.open',
                        error: err,
                    });
                }
                return next(err);
            }),
            next => this._setupExtensions(err => {
                if (err) {
                    this.log.error(
                        'error setting up ingestion extension', {
                            method: 'IngestionPopulator.open',
                            error: err,
                        });
                }
                return next(err);
            }),
            next => this._setupZookeeper(err => {
                if (err) {
                    return next(err);
                }
                this._setupLogSources();
                return next();
            }),
        ], err => {
            if (err) {
                this.log.error('error starting up ingestion populator',
                    { method: 'IngestionPopulator.open',
                        error: err });
                return cb(err);
            }
            return cb();
        });
    }

    _setupMetricsClients(cb) {
        // Metrics Consumer
        this._mConsumer = new MetricsConsumer(this.rConfig, this.mConfig,
            this.kafkaConfig);
        this._mConsumer.start();

        // Metrics Producer
        this._mProducer = new MetricsProducer(this.kafkaConfig, this.mConfig);
        this._mProducer.setupProducer(cb);
    }

    /**
     * Close provisioned ingestion source
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
            this._subscribeToLogSourceDispatcher('raft-id-dispatcher');
            break;
        case 'mongo':
            if (this.qpConfig.subscribeToLogSourceDispatcher) {
                // initialization of mongo log source is deferred until
                // the dispatcher notifies us we're the single populator
                // responsible for it
                this._subscribeToLogSourceDispatcher('mongo-dispatcher');
            } else {
                // always consume from mongo log (good for Kubernetes
                // deployments)
                this.logReadersUpdate = [
                    new MongoLogReader({
                        zkClient: this.zkClient,
                        kafkaConfig: this.kafkaConfig,
                        zkConfig: this.zkConfig,
                        mongoConfig: this.qpConfig.mongo,
                        logger: this.log,
                        extensions: this._extensions,
                        metricsProducer: this._mProducer,
                    }),
                ];
            }
            break;
        case 'dmd':
            this.logReadersUpdate = [
                new BucketFileLogReader({ zkClient: this.zkClient,
                    kafkaConfig: this.kafkaConfig,
                    dmdConfig: this.qpConfig.dmd,
                    logger: this.log,
                    extensions: this._extensions,
                    metricsProducer: this._mProducer,
                }),
            ];
            break;
        default:
            throw new Error("bad 'logSource' config value: expect 'bucketd,'" +
                        ` mongo or 'dmd', got '${this.qpConfig.logSource}'`);
        }
        if (this.extConfigs.ingestion) {
            this.extConfigs.ingestion.sources.map(sourceObj => {
                const zookeeperUrlIngest =
                    `${this.zkConfig.connectionString}` +
                    `${this.extConfigs.ingestion.zookeeperPath}/` +
                    `${sourceObj.name}`;
                const zkEndpointIngest =
                    `${zookeeperUrlIngest}/raft-id-dispatcher`;
                this._subscribeToLogSourceDispatcher(zkEndpointIngest,
                    sourceObj, this.extConfigs.ingestion.zookeeperPath);
                return undefined;
            });
        }
    }

    _subscribeToLogSourceDispatcher(zkEndpoint, bucketd, ingestionPath) {
        this.raftIdDispatcher =
            new ProvisionDispatcher({ connectionString: zkEndpoint });
        this.raftIdDispatcher.subscribe((err, items) => {
            if (err) {
                this.log.error(
                    'error when receiving log source provision list',
                    { zkEndpoint, error: err });
                return undefined;
            }
            if (items.length === 0) {
                this.log.info('no log source provisioned, idling',
                              { zkEndpoint });
            }
            const newRaftReaders = items.map(
                token => {
                    if (token === 'mongo') {
                        return new MongoLogReader({
                            zkClient: this.zkClient,
                            kafkaConfig: this.kafkaConfig,
                            zkConfig: this.zkConfig,
                            mongoConfig: this.qpConfig.mongo,
                            logger: this.log,
                            extensions: this._extensions,
                            metricsProducer: this._mProducer,
                        });
                    }
                    return new IngestionReader({
                        zkClient: this.zkClient,
                        kafkaConfig: this.kafkaConfig,
                        bucketdConfig: bucketd,
                        raftId: token,
                        logger: this.log,
                        extensions: this._extensions,
                        metricsProducer: this._mProducer,
                        ingestionPath,
                        qpConfig: this.qpConfig,
                        s3Config: this.s3Config,
                    });
                });
            newRaftReaders.forEach(reader =>
                this.logReadersUpdate.push(reader));
            return undefined;
        });
        this.log.info('waiting to be provisioned a log source',
                      { zkEndpoint });
    }

    _setupZookeeper(done) {
        const zookeeperUrl = this.zkConfig.connectionString;
        this.log.info('opening zookeeper connection for persisting ' +
                      'populator state',
                      { zookeeperUrl });
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

    _loadExtensions() {
        this._extensions = [];
        Object.keys(this.extConfigs).forEach(extName => {
            const extConfig = this.extConfigs[extName];
            const index = require(`../../extensions/${extName}/index.js`);
            if (index.queuePopulatorExtension) {
                // eslint-disable-next-line new-cap
                const ext = new index.queuePopulatorExtension({
                    config: extConfig,
                    logger: this.log,
                });
                ext.setZkConfig(this.zkConfig);
                this.log.info(`${index.name} extension is active`);
                this._extensions.push(ext);
            }
        });
    }

    _setupExtensions(cb) {
        return async.each(this._extensions, (ext, next) => {
            ext.setupZookeeper(err => {
                if (err) {
                    return next(err);
                }
                if (ext.createZkPath) {
                    if (ext instanceof IngestionQueuePopulator) {
                        return async.each(
                            this.extConfigs.ingestion.sources,
                            (source, next) => ext.createZkPath(next, source),
                            next);
                    }
                    return ext.createZkPath(next);
                }
                return next();
            });
        }, cb);
    }

    _setupUpdatedReaders(done) {
        const newReaders = this.logReadersUpdate;
        this.logReadersUpdate = [];
        async.each(newReaders, (logReader, cb) => logReader.setup(cb),
                   err => {
                       if (err) {
                           return done(err);
                       }
                       this.logReaders = newReaders;
                       return done();
                   });
    }

    _closeLogState(done) {
        if (this.raftIdDispatcher !== undefined) {
            return this.raftIdDispatcher.unsubscribe(done);
        }
        return process.nextTick(done);
    }

    _processLogEntries(params, done) {
        return async.map(
            this.logReaders,
            (logReader, cb) => logReader.processLogEntries(params, cb),
            done);
    }

    processLogEntries(params, done) {
        if (this.logReadersUpdate.length >= 1) {
            return this._setupUpdatedReaders(err => {
                if (err) {
                    return done(err);
                }
                return this._processLogEntries(params, done);
            });
        }
        return this._processLogEntries(params, done);
    }

    zkStatus() {
        return this.zkClient.getState();
    }
}


module.exports = IngestionPopulator;
