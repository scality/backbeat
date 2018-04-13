const async = require('async');

const Logger = require('werelogs').Logger;

const zookeeper = require('../clients/zookeeper');
const ProvisionDispatcher = require('../provisioning/ProvisionDispatcher');
const RaftLogReader = require('./RaftLogReader');
const BucketFileLogReader = require('./BucketFileLogReader');
const MetricsProducer = require('../MetricsProducer');
const MetricsConsumer = require('../MetricsConsumer');
const MongoLogReader = require('./MongoLogReader');
const constants = require('../../constants');

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
     * @param {Object} [qpConfig.mongo] - mongo source
     *   configuration (mandatory if logSource is "mongo")
     * @param {Object} mConfig - metrics configuration object
     * @param {string} mConfig.topic - metrics topic
     * @param {Object} rConfig - redis configuration object
     * @param {Object} extConfigs - configuration of extensions: keys
     *   are extension names and values are extension's config object.
     * @param {string} logSource - logSource determined by startup input
     * @param {object} iConfig - configuration for ingestion
     * @param {string} ingestionSource - source of ingestion
     */
    constructor(zkConfig, kafkaConfig, qpConfig, mConfig, rConfig,
                extConfigs, ingestionConfig, logSource, ingestionSource) {
        this.zkConfig = zkConfig;
        this.kafkaConfig = kafkaConfig;
        this.qpConfig = qpConfig;
        this.mConfig = mConfig;
        this.rConfig = rConfig;
        this.extConfigs = extConfigs;
        this.logSource = logSource;
        this.iConfig = ingestionConfig;
        this.iSource = ingestionSource;
        this.log = new Logger('Backbeat:QueuePopulator');

        // list of active log readers
        this.logReaders = [];

        // list of updated log readers, if any
        this.logReadersUpdate = null;
        // metrics clients
        this._mProducer = null;
        this._mConsumer = null;
    }

    /**
     * Open the queue populator
     *
     * @param {function} cb - callback function
     * @return {undefined}
     */
    open(cb) {
        this._loadExtensions();
        async.series([
            next => this._setupMetricsClients(next),
            next => this._setupExtensions(err => {
                console.log('setting up extensions');
                if (err) {
                    this.log.error(
                        'error setting up queue populator extensions', {
                            method: 'QueuePopulator.open',
                            error: err,
                        });
                }
                return next(err);
            }),
            next => this._setupZookeeper(err => {
                console.log('SETUP ZOOKEEPER');
                if (err) {
                    return next(err);
                }
                this._setupLogSources();
                return next();
            }),
        ], err => {
            if (err) {
                this.log.error('error starting up queue populator',
                    { method: 'QueuePopulator.open',
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
     * Close the queue populator
     * @param {function} cb - callback function
     * @return {undefined}
     */
    close(cb) {
        return this._closeLogState(cb);
    }

    _setupLogSources() {
        // don't ignore bucketd
        // if (this.qpConfig.logSource.indexOf('ingestion') > -1) {
        //     this._subscribeToRaftSessionDispatcher();
        // }
        console.log('SETUP LOG SOURCES');
        switch (this.logSource) {
        case 'bucketd':
            console.log('BUCKETD');
            // initialization of log source is deferred until the
            // dispatcher notifies us of which raft sessions we're
            // responsible for
            const zookeeperUrl = this.zkConfig.connectionString +
                      this.qpConfig.zookeeperPath;
            const zkEndpoint = `${zookeeperUrl}/raft-id-dispatcher`;
            this._subscribeToRaftSessionDispatcher(zkEndpoint,
                this.qpConfig.bucketd);
            break;
        case 'dmd':
            console.log('DMD');
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
        case 'mongo':
            this.logReadersUpdate = [
                new MongoLogReader({ zkClient: this.zkClient,
                    kafkaConfig: this.kafkaConfig,
                    zkConfig: this.zkConfig,
                    mongoConfig: this.qpConfig.mongo,
                    logger: this.log,
                    extensions: this._extensions,
                    metricsProducer: this._mProducer,
                }),
            ];
        case 'ingestion':
        // using the config values of zookeeper path, zookeeper suffix,
            console.log('INGESTION');
            console.log(this.iConfig);
            console.log(this.iSource);
            const zookeeperUrlIngest =
                `${this.zkConfig.connectionString}/ingestion/` +
                `${this.iSource}`;
            const zkEndpointIngest = `${zookeeperUrlIngest}/raft-id-dispatcher`;
            this._subscribeToRaftSessionDispatcher(zkEndpointIngest,
                this.iConfig.sources[this.iSource]);
            break;
        default:
            throw new Error("bad 'logSource' config value: expect 'bucketd,'" +
                        ` mongo or 'dmd', got '${this.qpConfig.logSource}'`);
        }
    }

    _subscribeToRaftSessionDispatcher(zkEndpoint, bucketd) {
        console.log(zkEndpoint);
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
                    bucketdConfig: bucketd,
                    raftId,
                    logger: this.log,
                    extensions: this._extensions,
                    metricsProducer: this._mProducer,
                }));
            return undefined;
        });
        this.log.info('waiting to be provisioned a raft ID',
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
                    // right now hardcoded to by default make all zkPaths for
                    // all raft session ids that are listed in constants
                    constants.raftSessionId.forEach(val =>
                        ext.createZkPath(next, val, this.iSource));
                }
                return next();
            });
        }, cb);
        // if (ext.IngestionProducer), run the ingestion
    }

    _setupUpdatedReaders(done) {
        const newReaders = this.logReadersUpdate;
        this.logReadersUpdate = null;
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

    _processAllLogEntries(params, done) {
        return async.map(
            this.logReaders,
            (logReader, done) => logReader.processAllLogEntries(params, done),
            (err, results) => {
                if (err) {
                    return done(err);
                }
                const annotatedResults = results.map(
                    (result, i) => Object.assign(result, {
                        logSource: this.logReaders[i].getLogInfo(),
                        logOffset: this.logReaders[i].getLogOffset(),
                    }));
                return done(null, annotatedResults);
            });
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
}


module.exports = QueuePopulator;
