const async = require('async');
const url = require('url');

const Logger = require('werelogs').Logger;

const config = require('../../conf/Config');
const zookeeper = require('../clients/zookeeper');
const ProvisionDispatcher = require('../provisioning/ProvisionDispatcher');
const IngestionReader = require('./IngestionReader');
const MetricsProducer = require('../MetricsProducer');
const MetricsConsumer = require('../MetricsConsumer');
const MongoIngestionInterface =
    require('../../extensions/ingestion/utils/MongoIngestionInterface');

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
     * @param {String} kafkaConfig.hosts - kafka hosts list
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
     * @param {String} mConfig.topic - metrics topic
     * @param {Object} rConfig - redis configuration object
     * @param {Object} ingestionConfig - ingestion extension configurations
     * @param {Object} s3Config - configuration to connect to S3 Client
     */
    constructor(zkConfig, kafkaConfig, qpConfig, mConfig, rConfig,
                ingestionConfig, s3Config) {
        this.zkConfig = zkConfig;
        this.kafkaConfig = kafkaConfig;
        this.qpConfig = qpConfig;
        this.mConfig = mConfig;
        this.rConfig = rConfig;
        this.ingestionConfig = ingestionConfig;
        this.s3Config = s3Config;

        this.log = new Logger('Backbeat:IngestionPopulator');

        // mongo client interface for finding ingestion buckets
        this._mongoClient = null;

        // list of active log readers
        this.logReaders = [];

        // list of updated log readers, if any
        this.logReadersUpdate = [];
        // metrics clients
        this._mProducer = null;
        this._mConsumer = null;
        // location details to be updated from config initManagement updates
        this._locationDetails = null;

        // provisioned ingestion log sources
        // i.e.: { an-ingestion-location: new ProvisionDispatcher() }
        this._activeIngestionSources = {};
    }

    /**
     * Open the ingestion populator
     *
     * @param {function} cb - callback function
     * @return {undefined}
     */
    open(cb) {
        this._loadExtension();
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
            next => this._setupIngestionExtension(err => {
                if (err) {
                    this.log.error(
                        'error setting up ingestion extension', {
                            method: 'IngestionPopulator.open',
                            error: err,
                        });
                }
                return next(err);
            }),
            next => this._setupZookeeper(next),
            next => {
                this._mongoClient =
                    new MongoIngestionInterface(this.qpConfig.mongo);
                this._mongoClient.setup(err => {
                    if (err) {
                        this.log.error('error setting up mongo client', {
                            method: 'IngestionPopulator.open',
                            error: err,
                        });
                    }
                    return next(err);
                });
            },
        ], err => {
            if (err) {
                this.log.error('error starting up ingestion populator',
                    { method: 'IngestionPopulator.open',
                        error: err });
                return cb(err);
            }
            this._locationDetails = config.getLocations();
            config.on('locations-update', () => {
                this._locationDetails = config.getLocations();
            });
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
     * Check mongo (__metastore) for new buckets and apply any updates
     * @param {Function} cb - callback(error)
     * @return {undefined}
     */
    applyUpdates(cb) {
        this._mongoClient.getIngestionBuckets((err, buckets) => {
            if (err) {
                return cb(err);
            }
            // Example buckets list:
            // [
            //     {
            //         name: 'my-zenko-bucket',
            //         ingestion: { Status: 'enabled' },
            //         locationConstraint: 'my-ring'
            //     },
            // ]

            const currentActiveSources =
                Object.keys(this._activeIngestionSources);

            return async.series([
                done => async.each(buckets, (bucket, next) => {
                    // zenko bucket name
                    const zenkoBucket = bucket.name;

                    if (!currentActiveSources.includes(zenkoBucket)) {
                        // Add new ingestion source
                        const newSourceInfo =
                            this._getNewSourceInformation(bucket);
                        return this.addNewLogSource(newSourceInfo, next);
                    }

                    // if the zenko bucket has already been setup and is already
                    // active, stop tracking bucket from `currentActiveSources`
                    const isAlreadyActive =
                        currentActiveSources.indexOf(zenkoBucket) >= 0;
                    if (isAlreadyActive) {
                        currentActiveSources.splice(
                            currentActiveSources.indexOf(zenkoBucket), 1);
                    }
                    return next();
                }, done),
                // Any leftover `currentActiveSources` are no longer active and
                // must be removed.
                done => async.each(currentActiveSources,
                    (source, next) => this.closeLogState(source, next),
                    done),
            ], cb);
        });
    }

    /**
     * Get necessary information for setting up a new IngestionReader and form
     * as single object
     * @param {Object} bucketInfo - ingestion bucket details fetched
     *   using extensions/ingestion/utils/MongoIngestionInterface.js
     * @param {String} bucketInfo.name - Zenko bucket name
     * @param {Object} bucketInfo.ingestion - ingestion object as
     *   { Status: 'enabled' } or { Status: 'disabled' }
     * @param {String} bucketInfo.locationConstraint - Zenko location storage
     *   name
     * @return {Object} new source config details
     */
    _getNewSourceInformation(bucketInfo) {
        // Example this._locationDetails
        // {
        //     'my-ring': {
        //         accessKey: ...,
        //         secretKey: ...,
        //         endpoint: 'http://10.100.1.128:8000',
        //         locationType: 'scality_s3',
        //         bucketName: 'source-bucket-1'
        //     }
        // }

        // Zenko related information
        const { name, locationConstraint } = bucketInfo;
        // Source location information
        const { accessKey, secretKey, endpoint, locationType, bucketName } =
            this._locationDetails[locationConstraint];

        const urlObject = url.parse(endpoint);
        // TODO: will change for non-ring sources
        const auth = { accessKey, secretKey };

        return {
            // target zenko bucket name
            name,
            // source bucket name to ingest from
            bucket: bucketName,
            // source (s3c) endpoint
            host: urlObject.hostname,
            port: parseInt(urlObject.port, 10) || 80,
            https: urlObject.protocol.startsWith('https'),
            type: locationType,
            raftCount: 8,
            auth,
        };
    }

    /**
     * add an Ingestion Reader
     * @param {Object} source - new source object
     * @param {Function} cb - callback(error)
     * @return {undefined}
     */
    addNewLogSource(source, cb) {
        this._extension.createZkPath(err => {
            if (err) {
                return cb(err);
            }
            const zookeeperUrlIngest =
                `${this.zkConfig.connectionString}` +
                `${this.ingestionConfig.zookeeperPath}/${source.name}`;
            const zkEndpointIngest = `${zookeeperUrlIngest}/raft-id-dispatcher`;
            this._subscribeToLogSourceDispatcher(zkEndpointIngest, source);
            return cb();
        }, source);
    }

    _subscribeToLogSourceDispatcher(zkEndpoint, bucketd) {
        const key = bucketd.name;
        if (this._activeIngestionSources[key]) {
            this.log.info('waiting to be provisioned a log source',
                          { zkEndpoint });
            return undefined;
        }

        // TODO: Remove/Replace ProvisionDispatcher
        const raftIdDispatcher =
            new ProvisionDispatcher({ connectionString: zkEndpoint });
        this._activeIngestionSources[key] = raftIdDispatcher;

        raftIdDispatcher.subscribe((err, items) => {
            if (err) {
                this.log.error(
                    'error when receiving log source provision list',
                    { zkEndpoint, error: err });
                // do not escalate error in order to allow retries.
                // provisioning log source may take a while
                return undefined;
            }
            if (items.length === 0) {
                this.log.info('no log source provisioned, idling',
                              { zkEndpoint });
            }
            const newRaftReaders = items.map(token => {
                const ingestionReader = new IngestionReader({
                    zkClient: this.zkClient,
                    kafkaConfig: this.kafkaConfig,
                    bucketdConfig: bucketd,
                    raftId: token,
                    logger: this.log,
                    extensions: [this._extension],
                    metricsProducer: this._mProducer,
                    qpConfig: this.qpConfig,
                    s3Config: this.s3Config,
                });
                return ingestionReader;
            });
            this.logReadersUpdate.push(...newRaftReaders);
            return undefined;
        });
        this.log.info('waiting to be provisioned a log source',
                      { zkEndpoint });
        return undefined;
    }

    _setupZookeeper(done) {
        const populatorZkPath = this.ingestionConfig.zookeeperPath;
        const zookeeperUrl =
            `${this.zkConfig.connectionString}${populatorZkPath}`;
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

    _loadExtension() {
        const index = require('../../extensions/ingestion/index.js');
        if (!index.queuePopulatorExtension) {
            this.log.fatal('Missing ingestion populator extension file');
            process.exit(1);
        }
        // eslint-disable-next-line new-cap
        const ext = new index.queuePopulatorExtension({
            config: this.ingestionConfig,
            logger: this.log,
        });
        ext.setZkConfig(this.zkConfig);
        this._extension = ext;

        this.log.info('ingestion extension is active');
    }

    _setupIngestionExtension(cb) {
        return this._extension.setupZookeeper(cb);
    }

    _setupUpdatedReaders(done) {
        const newReaders = this.logReadersUpdate;
        this.logReadersUpdate = [];
        async.each(newReaders, (logReader, cb) => logReader.setup(cb),
                   err => {
                       if (err) {
                           return done(err);
                       }
                       this.logReaders.push(...newReaders);
                       return done();
                   });
    }

    /**
     * Close all provisioned ingestion sources
     * @param {function} cb - callback function
     * @return {undefined}
     */
    close(cb) {
        return async.each(
            Object.keys(this._activeIngestionSources),
            (raftIdDispatcher, done) => {
                this.closeLogState(raftIdDispatcher, done);
            }, cb);
    }

    /**
     * remove and close an Ingestion Reader
     * @param {String} key - source key (zenko bucket name)
     * @param {Function} cb - callback(error)
     * @return {undefined}
     */
    closeLogState(key, cb) {
        const raftIdDispatcher = this._activeIngestionSources[key];
        if (raftIdDispatcher !== undefined) {
            delete this._activeIngestionSources[key];
            this._checkAndRemoveLogReader(key, this.logReaders);
            this._checkAndRemoveLogReader(key, this.logReadersUpdate);

            return raftIdDispatcher.unsubscribe(cb);
        }
        return process.nextTick(cb);
    }

    _checkAndRemoveLogReader(name, list) {
        const index = list.findIndex(lr =>
            lr.getTargetZenkoBucketName() === name);
        if (index !== -1) {
            list.splice(index, 1);
        }
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
