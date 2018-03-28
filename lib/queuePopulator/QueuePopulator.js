const async = require('async');
const bucketclient = require('bucketclient');
const Logger = require('werelogs').Logger;
const MetadataWrapper = require('arsenal').storage.metadata.MetadataWrapper;

const zookeeper = require('../clients/zookeeper');
const ProvisionDispatcher = require('../provisioning/ProvisionDispatcher');
const RaftLogReader = require('./RaftLogReader');
const BucketFileLogReader = require('./BucketFileLogReader');
const constants = require('../../constants');

const ringParams = {
    bucketdBootstrap: ['54.202.35.127'],
    bucketdLog: undefined,
};

class QueuePopulator {
    /**
     * Create a queue populator object to populate various kafka
     * queues from the metadata log
     *
     * @constructor
     * @param {Object} zkConfig - zookeeper configuration object
     * @param {string} zkConfig.connectionString - zookeeper
     *   connection string as "host:port[/chroot]"
     * @param {Object} qpConfig - queue populator configuration
     * @param {String} qpConfig.zookeeperPath - sub-path to use for
     *   storing populator state in zookeeper
     * @param {String} qpConfig.logSource - type of source
     *   log: "bucketd" (raft log) or "dmd" (bucketfile)
     * @param {Object} [qpConfig.bucketd] - bucketd source
     *   configuration (mandatory if logSource is "bucket")
     * @param {Object} [qpConfig.dmd] - dmd source
     *   configuration (mandatory if logSource is "dmd")
     * @param {Object} extConfigs - configuration of extensions: keys
     *   are extension names and values are extension's config object.
     */
    constructor(zkConfig, qpConfig, extConfigs) {
        this.zkConfig = zkConfig;
        this.qpConfig = qpConfig;
        this.extConfigs = extConfigs;

        this.log = new Logger('Backbeat:QueuePopulator');

        // list of active log readers
        this.logReaders = [];

        // list of updated log readers, if any
        this.logReadersUpdate = null;

        this.ringReader = new MetadataWrapper('scality', ringParams, bucketclient, this.log);
        this.requestLogger = this.log.newRequestLogger();
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
            next => this._setupExtensions(err => {
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

    /**
     * Close the queue populator
     * @param {function} cb - callback function
     * @return {undefined}
     */
    close(cb) {
        return this._closeLogState(cb);
    }

    _setupExtensions(cb) {
        return async.each(this._extensions, (ext, next) => {
            console.log('ext');
            console.log(ext);
            ext.setupZookeeper(err => {
                if (err) {
                    return next(err);
                }
                if (ext.createZkPath) {
                    constants.raftSessionId.forEach(val => {
                        console.log('node ', val);
                        return ext.createZkPath(val, next);
                    });
                }
                return next();
            });
        }, cb);
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
                new BucketFileLogReader({ zkClient: this.zkClient,
                    zkConfig: this.zkConfig,
                    dmdConfig: this.qpConfig.dmd,
                    logger: this.log,
                    extensions: this._extensions,
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
                    zkConfig: this.zkConfig,
                    bucketdConfig: this.qpConfig.bucketd,
                    raftId,
                    logger: this.log,
                    extensions: this._extensions,
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
    
    _parseBucketName(bucketKey) {
        return bucketKey.split(constants.splitter)[1];
    }

    getRaftSessionBuckets(done) {
        console.log('GETTING RAFT SESSIONS');
        return this.ringReader.listObject(constants.usersBucket, {},
        this.requestLogger, (err, res) => {
            console.log('list objects');
            if (err) {
                console.log('Error');
                console.log('Error');
                console.log(err);
                return done(err);
            }
            // using the res
            // for each 'object key', which is actually name of bucket, call
            // listObject on the bucket - then for list of objects from bucket,
            // get objectMD
            console.log('no error');
            return done(null, res.Contents);
        });
    }

    getRaftSessionBucketObjects(bucketList, done) {
        console.log('getting all the objects from each bucket');
        // make a dictionary
        // use map
        return async.map(bucketList, (bucketInfo, cb) => {
            // listObject gives me markers?
            // at most 1000 objects, process 10 at a time (parallelLimit async call)
            // for each object, produce JSON; combine all 10 and send to Kafka entry
            // move on and continue processing
            // reach end of the list, check  if there is a marker, list objects with marker
            // write another function that uses the list object, gets objects + marker
            // any method that is not exposed to outside of the class, use with leading '_'
            const bucketName = this._parseBucketName(bucketInfo.key);
            this.ringReader.listObject(bucketName, {}, this.requestLogger, (err, res) => {
                if (err) {
                    return done(err);
                }
                console.log('no error!');
                console.log(bucketName);
                return cb(null, { bucket: bucketName, objects: res.Contents });
            });
        }, (err, buckets) => {
            // console.log('here are the buckets with list of object keys!');
            // console.log(buckets);
            return done(null, buckets);
        });
    }

    getObjectMetadata(bucket, object, done) {
        console.log('trying to grab objectMetadata');
        return this.ringReader.getObjectMD(bucket, object.key, {}, this.requestLogger, (err, res) => {
            console.log('getting Object Metadata~!');
            if (err) {
                console.log('there is an error');
                console.log(err);
                return done(err);
            }
            console.log('no error, we are returning now');
            // console.log(JSON.parse(res));
            return done(null, res);
        });
    }
}


module.exports = QueuePopulator;
