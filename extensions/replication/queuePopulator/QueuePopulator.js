const async = require('async');
const zookeeper = require('node-zookeeper-client');

const Logger = require('werelogs').Logger;

const BackbeatProducer = require('../../../lib/BackbeatProducer');
const ProvisionDispatcher =
          require('../../../lib/provisioning/ProvisionDispatcher');
const RaftLogReader = require('./RaftLogReader');
const BucketFileLogReader = require('./BucketFileLogReader');


class QueuePopulator {
    /**
     * Create a queue populator object to activate Cross-Region
     * Replication from a source S3 server to a kafka topic dedicated
     * to store replication entries.
     *
     * @constructor
     * @param {Object} zkConfig - zookeeper configuration object
     * @param {string} zkConfig.connectionString - zookeeper connection string
     *   as "host:port[/chroot]"
     * @param {Object} sourceConfig - source configuration
     * @param {String} sourceConfig.logSource - type of source
     *   log: "bucketd" (raft log) or "dmd" (bucketfile)
     * @param {Object} [sourceConfig.bucketd] - bucketd source
     *   configuration (mandatory if logSource is "bucket")
     * @param {Object} [sourceConfig.dmd] - dmd source
     *   configuration (mandatory if logSource is "dmd")
     * @param {Object} repConfig - replication configuration object
     * @param {String} repConfig.topic - replication topic name
     * @param {String} repConfig.queuePopulator - config object
     *   specific to queue populator
     * @param {String} repConfig.queuePopulator.zookeeperPath -
     *   sub-path to use for storing populator state in zookeeper
     */
    constructor(zkConfig, sourceConfig, repConfig) {
        this.zkConfig = zkConfig;
        this.sourceConfig = sourceConfig;
        this.repConfig = repConfig;

        this.log = new Logger('Backbeat:Replication:QueuePopulator');

        // list of active log readers
        this.logReaders = [];

        // list of updated log readers, if any
        this.logReadersUpdate = null;
    }

    /**
     * Open the queue populator
     *
     * @param {function} cb - callback function
     * @return {undefined}
     */
    open(cb) {
        return async.parallel([
            done => this._setupProducer(done),
            done => this._setupZookeeper(done),
        ], err => {
            if (err) {
                this.log.error('error starting up queue populator',
                               { method: 'QueuePopulator.open',
                                 error: err });
                return cb(err);
            }
            this._setupLogSources();
            return cb();
        });
    }

    /**
     * Close the queue populator
     * @param {function} cb - callback function
     * @return {undefined}
     */
    close(cb) {
        return async.parallel([
            done => this._closeLogState(done),
            done => this._closeProducer(done),
        ], cb);
    }

    _setupLogSources() {
        switch (this.sourceConfig.logSource) {
        case 'bucketd':
            // initialization of log source is deferred until the
            // dispatcher notifies us of which raft sessions we're
            // responsible for
            this._subscribeToRaftSessionDispatcher();
            break;
        case 'dmd':
            this.logReadersUpdate = [
                new BucketFileLogReader({ zkClient: this.zkClient,
                                          kafkaProducer: this.producer,
                                          dmdConfig: this.sourceConfig.dmd,
                                          logger: this.log,
                                        }),
            ];
            break;
        default:
            throw new Error("bad 'logSource' config value: expect 'bucketd'" +
                            `or 'dmd', got '${this.sourceConfig.logSource}'`);
        }
    }

    _subscribeToRaftSessionDispatcher() {
        const zookeeperUrl =
                  this.zkConfig.connectionString +
                  this.repConfig.queuePopulator.zookeeperPath;
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
                    kafkaProducer: this.producer,
                    bucketdConfig: this.sourceConfig.bucketd,
                    raftId,
                    logger: this.log,
                }));
            return undefined;
        });
        this.log.info('waiting to be provisioned a raft ID',
                      { zkEndpoint });
    }

    _setupProducer(done) {
        const producer = new BackbeatProducer({
            zookeeper: this.zkConfig,
            topic: this.repConfig.topic,
        });
        producer.once('error', done);
        producer.once('ready', () => {
            producer.removeAllListeners('error');
            producer.on('error', err => {
                this.log.error('error from backbeat producer',
                               { error: err });
            });
            this.producer = producer;
            done();
        });
    }

    _setupZookeeper(done) {
        const populatorZkPath = this.repConfig.queuePopulator.zookeeperPath;
        const zookeeperUrl =
            `${this.zkConfig.connectionString}${populatorZkPath}`;
        this.log.info('opening zookeeper connection for persisting ' +
                      'populator state',
                      { zookeeperUrl });
        this.zkClient = zookeeper.createClient(zookeeperUrl);
        this.zkClient.connect();
        this.zkClient.once('connected', done);
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

    _closeProducer(done) {
        this.producer.close(done);
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
