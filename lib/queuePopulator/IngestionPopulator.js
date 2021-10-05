const async = require('async');
const url = require('url');
const Redis = require('ioredis');
const schedule = require('node-schedule');

const Logger = require('werelogs').Logger;

const config = require('../Config');
const IngestionReader = require('./IngestionReader');
const BackbeatProducer = require('../BackbeatProducer');
const MetricsConsumer = require('../MetricsConsumer');
const MetricsProducer = require('../MetricsProducer');
const { metricsExtension } = require('../../extensions/ingestion/constants');

const {
    zookeeperNamespace,
    zkStatePath,
    zkStateProperties,
} = require('../../extensions/ingestion/constants');

const POLL_INTERVAL_MS = 50;

class IngestionPopulator {
    /**
     * Create an ingestion populator object to populate various kafka
     * queues from the metadata log
     *
     * @constructor
     * @param {node-zookeeper-client.Client} zkClient - zookeeper client
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
    constructor(zkClient, zkConfig, kafkaConfig, qpConfig, mConfig, rConfig,
        ingestionConfig, s3Config) {
        this.zkClient = zkClient;
        this.zkConfig = zkConfig;
        this.kafkaConfig = kafkaConfig;
        this.qpConfig = qpConfig;
        this.mConfig = mConfig;
        this.rConfig = rConfig;
        this.ingestionConfig = ingestionConfig;
        this.s3Config = s3Config;

        this.log = new Logger('Backbeat:IngestionPopulator');

        // list of active log readers
        this.logReaders = [];

        // list of updated log readers, if any
        this.logReadersUpdate = [];

        // shared producer across readers
        this._producer = null;

        // metrics clients
        this._mProducer = null;
        this._mConsumer = null;
        this._redis = null;

        // all ingestion readers (including paused ones)
        // i.e.: { zenko-bucket-name: IngestionReader() }
        this._ingestionSources = {};
        // paused location constraints
        // i.e.: { location-constraint: null || new schedule() }
        this._pausedLocations = {};
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
            next => this._setupProducer(err => {
                if (err) {
                    this.log.error('error setting up producer', {
                        method: 'IngestionPopulator.open',
                        error: err,
                    });
                }
                return next(err);
            }),
        ], err => {
            if (err) {
                this.log.error('error starting up ingestion populator',
                    {
                        method: 'IngestionPopulator.open',
                        error: err
                    });
                return cb(err);
            }
            // setup redis for pause/resume
            this._setupRedis(this.rConfig);

            return cb();
        });
    }

    _setupMetricsClients(cb) {
        // Metrics Consumer
        this._mConsumer = new MetricsConsumer(this.rConfig, this.mConfig,
            this.kafkaConfig, metricsExtension);
        this._mConsumer.start();

        // Metrics Producer
        this._mProducer = new MetricsProducer(this.kafkaConfig, this.mConfig);
        this._mProducer.setupProducer(cb);
    }

    _setupProducer(cb) {
        if (this._producer) {
            return process.nextTick(cb);
        }
        const topic = this.ingestionConfig.topic;
        const producer = new BackbeatProducer({
            kafka: { hosts: this.kafkaConfig.hosts },
            topic,
            pollIntervalMs: POLL_INTERVAL_MS,
        });
        producer.once('error', cb);
        producer.once('ready', () => {
            this.log.debug('producer is ready', {
                kafkaConfig: this.kafkaConfig,
                topic,
            });
            producer.removeAllListeners('error');
            producer.on('error', err => {
                this.log.error('error from backbeat producer', {
                    error: err,
                    topic,
                });
            });
            this._producer = producer;
            cb();
        });
        return undefined;
    }

    /**
     * Setup the Redis Subscriber which listens for actions from other processes
     * (i.e. BackbeatAPI for pause/resume)
     * @param {Object} redisConfig - redis config
     * @return {undefined}
     */
    _setupRedis(redisConfig) {
        // redis pub/sub for pause/resume
        const redis = new Redis(redisConfig);
        // redis psubscribe and will need to parse location name
        const { topic } = this.ingestionConfig;
        const channelPattern = `${topic}-*`;
        redis.psubscribe(channelPattern, err => {
            if (err) {
                this.log.fatal('failed to subscribe to redis channel', {
                    method: 'IngestionPopulator._setupRedis',
                    error: err,
                });
                process.exit(1);
            }
            this._redis = redis;
        });
        redis.on('pmessage', (channel, pattern, message) => {
            const validActions = {
                pauseService: this._pauseService.bind(this),
                resumeService: this._resumeService.bind(this),
                deleteScheduledResumeService:
                    this._deleteScheduledResumeService.bind(this),
            };

            let locationConstraint;
            let parsedMessage;
            try {
                // remove topic in front of pattern to get location name
                locationConstraint = pattern.replace(`${topic}-`, '');
                parsedMessage = JSON.parse(message);
            } catch (e) {
                this.log.error('error parsing redis subscribe message', {
                    method: 'IngestionPopulator._setupRedis',
                    error: e,
                });
                return;
            }
            const { action, date } = parsedMessage;
            const cmd = validActions[action];
            if (typeof cmd === 'function') {
                cmd(locationConstraint, date);
            }
        });
    }

    setPausedLocationState(location, schedule) {
        // set paused location state for a location without a scheduled resume
        this._pausedLocations[location] = schedule || null;
    }

    removePausedLocationState(location) {
        if (Object.keys(this._pausedLocations).includes(location)) {
            delete this._pausedLocations[location];
        }
    }

    /**
     * Pause bucket(s) based on given location constraint
     * @param {String} location - location constraint
     * @return {undefined}
     */
    _pauseService(location) {
        const enabled = !Object.keys(this._pausedLocations).includes(location);
        if (enabled) {
            // if currently resumed/active, attempt to pause
            this._updateZkStateNode(location, 'paused', true, err => {
                if (err) {
                    this.log.trace('error occurred saving state to zookeeper', {
                        method: 'IngestionPopulator._pauseService',
                        locationConstraint: location,
                    });
                } else {
                    // remove from this.logReaders
                    const toPauseReaders = this.logReaders.filter(lr =>
                        lr.getLocationConstraint() === location);
                    toPauseReaders.forEach(reader => {
                        const zenkoBucket = reader.getTargetZenkoBucketName();
                        this._checkAndRemoveLogReader(zenkoBucket,
                            this.logReaders);
                        this.log.info('paused ingestion reader', {
                            zenkoBucket,
                            locationConstraint: location,
                        });
                    });
                    this._deleteScheduledResumeService(location);
                    // add to this._pausedLocations
                    this.setPausedLocationState(location);
                }
            });
        }
    }

    /**
     * Resume bucket(s) based on given location constraint
     * @param {String} location - location constraint
     * @param {Date} [date] - optional date object for scheduling resume
     * @return {undefined}
     */
    _resumeService(location, date) {
        const enabled = !Object.keys(this._pausedLocations).includes(location);
        const now = new Date();

        if (enabled) {
            this.log.info(`cannot resume, location ${location} is not paused`);
            return;
        }

        if (date && now < new Date(date)) {
            // if date is in the future, attempt to schedule job
            this.scheduleResume(location, date);
            return;
        }
        // otherwise, if !date || now >= new Date(date)
        this._updateZkStateNode(location, 'paused', false, err => {
            if (err) {
                this.log.trace('error occurred saving state to zookeeper', {
                    method: 'IngestionPopulator._resumeService',
                    locationConstraint: location,
                });
                return;
            }
            const zenkoBuckets = Object.keys(this._ingestionSources);
            this._deleteScheduledResumeService(location);
            zenkoBuckets.forEach(bucket => {
                const reader = this._ingestionSources[bucket];
                if (reader.getLocationConstraint() === location) {
                    // add to this.logReaders
                    this.logReaders.push(reader);
                    // remove from this._pausedLocations
                    this.removePausedLocationState(location);
                    this.log.info('resumed ingestion reader', {
                        zenkoBucket: bucket,
                        locationConstraint: location,
                    });
                }
            });
        });
    }

    /**
     * Delete scheduled resume (if any)
     * @param {String} location - location constraint
     * @return {undefined}
     */
    _deleteScheduledResumeService(location) {
        this._updateZkStateNode(location, 'scheduledResume', null, err => {
            if (err) {
                this.log.trace('error occurred saving state to zookeeper', {
                    method: 'IngestionPopulator._deleteScheduledResumeService',
                    locationConstraint: location,
                });
                return;
            }
            const schedule = this._pausedLocations[location];
            if (schedule) {
                schedule.cancel();
                this.setPausedLocationState(location);
                this.log.info('deleted scheduled resume for ingestion reader', {
                    locationConstraint: location,
                });
            }
        });
    }

    scheduleResume(location, date) {
        function triggerResume() {
            this._updateZkStateNode(location, 'scheduledResume', null, err => {
                if (err) {
                    this.log.error('error occurred saving state to zookeeper ' +
                        'to resume a scheduled resume. Retry again in 1 minute', {
                        method: 'IngestionPopulator.scheduleResume',
                        locationConstraint: location,
                        error: err,
                    });
                    // if an error occurs, need to retry
                    // for now, schedule minute from now
                    const date = new Date();
                    date.setMinutes(date.getMinutes() + 1);
                    const scheduledResume = schedule.scheduleJob(date,
                        triggerResume.bind(this));
                    this.setPausedLocationState(location, scheduledResume);
                } else {
                    const scheduledResume = this._pausedLocations[location];
                    if (scheduledResume) {
                        scheduledResume.cancel();
                    }
                    this.setPausedLocationState(location);
                    this._resumeService(location);
                }
            });
        }

        this._updateZkStateNode(location, 'scheduledResume', date, err => {
            if (err) {
                this.log.trace('error occurred saving state to zookeeper', {
                    method: 'IngestionPopulator.scheduleResume',
                    locationConstraint: location,
                });
                return;
            }
            const scheduledResume = schedule.scheduleJob(date,
                triggerResume.bind(this));
            this.setPausedLocationState(location, scheduledResume);
            this.log.info('scheduled ingestion resume', {
                locationConstraint: location,
                scheduleTime: date.toString(),
            });
        });
    }

    _getZkLocationNode(location) {
        return `${zookeeperNamespace}${zkStatePath}/${location}`;
    }

    /**
     * Update zookeeper state node for given location
     * @param {String} location - location constraint (also zk state node)
     * @param {String} key - key name to store in zk state node
     * @param {String|Boolean} value - value
     * @param {Function} cb - callback(error)
     * @return {undefined}
     */
    _updateZkStateNode(location, key, value, cb) {
        if (!zkStateProperties.includes(key)) {
            const errorMsg = 'incorrect zookeeper state property given';
            this.log.error(errorMsg, {
                method: 'IngestionPopulator._updateZkStateNode',
            });
            return cb(new Error(errorMsg));
        }
        const path = this._getZkLocationNode(location);
        return async.waterfall([
            next => this.zkClient.getData(path, (err, data) => {
                if (err) {
                    this.log.error('could not get state from zookeeper', {
                        method: 'IngestionPopulator._updateZkStateNode',
                        zookeeperPath: path,
                        error: err.message,
                    });
                    return next(err);
                }
                let bufferedData;
                try {
                    const state = JSON.parse(data.toString());
                    // set revised status
                    state[key] = value;
                    bufferedData = Buffer.from(JSON.stringify(state));
                } catch (err) {
                    this.log.error('could not parse state data from ' +
                        'zookeeper', {
                        method: 'IngestionPopulator._updateZkStateNode',
                        zookeeperPath: path,
                        error: err,
                    });
                    return next(err);
                }
                return next(null, bufferedData);
            }),
            (data, next) => this.zkClient.setData(path, data, err => {
                if (err) {
                    this.log.error('could not save state data in zookeeper', {
                        method: 'IngestionPopulator._updateZkStateNode',
                        zookeeperPath: path,
                        error: err,
                    });
                }
                return next(err);
            }),
        ], cb);
    }

    /**
     * Apply updates when ingestion buckets are updated in Config
     * @param {Function} cb - callback(error)
     * @return {undefined}
     */
    applyUpdates(cb) {
        // Example buckets param object:
        // [
        //      {
        //          accessKey: ...,
        //          secretKey: ...,
        //          endpoint: 'http://10.100.1.128:8000',
        //          locationType: 'scality_s3',
        //          bucketName: 'source-bucket-1',
        //          zenkoBucket: 'my-zenko-bucket',
        //          ingestion: { status: 'enabled' },
        //          locationConstraint: 'my-ring',
        //      }
        // ]
        const buckets = config.getIngestionBuckets();

        const configuredSources = Object.keys(this._ingestionSources);
        return async.each(buckets, (bucket, next) => {
            const { zenkoBucket } = bucket;
            // if the zenko bucket has already been setup and is already
            // active, stop tracking bucket from `currentActiveSources`
            const isAlreadyActive =
                configuredSources.indexOf(zenkoBucket) >= 0;
            if (isAlreadyActive) {
                // Get ingestion source information
                const sourceInfo = this._getSourceInformation(bucket);
                // remove from currentActiveSources
                configuredSources.splice(
                    configuredSources.indexOf(zenkoBucket), 1);
                // check/refresh the IngestionReader if any changes
                return this._ingestionSources[zenkoBucket].refresh(
                    sourceInfo, next);
            }

            // Get ingestion source information
            const sourceInfo = this._getSourceInformation(bucket);
            this.addNewLogSource(sourceInfo);
            return next();
        }, err => {
            if (err) {
                return cb(err);
            }
            // Any leftover `currentActiveSources` are no longer active and
            // must be removed (including zookeeper state)
            configuredSources.forEach(source => {
                this._closeLogState(source);
            });
            return cb();
        });
    }

    /**
     * Get necessary information for setting up a new IngestionReader or
     * updating an existing IngestionReader
     * @param {Object} ingestionInfo - ingestion bucket/location info
     * @return {Object} new source config details
     */
    _getSourceInformation(ingestionInfo) {
        const {
            accessKey, secretKey, endpoint, locationType,
            bucketName, zenkoBucket, locationConstraint,
        } = ingestionInfo;

        const urlObject = url.parse(endpoint);
        // TODO: will change for non-ring sources
        const auth = { accessKey, secretKey };

        return {
            // target zenko bucket name
            name: zenkoBucket,
            // source bucket name to ingest from
            bucket: bucketName,
            // source (s3c) endpoint
            host: urlObject.hostname,
            port: parseInt(urlObject.port, 10) || 80,
            https: urlObject.protocol.startsWith('https'),
            type: locationType,
            locationConstraint,
            auth,
        };
    }

    /**
     * add an Ingestion Reader
     * @param {Object} source - new source object
     * @return {undefined}
     */
    addNewLogSource(source) {
        const zenkoBucket = source.name;

        this.log.debug('add an ingestion reader', {
            zenkoBucket,
            method: 'IngestionPopulator.addNewLogSource',
        });

        const newRaftReader = new IngestionReader({
            zkClient: this.zkClient,
            ingestionConfig: this.ingestionConfig,
            kafkaConfig: this.kafkaConfig,
            bucketdConfig: source,
            logger: this.log,
            extensions: [this._extension],
            producer: this._producer,
            metricsProducer: this._mProducer,
            qpConfig: this.qpConfig,
            s3Config: this.s3Config,
        });

        // add to active ingestion sources list
        this._ingestionSources[zenkoBucket] = newRaftReader;
        // add to log readers update list
        this.logReadersUpdate.push(newRaftReader);
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
            instanceId: config.getPublicInstanceId(),
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
        async.each(newReaders, (logReader, cb) => logReader.setup(err => {
            if (err) {
                // if setup fails for a log reader, don't add it to `logReaders`
                // log the error and continue setting up others
                this.log.fatal('error setting up log reader', {
                    method: 'IngestionPopulator._setupUpdatedReaders',
                    error: err,
                });
            } else {
                this.logReaders.push(logReader);
            }
            return cb();
        }), done);
    }

    /**
     * Remove paused ingestion readers from this.logReaders
     * @return {undefined}
     */
    _removePausedReaders() {
        const locationsToPause = Object.keys(this._pausedLocations);

        this.log.debug('removing paused readers before processing log entries', {
            method: 'IngestionPopulator._removePausedReaders',
            locationsToPause,
        });

        locationsToPause.forEach(location => {
            // if IngestionReader is a paused location, remove from active
            // IngestionReader list: this.logReaders
            this.logReaders.forEach(lr => {
                if (lr.getLocationConstraint() === location) {
                    this._checkAndRemoveLogReader(
                        lr.getTargetZenkoBucketName(), this.logReaders);
                }
            });
        });
    }

    /**
     * Close IngestionPopulator and does not remove ZooKeeper state
     * @param {Function} done - callback(error)
     * @return {undefined}
     */
    close(done) {
        async.series([
            next => {
                if (this._mProducer) {
                    this.log.debug('closing metrics producer', {
                        method: 'IngestionPopulator.close',
                    });
                    return this._mProducer.close(next);
                }
                this.log.debug('no metrics producer to close', {
                    method: 'IngestionPopulator.close',
                });
                return next();
            },
            next => {
                if (this._mConsumer) {
                    this.log.debug('closing metrics consumer', {
                        method: 'IngestionPopulator.close',
                    });
                    return this._mConsumer.close(next);
                }
                this.log.debug('no metrics consumer to close', {
                    method: 'IngestionPopulator.close',
                });
                return next();
            },
            next => {
                if (this._producer) {
                    this.log.debug('closing producer', {
                        method: 'IngestionPopulator.close',
                    });
                    return this._producer.close(next);
                }
                this.log.debug('no producer to close', {
                    method: 'IngestionPopulator.close',
                });
                return next();
            },
        ], done);
    }

    _removeReaderState(reader, path) {
        const interval = setInterval(() => {
            if (!reader.isBatchInProgress()) {
                this.zkClient.removeRecur(path, err => {
                    if (!err) {
                        clearInterval(interval);
                    }
                });
            }
        }, 2000);
    }

    /**
     * Remove an IngestionReader and remove state in ZooKeeper
     * @param {String} key - source key (zenko bucket name)
     * @return {undefined}
     */
    _closeLogState(key) {
        if (this._checkAndRemoveLogReader(key, this.logReadersUpdate)) {
            // if removed from `this.logReadersUpdate`, zookeeper setup has not
            // been performed yet
            this.log.debug('removed ingestion reader from logReadersUpdate', {
                method: 'IngestionPopulator._closeLogState',
                bucket: key,
            });
            delete this._ingestionSources[key];
        }
        if (this._checkAndRemoveLogReader(key, this.logReaders)) {
            // if removed from `this.logReaders`, we must cleanup zookeeper
            // state for this bucket
            this.log.debug('removed ingestion reader from logReaders', {
                method: 'IngestionPopulator._closeLogState',
                bucket: key,
            });
            // we should first validate this reader is not currently processing
            // a batch of entries before removing zookeeper state
            const reader = this._ingestionSources[key];
            delete this._ingestionSources[key];
            const path = `${this.ingestionConfig.zookeeperPath}/${key}`;
            this._removeReaderState(reader, path);
        }
    }

    /**
     * Helper method to check an array of logReaders to remove a given bucket
     * name.
     * @param {String} name - bucket name
     * @param {Array} list - list of logReaders
     * @return {Boolean} true if something was removed from given list
     */
    _checkAndRemoveLogReader(name, list) {
        const index = list.findIndex(lr =>
            lr.getTargetZenkoBucketName() === name);
        if (index !== -1) {
            list.splice(index, 1);
            return true;
        }
        return false;
    }

    _processLogEntries(params, done) {
        const { maxParallelReaders } = this.ingestionConfig;
        return async.eachLimit(this.logReaders, maxParallelReaders,
            (logReader, cb) => {
                if (logReader.isBatchInProgress()) {
                    this.log.debug('ingestion batch still in progress', {
                        zenkoBucket: logReader.getTargetZenkoBucketName(),
                        location: logReader.getLocationConstraint(),
                    });
                    return cb();
                }
                return logReader.processLogEntries(params, error => {
                    if (error) {
                        this.log.warn('error processing log entries', {
                            error,
                            zenkoBucket: logReader.getTargetZenkoBucketName(),
                            location: logReader.getLocationConstraint(),
                        });
                    }
                    // do not escalate error to avoid crashing pod
                    // instead retry on next cron
                    return cb();
                });
            }, done);
    }

    processLogEntries(params, done) {
        return async.series([
            next => {
                if (this.logReadersUpdate.length >= 1) {
                    return this._setupUpdatedReaders(next);
                }
                return process.nextTick(next);
            },
            next => {
                this._removePausedReaders();
                return this._processLogEntries(params, next);
            },
        ], done);
    }

    zkStatus() {
        return this.zkClient.getState();
    }
}

module.exports = IngestionPopulator;
