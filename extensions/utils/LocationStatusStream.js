const { MongoClient } = require('mongodb');
const async = require('async');
const semver = require('semver');

const { locationStatusCollection } = require('../../lib/constants');
const { constructConnectionString, getMongoVersion } = require('./MongoUtils');
const ChangeStream = require('../../lib/wrappers/ChangeStream');

/**
 * Pause a location
 * @typedef { (locationName: string,) => void } PauseLocationFn
 */
/**
 * Resume a location
 * @typedef { (locationName: string) => void } ResumeLocationFn
 */

/**
 * @class LocationStatusStream
 *
 * @classdesc uses change streams to listen to a location statuss
 * for a service (crr, ingestion, lifecycle)
 */
class LocationStatusStream {

    /**
     * @constructor
     * @param {string} serviceName service name (i.e crr, ingestion, lifecycle)
     * @param {Object} mongoConfig mongodb configuration object
     * @param {PauseLocationFn} pauseFn pause location function
     * @param {ResumeLocationFn} resumeFn resume location function
     * @param {Logger} logger logger instance
     */
    constructor(serviceName, mongoConfig, pauseFn, resumeFn, logger) {
        this._serviceName = serviceName;
        this._mongoConfig = mongoConfig;
        this._mongoClient = null;
        this._mongoVersion = null;
        this._locationStatusColl = null;
        this._pauseServiceForLocation = pauseFn;
        this._resumeServiceForLocation = resumeFn;
        this._log = logger;
    }

    /**
     * Set up the mongo client and initiate the change stream
     * @param {function} done - callback
     * @return {undefined}
     */
    start(done) {
        async.series([
            done => this._setupMongoClient(done),
            done => this._setChangeStream(done),
            done => this._initializeLocationStatuses(done),
        ], done);
    }

    /**
     * Connects to MongoDB using and retreives
     * the location status collection
     * @param {function} cb callback
     * @returns {undefined} undefined
     */
    _setupMongoClient(cb) {
        const mongoUrl = constructConnectionString(this._mongoConfig);
        MongoClient.connect(mongoUrl, {
                replicaSet: this._mongoConfig.replicaSet,
                useNewUrlParser: true,
                useUnifiedTopology: true,
        }, (err, client) => {
            if (err) {
                this._log.error('Could not connect to MongoDB', {
                    method: 'ServiceStatusManager._setupMongoClient',
                    error: err.message,
                });
                return cb(err);
            }
            // connect to metadata DB
            this._mongoClient = client.db(this._mongoConfig.database, {
                ignoreUndefined: true,
            });
            this._locationStatusColl = this._mongoClient.collection(locationStatusCollection);
            this._log.info('Connected to MongoDB', {
                method: 'ServiceStatusManager._setupMongoClient',
            });
            // get mongodb version
            getMongoVersion(this._mongoClient, (err, version) => {
                if (err) {
                    this._log.error('Could not get MongoDB version', {
                        method: 'ServiceStatusManager._setupMongoClient',
                        error: err.message,
                    });
                    return cb(err);
                }
                this._mongoVersion = version;
                return cb();
            });
            return undefined;
        });
    }

    /**
     * Initialized the list of paused locations
     * @param {Function} cb callback
     * @returns {undefined}
     */
    _initializeLocationStatuses(cb) {
        this._locationStatusColl.find({})
            .toArray((err, locations) => {
                if (err) {
                    this._log.error('Could not fetch location statuses from mongo', {
                        method: 'ServiceStatusManager._initializeLocationStatuses',
                        error: err.message,
                    });
                    return cb(err);
                }
                locations.forEach(location => {
                    const isPaused = location.value[this._serviceName].paused;
                    if (isPaused) {
                        this._pauseServiceForLocation(location._id);
                    }
                });
                return cb();
            });
    }

    /**
     * Initializes a change stream on the location status collection
     * @param {function} cb callback
     * @returns {undefined}
     */
    _setChangeStream(cb) {
        const changeStreamPipeline = [
            {
                $match: {
                    operationType: {
                        $in: ['update', 'insert', 'replace', 'delete'],
                    },
                },
            },
        ];
        this._changeStreamWrapper = new ChangeStream({
            logger: this._log,
            collection: this._locationStatusColl,
            pipeline: changeStreamPipeline,
            handler: this._handleChangeStreamChangeEvent.bind(this),
            throwOnError: false,
            useStartAfter: semver.gte(this._mongoVersion, '4.2.0'),
        });
        // start watching metastore
        this._changeStreamWrapper.start();
        return cb();
    }

    /**
     * Handler for change stream events
     * Pauses or resumes a location based on the changes that occured
     * in mongo
     * @param {ChangeStreamDocument} changeEvent change stream document
     * @returns {undefined}
     */
    _handleChangeStreamChangeEvent(changeEvent) {
            const location = changeEvent.documentKey._id;
            if (changeEvent.operationType === 'delete') {
                this._resumeServiceForLocation(location);
            } else {
                const status = changeEvent.fullDocument.value[this._serviceName];
                if (status.paused) {
                    this._pauseServiceForLocation(location);
                } else {
                    this._resumeServiceForLocation(location);
                }
            }
            this._log.info('Change stream event processed', {
                method: 'ServiceStatusManager._handleChangeStreamChangeEvent',
                opType: changeEvent.operationType,
                location,
            });
    }
}

module.exports = LocationStatusStream;
