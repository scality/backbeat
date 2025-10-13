const cluster = require('cluster');
const async = require('async');
const schedule = require('node-schedule');
const { errors } = require('arsenal');

const validLocations = require('../../conf/locationConfig.json') || {};
const { locationStatusCollection } = require('../constants');

const actions = {
    deleteSchedule: 'deleteScheduledResumeService',
    pause: 'pauseService',
    resume: 'resumeService',
};

/**
 * Contains methods to incrememt different metrics
 * @typedef {Object} ServiceConfig
 * @property {string} namespace - crr config
 * @property {string} statePath - zookeeper state path
 * @property {string} topic - redis channel name
 * @property {string} isMongo - true if service uses mongo to store status
 */

const invalidStateTransitionError = new Error('Invalid state transition or location already in the desired state');

/**
 * @class LocationStatusManager
 *
 * @classdesc manages the pause/resume status of the locations
 * on different services.
 */
class LocationStatusManager {

    /**
     * @constructor
     * @param {MongoClient} mongoClient mongo client instance
     * @param {zookeeper-client.Client} zkClient zookeeper client instance
     * @param {Redis} redis redis publisher
     * @param {Object} serviceConfig config of each service
     * @param {ServiceConfig} serviceConfig.crr config of crr
     * @param {ServiceConfig} serviceConfig.ingestion config of ingestion
     * @param {ServiceConfig} serviceConfig.lifecycle config of lifecycle
     * @param {Logger} logger logger instance
     */
    constructor(mongoClient, zkClient, redis, serviceConfig, logger) {
        this._mongoClient = mongoClient;
        this._zkClient = zkClient;
        this._redis = redis;
        this._serviceConfig = serviceConfig;
        this._logger = logger;

        // services to initialize in mongo for each location
        this._supportedServices = Object.keys(serviceConfig)
            .filter(svc => serviceConfig[svc].isMongo);

        this._locationStatusColl = null;

        this._scheduledResumeJobs = {
            crr: {},
            ingestion: {},
            lifecycle: {},
        };
    }

    /**
     * Creates mongo collection for
     * storing the location status
     * @param {function} cb callback
     * @return {undefined}
     */
    _initCollection(cb) {
        return this._mongoClient.createCollection(locationStatusCollection)
            .then(() => {
                this._locationStatusColl = this._mongoClient.collection(locationStatusCollection);
                return cb();
            })
            .catch(err => {
                if (err.codeName !== 'NamespaceExists') {
                    this._logger.error('Could not create mongo collection', {
                        method: 'LocationStatusManager._initCollection',
                        collection: locationStatusCollection,
                        error: err.message,
                    });
                    return cb(err);
                }
                this._locationStatusColl = this._mongoClient.collection(locationStatusCollection);
                return cb();
            });
    }

    /**
     * list all location status documents from mongodb
     * @param {function} cb callback
     * @returns {undefined}}
     */
    _listCollectionDocuments(cb) {
        this._locationStatusColl.find({})
            .toArray()
            .then(docs => cb(null, docs))
            .catch(err => {
                this._logger.error('Could not list documents', {
                    method: 'LocationStatusManager._listCollectionDocuments',
                    collection: locationStatusCollection,
                    error: err.message,
                });
                cb(err);
            });
    }

    /**
     * Handles previously scheduled resume jobs for locations
     * @param {Object} locations map of location data from mongo
     * @param {function} cb callback
     * @returns {undefined}
     */
    _handleScheduledResume(locations, cb) {
        async.eachLimit(Object.keys(locations), 10, (loc, next) => {
            const services = Object.keys(locations[loc]);
            services.forEach(svc => {
                if (locations[loc][svc]?.scheduledResume) {
                    this._scheduleResumeJob(loc, svc, new Date(locations[loc][svc].scheduledResume));
                }
            });
            return next();
        }, err => {
            if (err) {
                this._logger.error('Could not handle scheduled resume jobs', {
                    method: 'LocationStatusManager._handleScheduledResume',
                    error: err.message,
                });
                return cb(err);
            }
            return cb(null, locations);
        });
    }

    /**
     * remove invalid locations from mongo
     * @param {Object[]} locations list of locations data from mongo
     * @param {function} cb callback
     * @returns {undefined}
     */
    _deleteInvalidLocations(locations, cb) {
        const validMongoLocations = {};
        const invalidMongoLocationNames = [];
        locations.forEach(loc => {
            !validLocations[loc._id] ?
                invalidMongoLocationNames.push(loc._id) : validMongoLocations[loc._id] = loc.value;
        });
        return this._locationStatusColl.deleteMany({
            _id: {
                $in: invalidMongoLocationNames
            }
        })
        .then(() => cb(null, validMongoLocations))
        .catch(err => {
            this._logger.error('Could not delete invalid locations', {
                method: 'LocationStatusManager._deleteInvalidLocations',
                error: err.message,
            });
            cb(err);
        });
    }

    /**
     * Returns the initial state object for a location
     * @param {String[]} servicesToInit
     * @returns {Object} initial state
     */
    _getInitState(servicesToInit) {
        const state = {
            crr: null,
            ingestion: null,
            lifecycle: null,
        };
        servicesToInit.forEach(svc => {
            state[svc] = {
                paused: false,
                scheduledResume: null,
            };
        });
        return state;
    }

    /**
     * Adds newly added locations to
     * mongo and the local store
     * @param {Object} locations Map of location data from mongo
     * @param {function} cb callback
     * @returns {undefined}
     */
    _addNewLocations(locations, cb) {
        const newLocations = Object.keys(validLocations).filter(loc => !locations[loc]);
        async.eachLimit(newLocations, 10, (location, next) => {
            this._locationStatusColl.insertOne({
                _id: location,
                value: this._getInitState(this._supportedServices),
            }).then(() => next()).catch(next);
        }, err => {
            if (err) {
                this._logger.error('Could not add new locations', {
                    method: 'LocationStatusManager._addNewLocations',
                    error: err.message,
                });
                return cb(err);
            }
            return cb(null);
        });
    }

    /**
     * Initialize the status of locations stored in mongo
     * @param {function} cb callback
     * @return {undefined}
     */
    _setupLocationStatusStore(cb) {
        this._initCollection(err => {
            if (err) {
                return cb(err);
            }
            if (cluster.isWorker) {
                return cb();
            }
            return async.waterfall([
                done => this._listCollectionDocuments(done),
                (locations, done) => this._deleteInvalidLocations(locations, done),
                (locations, done) => this._handleScheduledResume(locations, done),
                (locations, done) => this._addNewLocations(locations, done),
            ], err => {
                if (err) {
                    this._logger.error('Could not setup location statuses in mongo', {
                        method: 'LocationStatusManager._setupLocationStatusStore',
                        error: err.message,
                    });
                    return cb(err);
                }
                this._logger.info('Locations status setup complete', {
                    method: 'LocationStatusManager._setupLocationStatusStore',
                });
                return cb();
            });
        });
    }

    /**
     * publish an action to redis channel
     * @param {string} service service name
     * @param {string} location location name
     * @param {string} action action name
     * @param {Date} [schedule] scheduled resume date
     * @returns {undefined}
     */
    _pushActionToRedis(service, location, action, schedule = null) {
        const topic = this._serviceConfig[service].topic;
        const channel = `${topic}-${location}`;
        const message = {
            action,
        };
        if (schedule) {
            message.date = schedule;
        }
        this._redis.publish(channel, JSON.stringify(message));
    }

    /**
     * Updates a location's pause/resume status
     * in mongo
     * @param {object} query query to find location to update
     * @param {object} conditions conditional update query
     * @param {object} update update query
     * @param {callback} cb callback
     * @returns {undefined}
     */
    _updateServiceStatusForLocation(query, update, cb) {
        this._locationStatusColl.updateOne(
            query,
            update,
            { upsert: false })
            .then(res => {
                if (res.modifiedCount === 0) {
                    this._logger.warn('Invalid state transition or location already in the desired state', {
                        method: 'LocationStatusManager._updateServiceStatusForLocation',
                        location,
                        service,
                    });
                    return cb(invalidStateTransitionError);
                }
                return cb();
            })
            .catch(err => {
                this._logger.error('Could not update location service status in MongoDB', {
                    method: 'LocationStatusManager._updateServiceStatusForLocation',
                    error: err.message,
                });
                return cb(err);
            });
    }

    /**
     * Get state details for a service from MongoDB
     * @param {string} service service name
     * @param {string[]} locations location names
     * @param {Function} cb callback
     * @returns {undefined}
     */
    _getMongoStateDetails(service, locations, cb) {
        this._locationStatusColl.find(
            { _id: { $in: locations } },
            { projection: { _id: 1, [`value.${service}`]: 1 } }
        )
        .toArray()
        .then(docs => {
            const states = docs.reduce((acc, doc) => {
                if (doc.value?.[service]) {
                    return { ...acc, [doc._id]: doc.value[service] };
                }
                return acc;
            }, {});
            return cb(null, states);
        })
        .catch(err => {
            this._logger.error('Could not get location service status from MongoDB', {
                method: 'LocationStatusManager._getMongoStateDetails',
                error: err.message,
            });
            return cb(err);
        });
    }

    /**
     * Helper method to get zookeeper state details for given location(s)
     * @param {string} service service name, can be one of "lifecycle", "crr", "ingestion"
     * @param {string[]} locations location names to get
     * @param {Function} cb callback(error, stateBySite)
     * @return {undefined}
     */
    _getZkStateDetails(service, locations, cb) {
        const stateBySite = {};
        async.each(locations, (location, next) => {
            const zkNamespace = this._serviceConfig[service].namespace;
            const zkStatePath = this._serviceConfig[service].statePath;
            const path = `${zkNamespace}${zkStatePath}/${location}`;
            this._zkClient.getData(path, (err, data) => {
                if (err) {
                    return next(err);
                }
                try {
                    const d = JSON.parse(data.toString());
                    stateBySite[location] = d;
                } catch (e) {
                    return next(e);
                }
                return next();
            });
        }, error => {
            if (error) {
                let errMessage = 'error getting state node details';
                if (error.name === 'NO_NODE') {
                    errMessage = 'zookeeper path was not created on queue ' +
                        'processor start up';
                }
                this._logger.error(errMessage, {
                    method: 'LocationStatusManager._getZkStateDetails',
                    error,
                });
                return cb(errors.InternalError);
            }
            return cb(null, stateBySite);
        });
    }

    /**
     * Calls the correct location state getter based
     * on where the service state is stored
     * @param {string} service service to pause for locations
     * @param {string[]} locations location names
     * @param {Function} cb callback
     * @returns {undefined}
     */
    _getStateDetails(service, locations, cb) {
        if (!this._serviceConfig[service] || this._serviceConfig[service].isMongo) {
            return this._getMongoStateDetails(service, locations, cb);
        }
        return this._getZkStateDetails(service, locations, cb);
    }

    /**
     * Get status of service for one or multiple locations
     * @param {string} service service to pause for locations
     * @param {string[]} locations location names to pause
     * @param {Function} cb callback
     * @returns {undefined}
     */
    getServiceStatus(service, locations, cb) {
        this._getStateDetails(service, locations, (err, data) => {
            if (err) {
                return cb(err);
            }
            const statuses = {};
            Object.keys(data).forEach(location => {
                statuses[location] = data[location].paused ? 'disabled' : 'enabled';
            });
            return cb(null, statuses);
        });
    }

    /**
     * Get scheduled resume of service for one or multiple locations
     * @param {string} service service to pause for locations
     * @param {string[]} locations location names to pause
     * @param {Function} cb callback
     * @returns {undefined}
     */
    getResumeSchedule(service, locations, cb) {
        this._getStateDetails(service, locations, (err, data) => {
            if (err) {
                return cb(err);
            }
            const schedules = {};
            Object.keys(data).forEach(location => {
                schedules[location] = data[location].scheduledResume || 'none';
            });
            return cb(null, schedules);
        });
    }

    /**
     * Removes previously scheduled resume job
     * @param {string} location location name
     * @param {string} service service name
     * @returns {undefined}
     */
    _deleteResumeJob(location, service) {
        const prvSchedule = this._scheduledResumeJobs[service][location];
        if (prvSchedule) {
            prvSchedule.cancel();
            delete this._scheduledResumeJobs[service][location];
        }
    }

    /**
     * Validate that the POST request body has the necessary content
     * @param {String} body the POST request body string
     * @return {Object} object containing an error and the request body
     */
    _parseScheduleResumeBody(body) {
        const msg = 'The body of your POST request is not well-formed';
        let reqBody;
        const defaultRes = { hours: 6 };
        if (!body) {
            return defaultRes;
        }
        try {
            reqBody = JSON.parse(body);
            // default 6 hours if no body value sent by user
            if (reqBody.hours === undefined || reqBody.hours === null) {
                return defaultRes;
            }
            if (reqBody.hours === '' || isNaN(reqBody.hours) ||
                parseInt(reqBody.hours, 10) <= 0) {
                return {
                    error: errors.MalformedPOSTRequest.customizeDescription(
                        `${msg}: hours must be an integer greater than 0`),
                };
            }
            return { hours: parseInt(reqBody.hours, 10) };
        } catch (e) {
            this._logger.error('Error parsing request body', {
                method: 'LocationStatusManager._parseScheduleResumeBody',
                error: e.message,
            });
            return {
                error: errors.MalformedPOSTRequest.customizeDescription(msg),
            };
        }
    }

    /**
     * Schedules resume of a location status for a service
     * @param {string} location location name
     * @param {string} service service name
     * @param {Date} date resume date
     * @returns {undefined}
     */
    _scheduleResumeJob(location, service, date) {
        function triggerResume() {
            const query = {
                _id: location,
                [`value.${service}.paused`]: true,
                [`value.${service}.scheduledResume`]: { $eq: date }
            };
            const update = {
                $set: {
                    [`value.${service}`]: {
                        paused: false,
                        scheduledResume: null,
                    }
                }
            };
            this._updateServiceStatusForLocation(query, update, err => {
                if (err) {
                    if (err === invalidStateTransitionError) {
                        return;
                    }
                    this._logger.error('error resuming scheduled job, retrying in 1min', {
                        method: 'LocationStatusManager._scheduleResumeJob',
                        error: err,
                        location,
                    });
                    // if an error occurs, need to retry
                    // for now, schedule minute from now
                    const date = new Date();
                    date.setMinutes(date.getMinutes() + 1);
                    this._scheduledResumeJobs[service][location] = schedule.scheduleJob(date,
                        triggerResume.bind(this));
                }
                this._logger.info('location resumed', {
                    method: 'LocationStatusManager._scheduleResumeJob',
                    location,
                    service,
                });
            });
        }
        this._deleteResumeJob(location, service);
        if (new Date() > date) {
            triggerResume.bind(this)();
        } else {
            this._scheduledResumeJobs[service][location] = schedule.scheduleJob(date,
                triggerResume.bind(this));
            }
    }

    /**
     * Deletes the scheduled resume of service for one or more locations
     * @param {string} service service to pause for locations
     * @param {string[]} locations location names
     * @param {Function} cb callback
     * @returns {undefined}
     */
    deleteScheduledResumeService(service, locations, cb) {
        return async.eachLimit(locations, 10, (location, next) => {
            if (this._serviceConfig[service] && !this._serviceConfig[service].isMongo) {
                this._pushActionToRedis(service, location, actions.deleteSchedule);
                return next();
            }
            const query = {
                _id: location,
                [`value.${service}.paused`]: true,
                [`value.${service}.scheduledResume`]: { $ne: null }
            };
            const update = {
                $set: {
                    [`value.${service}`]: {
                        paused: true,
                        scheduledResume: null,
                    }
                }
            };
            this._deleteResumeJob(location, service);
            return this._updateServiceStatusForLocation(query, update, next);
        }, err => {
            if (err) {
                const errMsg = `failed to delete scheduled resume for locations: ${locations}`;
                this._logger.error(errMsg, {
                    method: 'LocationStatusManager.deleteScheduledResumeService',
                    error: err.message,
                    service,
                });
                return cb(null, errors.InternalError.customizeDescription(err.message));
            }
            this._logger.info(`deleted scheduled resume for locations: ${locations}`, {
                method: 'LocationStatusManager.deleteScheduledResumeService',
                service,
            });
            return cb(null, {});
        });
    }

    /**
     * Pauses a service for one or multiple locations
     * @param {string} service service to pause for locations
     * @param {string[]} locations location names to pause
     * @param {Function} cb callback
     * @returns {undefined}
     */
    pauseService(service, locations, cb) {
        async.eachLimit(locations, 10, (location, next) => {
            if (this._serviceConfig[service] && !this._serviceConfig[service].isMongo) {
                this._pushActionToRedis(service, location, actions.pause);
                return next();
            }
            const query = {
                _id: location,
                [`value.${service}.paused`]: false,
            };
            const update = {
                $set: {
                    [`value.${service}`]: {
                        paused: true,
                        scheduledResume: null,
                    }
                },
            };
            return this._updateServiceStatusForLocation(query, update, next);
        }, err => {
            if (err) {
                const errMsg = `failed to pause ${service} service for locations: ${locations}`;
                this._logger.error(errMsg, {
                    method: 'LocationStatusManager.pauseService',
                    error: err.message,
                    service,
                });
                return cb(null, errors.InternalError.customizeDescription(err.message));
            }
            this._logger.info(`${service} service paused for locations: ${locations}`, {
                method: 'LocationStatusManager.pauseService',
                service,
            });
            return cb(null, {});
        });
    }

    /**
     * Resumes a service for one or multiple locations
     * @param {string} service service to resume for locations
     * @param {string[]} locations location names to resume
     * @param {boolean|undefined} isScheduled true if the
     * resume should be scheduled
     * @param {string} body the POST request body string
     * @param {Function} cb callback
     * @returns {undefined}
     */
    resumeService(service, locations, isScheduled, body, cb) {
        let schedule;

        if (typeof isScheduled === 'boolean') {
            if (!isScheduled) {
                // escalate error
                this._logger.error('error scheduling resume, wrong route path');
                return cb(errors.RouteNotFound);
            }
            // parse body and handle scheduling
            const { error, hours } = this._parseScheduleResumeBody(body);
            if (error) {
                return cb(error);
            }
            schedule = new Date();
            schedule.setHours(schedule.getHours() + hours);
        }

        return async.eachLimit(locations, 10, (location, next) => {
            if (this._serviceConfig[service] && !this._serviceConfig[service].isMongo) {
                this._pushActionToRedis(service, location, actions.resume, schedule);
                return next();
            }
            const query = {
                _id: location,
                [`value.${service}.paused`]: true,
            };
            const update = {
                $set: {
                    [`value.${service}`]: {
                        paused: !!schedule,
                        scheduledResume: schedule || null,
                    }
                }
            };
            return this._updateServiceStatusForLocation(query, update, err => {
                if (err) {
                    return next(err);
                }
                if (schedule) {
                    this._scheduleResumeJob(location, service, schedule);
                }
                return next();
            });
        }, err => {
            if (err) {
                const errMsg = `failed to resume ${service} service for locations: ${locations}`;
                this._logger.error(errMsg, {
                    method: 'LocationStatusManager.resumeService',
                    error: err.message,
                    service,
                });
                return cb(null, errors.InternalError.customizeDescription(err.message));
            }
            const logMsg = schedule ? `${service} for locations ${locations} scheduled ` +
                `to resume at a later time: ${schedule}` : `${service} resumed for locations ${locations}`;
            this._logger.info(logMsg, {
                method: 'LocationStatusManager.resumeService',
                service,
            });
            return cb(null, {});
        });
    }
}

module.exports = LocationStatusManager;
