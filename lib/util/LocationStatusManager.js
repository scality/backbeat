const async = require('async');
const schedule = require('node-schedule');
const { errors } = require('arsenal');
const LocationStatus = require('../models/LocationStatus');

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

        this._locationStatusStore = {};
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
     * Add valid mongo locations to local store
     * @param {Object[]} locations list of locations data from mongo
     * @param {function} cb callback
     * @returns {undefined}
     */
    _getPreviousLocationStates(locations, cb) {
        const validLocationNames = Object.keys(validLocations);
        locations
            .filter(loc => validLocationNames.includes(loc._id))
            .forEach(loc => {
                this._locationStatusStore[loc._id] = new LocationStatus(this._supportedServices, loc.value);
                // schedule jobs if any
                Object.keys(loc.value).forEach(service => {
                    const date = loc.value[service] && loc.value[service].scheduledResume;
                    if (date) {
                        this._scheduleResumeJob(loc._id, service, new Date(date));
                    }
                });
            });
        return cb(null, locations);
    }

    /**
     * remove invalid locations from mongo
     * @param {Object[]} locations list of locations data from mongo
     * @param {function} cb callback
     * @returns {undefined}
     */
    _deleteInvalidLocations(locations, cb) {
        const validLocationNames = Object.keys(validLocations);
        const invalidLocations = locations
            .filter(loc => !validLocationNames.includes(loc._id))
            .map(loc => loc._id);
        return this._locationStatusColl.deleteMany({
            _id: {
                $in: invalidLocations
            }
        })
        .then(() => cb(null, locations))
        .catch(err => {
            this._logger.error('Could not delete invalid locations', {
                method: 'LocationStatusManager._deleteInvalidLocations',
                error: err.message,
            });
            cb(err);
        });
    }

    /**
     * Adds newly added locations to
     * mongo and the local store
     * @param {Object[]} locations list of locations data from mongo
     * @param {function} cb callback
     * @returns {undefined}
     */
    _addNewLocations(locations, cb) {
        const validLocationNames = Object.keys(validLocations);
        const mongoLocations = locations.map(loc => loc._id);
        const newLocations = validLocationNames
            .filter(loc => !mongoLocations.includes(loc));
        async.eachLimit(newLocations, 10, (location, next) => {
            const locationConfig = new LocationStatus(this._supportedServices);
            this._locationStatusStore[location] = locationConfig;
            this._locationStatusColl.insertOne({
                _id: location,
                value: locationConfig.getValue(),
            }).then(() => next()).catch(next);
        }, err => {
            if (err) {
                this._logger.error('Could not add new locations', {
                    method: 'LocationStatusManager._addNewLocations',
                    error: err.message,
                });
                return cb(err);
            }
            return cb(null, locations);
        });
    }

    /**
     * Initialize the status of locations stored in mongo
     * @param {function} cb callback
     * @return {undefined}
     */
    _setupLocationStatusStore(cb) {
        async.waterfall([
            done => this._initCollection(done),
            done => this._listCollectionDocuments(done),
            (locations, done) => this._getPreviousLocationStates(locations, done),
            (locations, done) => this._deleteInvalidLocations(locations, done),
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
     * in mongo based on local values
     * @param {string} location location name
     * @param {callback} cb callack
     * @returns {undefined}
     */
    _updateServiceStatusForLocation(location, cb) {
        this._locationStatusColl.updateOne(
            { _id: location },
            {
                $set: {
                    _id: location,
                    value: this._locationStatusStore[location].getValue(),
                }
            },
            { upsert: false })
            .then(() => cb())
            .catch(err => {
                if (err) {
                    this._logger.error('Could not update location service status in MongoDB', {
                        method: 'LocationStatusManager._updateServiceStatusForLocation',
                        error: err.message,
                    });
                    return cb(err);
                }
                return cb();
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
     * Pauses a service for one or multiple locations
     * @param {string} service service to pause for locations
     * @param {string[]} locations location names to pause
     * @param {Function} cb callback
     * @returns {undefined}
     */
    pauseService(service, locations, cb) {
        // Migrate to use transactional approach by default
        return this.pauseServiceTransactional(service, locations, cb);
    }

    /**
     * Resumes a service for one or multiple locations  
     * MIGRATED: Now uses backend-first transactional approach
     * @param {string} service service to resume for locations
     * @param {string[]} locations location names to resume
     * @param {boolean|undefined} isScheduled true if the resume should be scheduled
     * @param {string} body the POST request body string
     * @param {Function} cb callback
     * @returns {undefined}
     */
    resumeService(service, locations, isScheduled, body, cb) {
        // Migrate to use transactional approach by default
        return this.resumeServiceTransactional(service, locations, isScheduled, body, cb);
    }

    /**
     * Get Service Status (Pause/Resume)
     * MIGRATED: Now uses backend-first transactional approach
     * @param {string} service service to check status for
     * @param {string[]} locations location names to check
     * @param {Function} cb callback
     * @returns {undefined}
     */
    getServiceStatus(service, locations, cb) {
        // Migrate to use transactional approach by default
        return this.getServiceStatusTransactional(service, locations, cb);
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
            if (!reqBody.hours) {
                return defaultRes;
            }
            if (reqBody.hours === '' || isNaN(reqBody.hours) ||
                parseInt(reqBody.hours, 10) <= 0) {
                return {
                    error: errors.MalformedPOSTrequest.customizeDescription(
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
            this._locationStatusStore[location].resumeLocation(service);
            this._updateServiceStatusForLocation(location, err => {
                if (err) {
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
            const paused = this._locationStatusStore[location].getServicePauseStatus(service);
            if (paused) {
                this._deleteResumeJob(location, service);
                this._locationStatusStore[location].setServiceResumeSchedule(service, null);
                return this._updateServiceStatusForLocation(location, next);
            }
            return next();
        }, err => {
            if (err) {
                const errMsg = `failed to delete scheduled resume for locations: ${locations}`;
                this._logger.error(errMsg, {
                    method: 'LocationStatusManager.deleteScheduledResumeService',
                    error: err.message,
                    service,
                });
                return cb(null, errors.InternalError.customizeDescription(errMsg));
            }
            this._logger.info(`deleted scheduled resume for locations: ${locations}`, {
                method: 'LocationStatusManager.deleteScheduledResumeService',
                service,
            });
            return cb(null, {});
        });
    }

    /**
     * Transactional pause service operation - backend-first approach
     * @param {string} service service to pause for locations
     * @param {string[]} locations location names to pause
     * @param {Function} cb callback
     * @returns {undefined}
     */
    pauseServiceTransactional(service, locations, cb) {
        async.eachLimit(locations, 10, (location, next) => {
            if (this._serviceConfig[service] && !this._serviceConfig[service].isMongo) {
                // For ZooKeeper-based services, use optimistic locking
                this._pauseServiceZkTransactional(service, location, next);
            } else {
                // For MongoDB-based services, check paused state first, then use atomic operations
                const paused = this._locationStatusStore[location].getServicePauseStatus(service);
                if (!paused) {
                    this._pauseServiceMongoTransactional(service, location, next);
                } else {
                    // Already paused - skip
                    return next();
                }
            }
        }, err => {
            if (err) {
                const errMsg = `failed to pause ${service} service for locations: ${locations}`;
                this._logger.error(errMsg, {
                    method: 'LocationStatusManager.pauseServiceTransactional',
                    error: err.message,
                    service,
                });
                return cb(null, errors.InternalError.customizeDescription(errMsg));
            }
            this._logger.info(`${service} service paused for locations: ${locations}`, {
                method: 'LocationStatusManager.pauseServiceTransactional',
                service,
            });
            return cb(null, {});
        });
    }

    /**
     * Atomic MongoDB pause operation with optimistic locking
     * @param {string} service service name
     * @param {string} location location name
     * @param {Function} cb callback
     * @returns {undefined}
     */
    _pauseServiceMongoTransactional(service, location, cb) {
        // Use MongoDB's atomic findOneAndUpdate with current state as precondition
        const filter = { _id: location };
        const currentServicePath = `value.${service}.paused`;
        
        // Only update if currently not paused (or if service doesn't exist)
        const updateFilter = {
            ...filter,
            $or: [
                { [currentServicePath]: { $ne: true } },
                { [currentServicePath]: { $exists: false } }
            ]
        };

        const update = {
            $set: {
                [`value.${service}.paused`]: true,
                [`value.${service}.scheduledResume`]: null
            }
        };

        this._locationStatusColl.findOneAndUpdate(
            updateFilter,
            update,
            { 
                upsert: false,
                returnDocument: 'after'
            }
        ).then(result => {
            if (!result.value) {
                // Document wasn't updated - either already paused or doesn't exist
                this._logger.debug(`Service ${service} for location ${location} was already paused or doesn't exist`);
                return cb();
            }
            
            // Update in-memory state to match backend
            this._locationStatusStore[location] = new LocationStatus(this._supportedServices, result.value.value);
            
            // Send Redis notification to consumers
            this._pushActionToRedis(service, location, actions.pause);
            return cb();
        }).catch(err => {
            this._logger.error('Failed to pause service atomically', {
                method: 'LocationStatusManager._pauseServiceMongoTransactional',
                error: err.message,
                service,
                location,
            });
            return cb(err);
        });
    }

    /**
     * ZooKeeper pause operation with optimistic locking using version
     * @param {string} service service name
     * @param {string} location location name
     * @param {Function} cb callback
     * @returns {undefined}
     */
    _pauseServiceZkTransactional(service, location, cb) {
        const zkNamespace = this._serviceConfig[service].namespace;
        const zkStatePath = this._serviceConfig[service].statePath;
        const path = `${zkNamespace}${zkStatePath}/${location}`;

        this._zkClient.getData(path, (err, data, stat) => {
            if (err) {
                this._logger.error('Failed to read ZooKeeper state for pause', {
                    method: 'LocationStatusManager._pauseServiceZkTransactional',
                    error: err.message,
                    path,
                });
                return cb(err);
            }

            try {
                const currentState = JSON.parse(data.toString());
                
                // Check if already paused
                if (currentState.paused === true) {
                    this._logger.debug(`Service ${service} for location ${location} was already paused`);
                    return cb();
                }

                // Update state
                const newState = {
                    ...currentState,
                    paused: true,
                    scheduledResume: null
                };

                // Use version-based optimistic locking
                this._zkClient.setData(path, Buffer.from(JSON.stringify(newState)), stat.version, err => {
                    if (err) {
                        if (err.name === 'BAD_VERSION') {
                            // Retry with exponential backoff
                            setTimeout(() => this._pauseServiceZkTransactional(service, location, cb), 
                                Math.random() * 100);
                            return;
                        }
                        this._logger.error('Failed to update ZooKeeper state for pause', {
                            method: 'LocationStatusManager._pauseServiceZkTransactional',
                            error: err.message,
                            path,
                        });
                        return cb(err);
                    }

                    // Send Redis notification to consumers
                    this._pushActionToRedis(service, location, actions.pause);
                    return cb();
                });
            } catch (parseErr) {
                this._logger.error('Failed to parse ZooKeeper state data', {
                    method: 'LocationStatusManager._pauseServiceZkTransactional',
                    error: parseErr.message,
                    path,
                });
                return cb(parseErr);
            }
        });
    }

    /**
     * Transactional resume service operation - backend-first approach
     * @param {string} service service to resume for locations
     * @param {string[]} locations location names to resume
     * @param {boolean|undefined} isScheduled true if the resume should be scheduled
     * @param {string} body the POST request body string
     * @param {Function} cb callback
     * @returns {undefined}
     */
    resumeServiceTransactional(service, locations, isScheduled, body, cb) {
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
                this._resumeServiceZkTransactional(service, location, schedule, next);
            } else {
                // For MongoDB-based services, check paused state first, then use atomic operations
                const paused = this._locationStatusStore[location].getServicePauseStatus(service);
                if (paused) {
                    this._locationStatusStore[location].resumeLocation(service, schedule);
                    if (schedule) {
                        this._scheduleResumeJob(location, service, schedule);
                    }
                    return this._resumeServiceMongoTransactional(service, location, schedule, next);
                }
                // Not paused - skip
                return next();
            }
        }, err => {
            if (err) {
                const errMsg = `failed to resume ${service} service for locations: ${locations}`;
                this._logger.error(errMsg, {
                    method: 'LocationStatusManager.resumeServiceTransactional',
                    error: err.message,
                    service,
                });
                return cb(null, errors.InternalError.customizeDescription(errMsg));
            }
            const logMsg = schedule ? `${service} for locations ${locations} scheduled ` +
                `to resume at a later time: ${schedule}` : `${service} resumed for locations ${locations}`;
            this._logger.info(logMsg, {
                method: 'LocationStatusManager.resumeServiceTransactional',
                service,
            });
            return cb(null, {});
        });
    }

    /**
     * Atomic MongoDB resume operation
     * @param {string} service service name
     * @param {string} location location name
     * @param {Date} schedule optional schedule date
     * @param {Function} cb callback
     * @returns {undefined}
     */
    _resumeServiceMongoTransactional(service, location, schedule, cb) {
        const filter = { _id: location };
        const currentServicePath = `value.${service}.paused`;
        
        // Only update if currently paused
        const updateFilter = {
            ...filter,
            [currentServicePath]: true
        };

        const update = {
            $set: {
                [`value.${service}.paused`]: false,
                [`value.${service}.scheduledResume`]: schedule || null
            }
        };

        this._locationStatusColl.findOneAndUpdate(
            updateFilter,
            update,
            { 
                upsert: false,
                returnDocument: 'after'
            }
        ).then(result => {
            if (!result.value) {
                this._logger.debug(`Service ${service} for location ${location} was already resumed or doesn't exist`);
                return cb();
            }
            
            // Update in-memory state to match backend
            this._locationStatusStore[location] = new LocationStatus(this._supportedServices, result.value.value);
            
            // Handle scheduling if needed
            if (schedule) {
                this._scheduleResumeJob(location, service, schedule);
            }
            
            // Send Redis notification to consumers
            this._pushActionToRedis(service, location, actions.resume, schedule);
            return cb();
        }).catch(err => {
            this._logger.error('Failed to resume service atomically', {
                method: 'LocationStatusManager._resumeServiceMongoTransactional',
                error: err.message,
                service,
                location,
            });
            return cb(err);
        });
    }

    /**
     * ZooKeeper resume operation with optimistic locking
     * @param {string} service service name
     * @param {string} location location name
     * @param {Date} schedule optional schedule date
     * @param {Function} cb callback
     * @returns {undefined}
     */
    _resumeServiceZkTransactional(service, location, schedule, cb) {
        const zkNamespace = this._serviceConfig[service].namespace;
        const zkStatePath = this._serviceConfig[service].statePath;
        const path = `${zkNamespace}${zkStatePath}/${location}`;

        this._zkClient.getData(path, (err, data, stat) => {
            if (err) {
                this._logger.error('Failed to read ZooKeeper state for resume', {
                    method: 'LocationStatusManager._resumeServiceZkTransactional',
                    error: err.message,
                    path,
                });
                return cb(err);
            }

            try {
                const currentState = JSON.parse(data.toString());
                
                // Check if already resumed
                if (currentState.paused !== true) {
                    this._logger.debug(`Service ${service} for location ${location} was already resumed`);
                    return cb();
                }

                // Update state
                const newState = {
                    ...currentState,
                    paused: false,
                    scheduledResume: schedule || null
                };

                // Use version-based optimistic locking
                this._zkClient.setData(path, Buffer.from(JSON.stringify(newState)), stat.version, err => {
                    if (err) {
                        if (err.name === 'BAD_VERSION') {
                            // Retry with exponential backoff
                            setTimeout(() => this._resumeServiceZkTransactional(service, location, schedule, cb), 
                                Math.random() * 100);
                            return;
                        }
                        this._logger.error('Failed to update ZooKeeper state for resume', {
                            method: 'LocationStatusManager._resumeServiceZkTransactional',
                            error: err.message,
                            path,
                        });
                        return cb(err);
                    }

                    // Send Redis notification to consumers
                    this._pushActionToRedis(service, location, actions.resume, schedule);
                    return cb();
                });
            } catch (parseErr) {
                this._logger.error('Failed to parse ZooKeeper state data', {
                    method: 'LocationStatusManager._resumeServiceZkTransactional',
                    error: parseErr.message,
                    path,
                });
                return cb(parseErr);
            }
        });
    }

    /**
     * Backend-first service status check that always reads from persistent storage
     * @param {string} service service to check status for
     * @param {string[]} locations location names to check
     * @param {Function} cb callback
     * @returns {undefined}
     */
    getServiceStatusTransactional(service, locations, cb) {
        if (!this._serviceConfig[service] || this._serviceConfig[service].isMongo) {
            // For MongoDB services, read directly from database
            this._getServiceStatusMongo(service, locations, cb);
        } else {
            // For ZooKeeper services, read directly from ZooKeeper
            this._getZkStateDetails(service, locations, (err, data) => {
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
    }

    /**
     * Get service status directly from MongoDB
     * @param {string} service service name
     * @param {string[]} locations location names
     * @param {Function} cb callback
     * @returns {undefined}
     */
    _getServiceStatusMongo(service, locations, cb) {
        this._locationStatusColl.find(
            { _id: { $in: locations } },
            { projection: { _id: 1, [`value.${service}`]: 1 } }
        ).toArray().then(docs => {
            const statuses = {};
            locations.forEach(location => {
                const doc = docs.find(d => d._id === location);
                const serviceData = doc && doc.value && doc.value[service];
                statuses[location] = (serviceData && serviceData.paused) ? 'disabled' : 'enabled';
            });
            
            // Update in-memory cache to match backend
            docs.forEach(doc => {
                if (this._locationStatusStore[doc._id]) {
                    const currentValue = this._locationStatusStore[doc._id].getValue();
                    const updatedValue = { ...currentValue, ...doc.value };
                    this._locationStatusStore[doc._id] = new LocationStatus(this._supportedServices, updatedValue);
                }
            });
            
            return cb(null, statuses);
        }).catch(err => {
            this._logger.error('Failed to get service status from MongoDB', {
                method: 'LocationStatusManager._getServiceStatusMongo',
                error: err.message,
                service,
                locations,
            });
            return cb(err);
        });
    }

    /**
     * Update in-memory cache to match backend state
     * This should only be called after successful backend operations
     * @param {string} location location name
     * @param {Object} backendValue value from backend
     * @returns {undefined}
     */
    _syncCacheFromBackend(location, backendValue) {
        if (backendValue) {
            this._locationStatusStore[location] = new LocationStatus(this._supportedServices, backendValue);
        }
    }

    /**
     * Calls the correct location state getter based on where the service state is stored
     * @param {string} service service to pause for locations
     * @param {string[]} locations location names to pause
     * @param {Function} cb callback
     * @returns {undefined}
     */
    _getStateDetails(service, locations, cb) {
        // Legacy method - redirect to transactional version
        console.warn('DEPRECATED: _getStateDetails is deprecated, use getServiceStatusTransactional instead');
        return this.getServiceStatusTransactional(service, locations, cb);
    }
}

module.exports = LocationStatusManager;
