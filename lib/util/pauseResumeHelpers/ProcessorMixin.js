'use strict'; // eslint-disable-line

const async = require('async');
const schedule = require('node-schedule');

const { errors } = require('arsenal');

const validStateProperties = ['paused', 'scheduledResume'];
const serviceMap = {
    QueueProcessor: 'replication',
    MongoQueueProcessor: 'ingestion',
};

/*
    To use this mixin, must setup Redis on parent class, and
    must add following required methods on parent class:
    - getPauseResumeVars
            return { logger, site, zkClient, consumer }
    - _getZkSiteNode
            return `${zkNamespace}/${zkStatePath}/${site}`
*/

// Mixin for Processor-side pause/resume utilities
const PauseResumeProcessorMixin = {
    /* Helper Methods */

    _getServiceName() {
        const { logger } = this.getPauseResumeVars();
        const name = serviceMap[this.constructor.name];
        if (!name) {
            // for development purpose
            logger.fatal('pause/resume unavailable for this service', {
                method: `${this.constructor.name}._getServiceName`,
                origin: 'lib/util/pauseResumeHelpers/PauseResumeProcessorMixin',
            });
            throw errors.InternalError.customizeDescription(
                'pause/resume unavailable for this service');
        }
        return name;
    },

    _handlePauseResumeRequest(redisClient, message) {
        const { logger, site } = this.getPauseResumeVars();
        const validActions = {
            pauseService: this._pauseService.bind(this),
            resumeService: this._resumeService.bind(this),
            deleteScheduledResumeService:
                this._deleteScheduledResumeService.bind(this),
        };
        try {
            const { action, date } = JSON.parse(message);
            const cmd = validActions[action];
            if (typeof cmd === 'function') {
                cmd(date);
            }
        } catch (e) {
            logger.error('error parsing redis subscribe message', {
                method: `${this.constructor.name}._setupRedis`,
                error: e,
                service: this._getServiceName(),
                site,
            });
        }
    },

    /**
     * Update zookeeper state node for this site-defined Processor
     * @param {String} key - key name to store in zk state node
     * @param {String|Boolean} value - value
     * @param {Function} cb - callback(error)
     * @return {undefined}
     */
    _updateZkStateNode(key, value, cb) {
        const { logger, zkClient } = this.getPauseResumeVars();
        if (!validStateProperties.includes(key)) {
            // for development purpose
            logger.fatal('incorrect zookeeper state property given', {
                method: `${this.constructor.name}._updateZkStateNode`,
                origin: 'lib/util/pauseResumeHelpers/PauseResumeProcessorMixin',
            });
            throw errors.InternalError.customizeDescription(
                'incorrect zookeeper state property given');
        }
        const path = this._getZkSiteNode();
        return async.waterfall([
            next => zkClient.getData(path, (err, data) => {
                if (err) {
                    logger.error('could not get state from zookeeper', {
                        method: `${this.constructor.name}._updateZkStateNode`,
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
                    logger.error('could not parse state data from zookeeper', {
                        method: `${this.constructor.name}._updateZkStateNode`,
                        zookeeperPath: path,
                        error: err,
                    });
                    return next(err);
                }
                return next(null, bufferedData);
            }),
            (data, next) => zkClient.setData(path, data, err => {
                if (err) {
                    logger.error('could not save state data in zookeeper', {
                        method: `${this.constructor.name}._updateZkStateNode`,
                        zookeeperPath: path,
                        error: err,
                    });
                    return next(err);
                }
                return next();
            }),
        ], cb);
    },

    _scheduleResume(date) {
        const { logger, site } = this.getPauseResumeVars();
        function triggerResume() {
            this._updateZkStateNode('scheduledResume', null, err => {
                if (err) {
                    logger.trace('error occurred for resuming a scheduled ' +
                    'resume. Retry again in 1 minute', {
                        method: `${this.constructor.name}._scheduleResume`,
                        service: this._getServiceName(),
                        site,
                    });
                    // if an error occurs, need to retry
                    // for now, schedule 1 minute from now
                    const date = new Date();
                    date.setMinutes(date.getMinutes() + 1);
                    this.scheduledResume = schedule.scheduleJob(date,
                        triggerResume.bind(this));
                } else {
                    if (this.scheduledResume) {
                        this.scheduledResume.cancel();
                    }
                    this.scheduledResume = null;
                    this._resumeService();
                }
            });
        }

        this._updateZkStateNode('scheduledResume', date, err => {
            if (err) {
                logger.trace('error occurred saving state to zookeeper', {
                    method: `${this.constructor.name}._scheduleResume`,
                    service: this._getServiceName(),
                    site,
                });
            } else {
                this.scheduledResume = schedule.scheduleJob(date,
                    triggerResume.bind(this));
                logger.info('scheduled resume for service', {
                    scheduledTime: date.toString(),
                    service: this._getServiceName(),
                    site,
                });
            }
        });
    },

    /* Main Methods

    /**
     * Cleanup zookeeper node if the site has been removed as a location
     * @param {function} cb - callback(error)
     * @return {undefined}
     */
    removeZkState(cb) {
        const { logger, site, zkClient } = this.getPauseResumeVars();
        const path = this._getZkSiteNode();
        zkClient.remove(path, err => {
            if (err && err.name !== 'NO_NODE') {
                logger.error('failed to remove zookeeper state node', {
                    method: `${this.constructor.name}.removeZkState`,
                    zookeeperPath: path,
                    site,
                    error: err,
                });
                return cb(err);
            }
            return cb();
        });
    },

    /**
     * Pause consumer(s)
     * @return {undefined}
     */
    _pauseService() {
        const { consumer, logger, site } = this.getPauseResumeVars();
        const enabled = consumer.getServiceStatus();
        if (enabled) {
            // if currently resumed/active, attempt to pause
            this._updateZkStateNode('paused', true, err => {
                if (err) {
                    logger.trace('error occurred saving state to zookeeper', {
                        method: `${this.constructor.name}._pauseService`,
                        service: this._getServiceName(),
                        site,
                    });
                } else {
                    consumer.pause(site);
                    logger.info('paused service processor', {
                        service: this._getServiceName(),
                        site,
                    });
                    this._deleteScheduledResumeService();
                }
            });
        }
    },

    /**
     * Resume consumer(s)
     * @param {Date} [date] - optional date object for scheduling resume
     * @return {undefined}
     */
    _resumeService(date) {
        const { consumer, logger, site } = this.getPauseResumeVars();
        const enabled = consumer.getServiceStatus();
        const now = new Date();

        if (enabled) {
            logger.info('cannot resume, service site is not paused', {
                service: this._getServiceName(),
                site,
            });
            return;
        }

        if (date && now < new Date(date)) {
            // if date is in the future, attempt to schedule job
            this._scheduleResume(date);
        } else {
            this._updateZkStateNode('paused', false, err => {
                if (err) {
                    logger.trace('error occurred saving state to zookeeper', {
                        method: `${this.constructor.name}._resumeService`,
                        service: this._getServiceName(),
                        site,
                    });
                } else {
                    consumer.resume(site);
                    logger.info('resumed service processor', {
                        service: this._getServiceName(),
                        site,
                    });
                    this._deleteScheduledResumeService();
                }
            });
        }
    },

    /**
     * Delete scheduled resume (if any)
     * @return {undefined}
     */
    _deleteScheduledResumeService() {
        const { logger, site } = this.getPauseResumeVars();
        this._updateZkStateNode('scheduledResume', null, err => {
            if (err) {
                logger.trace('error occurred saving state to zookeeper', {
                    method:
                    `${this.constructor.name}._deleteScheduledResumeService`,
                    service: this._getServiceName(),
                    site,
                });
            }
            if (this.scheduledResume) {
                this.scheduledResume.cancel();
                logger.info('deleted scheduled resume', {
                    service: this._getServiceName(),
                    site,
                });
            }
            this.scheduledResume = null;
        });
    },
};

module.exports = PauseResumeProcessorMixin;
