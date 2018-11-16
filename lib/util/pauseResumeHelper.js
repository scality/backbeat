'use strict'; // eslint-disable-line

const async = require('async');
const schedule = require('node-schedule');

const { errors } = require('arsenal');

const validStateProperties = ['paused', 'scheduledResume'];
const serviceMap = {
    QueueProcessor: 'replication',
    MongoQueueProcessor: 'ingestion',
};

// Mixin for QueueProcessor pause/resume utilities
const processorMixin = {

    /* Helper Methods */

    _getServiceName() {
        const name = serviceMap[this.constructor.name];
        if (!name) {
            this.logger.fatal('pause/resume unavailable for this service', {
                method: `${this.constructor.name}._getServiceName`,
            });
            throw errors.InternalError.customizeDescription(
                'pause/resume unavailable for this service');
        }
        return name;
    },

    _handlePauseResumeRequest(redisClient, message) {
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
            this.logger.error('error parsing redis subscribe message', {
                method: `${this.constructor.name}._setupRedis`,
                error: e,
            });
        }
    },

    /**
     * Update zookeeper state node for this site-defined QueueProcessor
     * @param {String} key - key name to store in zk state node
     * @param {String|Boolean} value - value
     * @param {Function} cb - callback(error)
     * @return {undefined}
     */
    _updateZkStateNode(key, value, cb) {
        if (!validStateProperties.includes(key)) {
            this.logger.fatal('incorrect zookeeper state property given', {
                method: `${this.constructor.name}._updateZkStateNode`,
            });
            throw errors.InternalError.customizeDescription(
                'incorrect zookeeper state property given');
        }
        const path = this._getZkSiteNode();
        return async.waterfall([
            next => this.zkClient.getData(path, (err, data) => {
                if (err) {
                    this.logger.error('could not get state from zookeeper', {
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
                    this.logger.error('could not parse state data from ' +
                    'zookeeper', {
                        method: `${this.constructor.name}._updateZkStateNode`,
                        zookeeperPath: path,
                        error: err,
                    });
                    return next(err);
                }
                return next(null, bufferedData);
            }),
            (data, next) => this.zkClient.setData(path, data, err => {
                if (err) {
                    this.logger.error('could not save state data in ' +
                    'zookeeper', {
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
        function triggerResume() {
            this._updateZkStateNode('scheduledResume', null, err => {
                if (err) {
                    this.logger.error('error occurred saving state ' +
                    'to zookeeper for resuming a scheduled resume. Retry ' +
                    'again in 1 minute', {
                        method: `${this.constructor.name}._scheduleResume`,
                        error: err,
                    });
                    // if an error occurs, need to retry
                    // for now, schedule minute from now
                    const date = new Date();
                    date.setMinutes(date.getMinutes() + 1);
                    this.scheduleResume = schedule.scheduleJob(date,
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
                this.logger.trace('error occurred saving state to zookeeper', {
                    method: `${this.constructor.name}._scheduleResume`,
                });
            } else {
                this.scheduledResume = schedule.scheduleJob(date,
                    triggerResume.bind(this));
                this.logger.info('scheduled CRR resume', {
                    scheduleTime: date.toString(),
                });
            }
        });
    },

    /* Main Methods */

    /**
     * Pause consumers
     * @return {undefined}
     */
    _pauseService() {
        const enabled = this._consumer.getServiceStatus();
        if (enabled) {
            // if currently resumed/active, attempt to pause
            this._updateZkStateNode('paused', true, err => {
                if (err) {
                    this.logger.trace('error occurred saving state to ' +
                    'zookeeper', {
                        method: `${this.constructor.name}._pauseService`,
                    });
                } else {
                    this._consumer.pause(this.site);
                    this.logger.info(`paused ${this._getServiceName()} for ` +
                        `location: ${this.site}`);
                    this._deleteScheduledResumeService();
                }
            });
        }
    },

    /**
     * Resume consumers
     * @param {Date} [date] - optional date object for scheduling resume
     * @return {undefined}
     */
    _resumeService(date) {
        const enabled = this._consumer.getServiceStatus();
        const now = new Date();

        if (enabled) {
            this.logger.info(`cannot resume, site ${this.site} is not paused`);
            return;
        }

        if (date && now < new Date(date)) {
            // if date is in the future, attempt to schedule job
            this._scheduleResume(date);
        } else {
            this._updateZkStateNode('paused', false, err => {
                if (err) {
                    this.logger.trace('error occurred saving state to ' +
                    'zookeeper', {
                        method: `${this.constructor.name}._resumeService`,
                    });
                } else {
                    this._consumer.resume(this.site);
                    this.logger.info(`resumed ${this._getServiceName()} for ` +
                        `location: ${this.site}`);
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
        this._updateZkStateNode('scheduledResume', null, err => {
            if (err) {
                this.logger.trace('error occurred saving state to zookeeper', {
                    method:
                    `${this.constructor.name}._deleteScheduledResumeService`,
                });
            } else if (this.scheduledResume) {
                this.scheduledResume.cancel();
                this.scheduledResume = null;
                this.logger.info(`deleted ${this._getServiceName()} scheduled` +
                    ` resume for location: ${this.site}`);
            }
        });
    },

};

module.exports = {
    PauseResumeProcessorMixin: processorMixin,
};
