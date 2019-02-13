const Redis = require('ioredis');

/**
 * @class MockAPI
 *
 * @classdesc mimics an api request sent to service process with pause/resume
 */
class MockAPI {
    /**
     * @param {Object} config - config object
     * @param {String} config.topic - service topic
     */
    constructor(config) {
        this._topic = config.topic;
        this.publisher = new Redis();
    }

    _sendRequest(site, msg) {
        const channel = `${this._topic}-${site}`;
        this.publisher.publish(channel, msg);
    }

    /**
     * mock a delete schedule resume call
     * @param {string} site - site name
     * @return {undefined}
     */
    deleteScheduledResumeService(site) {
        const message = JSON.stringify({
            action: 'deleteScheduledResumeService',
        });
        this._sendRequest(site, message);
    }

    /**
     * mock a resume api call
     * @param {string} site - site name
     * @param {Date} [date] - optional date object
     * @return {undefined}
     */
    resumeService(site, date) {
        const message = {
            action: 'resumeService',
        };
        if (date) {
            message.date = date;
        }
        this._sendRequest(site, JSON.stringify(message));
    }

    /**
     * mock a pause api call
     * @param {string} site - site name
     * @return {undefined}
     */
    pauseService(site) {
        const message = JSON.stringify({
            action: 'pauseService',
        });
        this._sendRequest(site, message);
    }
}

module.exports = MockAPI;
