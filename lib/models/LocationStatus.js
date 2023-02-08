/**
 * Class to manage a location's pause/resume state on its
 * different services.
 * Current supported services are : crr, ingestion, lifecycle
 */
class LocationStatus {

    /**
     * @constructor
     * @param {string[]} services services to init
     * @param {Object} locationStatus initial location status values
     */
    constructor(services, locationStatus) {
        this._data = this._initStatus(services);
        if (locationStatus) {
            const data = locationStatus instanceof LocationStatus ?
                locationStatus._data : locationStatus;
            Object.keys(this._data).forEach(svc => {
                this._data[svc] = {
                    paused: (data[svc] && data[svc].paused) || false,
                    scheduledResume: (data[svc] && data[svc].scheduledResume) || null,
                };
            });
        }
    }

    /**
     * Initializes the status of all services
     * The default status of a service is unpaused
     * @param {string[]} servicesToInit services to be initialized
     * @return {Object} initial services status
     */
    _initStatus(servicesToInit) {
        const initStatus = {
            paused: false,
            scheduledResume: null,
        };
        return {
            crr: servicesToInit.includes('crr') ? initStatus : null,
            ingestion: servicesToInit.includes('ingestion') ? initStatus : null,
            lifecycle: servicesToInit.includes('lifecycle') ? initStatus : null,
        };
    }

    /**
     * initializes a service status
     * @param {string} service service name
     * @return {undefined}
     */
    _initService(service) {
        this._data[service] = {
            paused: false,
            scheduledResume: null,
        };
    }

    /**
     * @param {string} service service name
     * @param {boolean} paused true if paused
     * @return {undefined}
     */
    setServicePauseStatus(service, paused) {
        if (Object.keys(this._data).includes(service)) {
            if (!this._data[service]) {
                this._initService(service);
            }
            this._data[service].paused = paused;
        }
    }

    /**
     * @param {string} service service name
     * @return { boolean | null} true if paused
     */
    getServicePauseStatus(service) {
        if (!this._data[service]) {
            return null;
        }
        return this._data[service].paused;
    }

    /**
     * @param {string} service service name
     * @param {Date | null} [date] scheduled resume date
     * @return {undefined}
     */
    setServiceResumeSchedule(service, date) {
        if (this._data[service]) {
            if (date) {
                this._data[service].scheduledResume = date.toString();
            } else {
                this._data[service].scheduledResume = null;
            }
        }
    }

    /**
     * @param {string} service service name
     * @return { Date | null} scheduled resume date
     */
    getServiceResumeSchedule(service) {
        const schedule = this._data[service] && this._data[service].scheduledResume;
        if (!schedule) {
            return null;
        }
        return new Date(schedule);
    }

    /**
     * @param {string | string[]} service service(s) name
     * @return {undefined}
     */
    pauseLocation(service) {
        const servicesList = Array.isArray(service) ?
            service : [service];
        servicesList.forEach(svc => {
            if (!this.getServicePauseStatus(svc)) {
                this.setServicePauseStatus(svc, true);
            }
        });
    }

    /**
     * @param {string | string[]} service service(s) name
     * @param {Date} [schedule] date to resume service(s)
     * @return {undefined}
     */
    resumeLocation(service, schedule) {
        const servicesList = Array.isArray(service) ?
            service : [service];
        servicesList.forEach(svc => {
            if (!this.getServicePauseStatus(svc)) {
                return;
            }
            let shouldPause = false;
            if (schedule) {
                shouldPause = true;
            }
            this.setServicePauseStatus(svc, shouldPause);
            this.setServiceResumeSchedule(svc, schedule || null);
        });
    }

    /**
     * @param {string} service service name
     * @return {Object|null} service status object
     */
    getService(service) {
        return this._data[service] || null;
    }

    /**
     * @return {Object} location status object
     */
    getValue() {
        return this._data;
    }

    /**
     * @return {string} serialized location status data
     */
    getSerialized() {
        return JSON.stringify(this.getValue());
    }
}

module.exports = LocationStatus;
