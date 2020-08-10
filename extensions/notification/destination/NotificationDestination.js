const assert = require('assert');

class NotificationDestination {
    /**
     * @constructor
     * @param {Object} params - constructor params
     * @param {Object} params.destConfig - destination-specific
     *   configuration object
     * @param {Logger} params.logger - logger object
     */
    constructor(params) {
        this._destinationConfig = params.destConfig;
        this._log = params.logger;
    }

    /**
     * Init the notification client.
     *
     * This method must be implemented by subclasses of NotificationDestination
     * @return {undefined}
     */
    init() {
        assert(false,
            'sub-classes of NotificationDestination must implement ' +
            'the init() method');
    }

    /**
     * Process entry in the sub-class and send it
     *
     * This method must be implemented by subclasses of NotificationDestination
     * @param {Object[]} messages - array of messages to be sent
     * @return {undefined}
     */
    send(messages) { // eslint-disable-line no-unused-vars
        assert(false,
            'sub-classes of NotificationDestination must implement ' +
            'the send() method');
    }

    /**
     * Stops the destination client
     *
     * @return {undefined}
     */
    stop() {
        assert(false,
            'sub-classes of NotificationDestination must implement ' +
            'the stop() method');
    }
}

module.exports = NotificationDestination;
