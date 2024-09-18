const { errors } = require('arsenal');

class AllocationStrategy {

    /**
     * @constructor
     * @param {Object} params params
     * @param {Logger} params.logger logger object
     */
    constructor(params) {
        this._logger = params.logger;
    }

    /**
     * Get best connector to assign a bucket to.
     * If no connector is available, null is returned.
     * @param {Array<Connector>} connectors connectors
     * @param {String} bucket bucket name
     * @returns {Connector | null} connector
     */
    getConnector(connectors, bucket) { // eslint-disable-line no-unused-vars
        throw errors.NotImplemented;
    }

    /**
     * Assess if a pipeline can be updated
     * @param {Connector} connector connector
     * @returns {Boolean} true if the connector can be updated
     */
    canUpdate(connector) { // eslint-disable-line no-unused-vars
        throw errors.NotImplemented;
    }
}

module.exports = AllocationStrategy;
