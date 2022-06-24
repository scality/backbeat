const { errors } = require('arsenal');

class AllocationStrategy {

    /**
     * @constructor
     * @param {Logger} logger logger object
     */
    constructor(logger) {
        this._logger = logger;
    }

    /**
     * Get best connector for assigning a bucket
     * @param {Connector[]} connectors available connectors
     * @returns {Connector} connector
     */
    getConnector(connectors) { // eslint-disable-line no-unused-vars
        throw errors.NotImplemented;
    }

}

module.exports = AllocationStrategy;
