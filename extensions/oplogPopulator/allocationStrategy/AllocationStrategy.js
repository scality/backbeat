const { errors } = require('arsenal');

class AllocationStrategy {

    /**
     * @constructor
     * @param {Object} params params
     * @param {Number} params.maximumBucketsPerConnector maximum number of buckets per connector
     * @param {Function} params.addConnector function to add a connector
     * @param {Logger} params.logger logger object
     */
    constructor(params) {
        this._logger = params.logger;
        this._maximumBucketsPerConnector = params.maximumBucketsPerConnector;
        this._addConnector = params.addConnector.bind(this);
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
