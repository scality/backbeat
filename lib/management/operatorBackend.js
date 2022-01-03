const arsenal = require('arsenal');
const config = require('../Config');
const werelogs = require('werelogs');

const { reshapeExceptionError } = arsenal.errorUtils;
const { BaseServiceState } = require('./serviceState');
const {
    periodicallyUpdateIngestionBuckets,
} = require('./patchConfiguration');

werelogs.configure({
    level: config.log.logLevel,
    dump: config.log.dumpLevel,
});

const serviceCredentials = {};

const logger = new werelogs.Logger('management:operatorBackend');

class ZookeeperServiceState extends BaseServiceState {
    constructor(serviceName, zkClient) {
        super(serviceName);
        this.zkPath = `/${this.serviceName}/management/serviceState`;
        this.zkClient = zkClient;
    }

    load(cb) {
        logger.debug(`loading ${this.serviceName} state`);
        this.zkClient.getData(this.zkPath, (err, data) => {
            if (err && err.name !== 'NO_NODE') {
                return cb(err);
            }

            try {
                const state = data ?
                    JSON.parse(data.toString()) :
                    this.getInitialState();
                return cb(null, state);
            } catch (err) {
                return cb(reshapeExceptionError(err));
            }
        });
    }

    save(newState, cb) {
        logger.debug(`saving ${this.serviceName} state`, { newState });
        try {
            const data = Buffer.from(JSON.stringify(newState));
            return this.zkClient.setOrCreate(this.zkPath, data, cb);
        } catch (err) {
            return cb(reshapeExceptionError(err));
        }
    }
}

/**
 * Initialize management layer
 *
 * @param {object} params - params object
 * @param {string} params.serviceName - name of service to manage
 * @param {string} [params.serviceAccount] - name of managed service
 *   account, if any
 * @param {function} [params.applyBucketWorkflows] - called when a
 *   bucket has a changed set of workflows that needs to be applied by
 *   the service: applyBucketWorkflows(bucketName, bucketWorkflows,
 *   workflowUpdates, cb)
 * @param {function} done - callback function when init is complete
 * @return {undefined}
 */
function initManagement(params, done) {
    const authData = require('../../conf/authdata.json') || {};
    const confAccounts = authData.accounts || [];
    serviceCredentials.accounts = confAccounts
        .filter(a => a.name === params.serviceAccount)
        .map(a => ({
            ...a,
            keys: a.keys[0],
        }));
    const locations = require('../../conf/locationConfig.json') || {};

    const ingestionEnabled = params.enableIngestionUpdates;
    if (ingestionEnabled) {
        return periodicallyUpdateIngestionBuckets(locations, logger, done);
    }
    return process.nextTick(done);
}

function getLatestServiceAccountCredentials() {
    return serviceCredentials;
}

function createServiceState(serviceName, zkClient) {
    return new ZookeeperServiceState(serviceName, zkClient);
}

module.exports = {
    createServiceState,
    initManagement,
    getLatestServiceAccountCredentials,
};
