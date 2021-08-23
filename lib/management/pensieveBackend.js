const async = require('async');
const arsenal = require('arsenal');
const { decryptSecret } = arsenal.pensieve.credentialUtils;
const config = require('../Config');
const werelogs = require('werelogs');
const bucketclient = require('bucketclient');

const { BaseServiceState } = require('./serviceState');
const convertOverlayFormat = require('./convertOverlayFormat');
const convertServiceStateFormat = require('./convertServiceStateFormat');
const { patchConfiguration, buildMetadataParams, refreshInterval } = require('./patchConfiguration');
const { reshapeExceptionError } = arsenal.errorUtils;

werelogs.configure({
    level: config.log.logLevel,
    dump: config.log.dumpLevel,
});
const logger = new werelogs.Logger('management:pensieveBackend');

const serviceBucket = 'PENSIEVE';

const Metadata = arsenal.storage.metadata.MetadataWrapper;
let metadata;

let serviceName;
let overlayVersion;

// let initialized = false;
let serviceCredentials = {
    accounts: [],
};

const tokenKey = 'auth/zenko/remote-management-token';

function saveServiceCredentials(conf, params, instanceAuth) {
    // TODO use a proper account id in arn
    serviceCredentials = {
        accounts: (conf.users || [])
            .filter(u => u.accountType === params.serviceAccount)
            .map(u => ({
                name: u.accountType,
                accountType: u.accountType,
                arn: 'aws::iam:234456789012:root',
                canonicalID: u.canonicalId,
                displayName: u.userName,
                keys: {
                    access: u.accessKey,
                    secret: decryptSecret(instanceAuth, u.secretKey),
                },
            })),
    };
    // initialized = true;
}

function loadOverlayVersion(metadata, version, cb) {
    metadata.getObjectMD(serviceBucket, `configuration/overlay/${version}`, {},
    logger, (err, val) => {
        if (err) {
            return cb(err);
        }
        const convConf = convertOverlayFormat(val);
        logger.debug('converted overlay config to newest format');
        return cb(null, convConf);
    });
}

class PensieveServiceState extends BaseServiceState {
    constructor(serviceName) {
        super(serviceName);
        this.key = `configuration/state/${this.serviceName}`;
    }

    load(cb) {
        logger.debug(`loading ${this.serviceName} state`);
        metadata.getObjectMD(
            serviceBucket, this.key,
            {}, logger, (err, currentStateSerialized) => {
                if (err && err.NoSuchKey) {
                    return cb(null, this.getInitialState());
                }
                if (err) {
                    return cb(err);
                }
                const currentState = JSON.parse(currentStateSerialized);
                const convState = convertServiceStateFormat(currentState);
                logger.debug(`converted ${this.serviceName} state`,
                            { convState });
                return cb(null, convState);
            });
    }

    save(newState, cb) {
        logger.debug(`saving ${this.serviceName} state`, { newState });
        return metadata.putObjectMD(serviceBucket, this.key,
                                    JSON.stringify(newState), {}, logger, cb);
    }
}

/**
 * Initialize Orbit management layer
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
    if (process.env.REMOTE_MANAGEMENT_DISABLE &&
        process.env.REMOTE_MANAGEMENT_DISABLE !== '0') {
        logger.info('remote management disabled');
        return process.nextTick(done);
    }
    const ingestionEnabled = params.enableIngestionUpdates;
    serviceName = params.serviceName;
    const mdParams = buildMetadataParams(config);
    let setup = false;
    function iterate(done) {
        const serviceState = new PensieveServiceState(serviceName);
        return async.waterfall([
            cb => {
                if (!setup) {
                    setup = true;
                    metadata = new Metadata('mongodb', mdParams, bucketclient,
                        logger);
                    return metadata.setup(() => cb());
                }
                return process.nextTick(cb);
            },
            cb => {
                metadata.getObjectMD(serviceBucket,
                'configuration/overlay-version', {}, logger, (err, res) =>
                    cb(err, res));
            },
            (version, cb) => loadOverlayVersion(metadata, version, cb),
            (conf, cb) => patchConfiguration(overlayVersion, conf, metadata,
            ingestionEnabled, logger, err => {
                if (err) {
                    return cb(err);
                }
                if (conf.version) {
                    overlayVersion = conf.version;
                }
                return cb(null, conf);
            }),
            (conf, cb) => metadata.getObjectMD(serviceBucket, tokenKey, {},
            logger, (err, instanceAuth) => {
                if (err) {
                    return cb(err);
                }
                saveServiceCredentials(conf, params, instanceAuth);
                return cb(null, conf);
            }),
            (conf, cb) => serviceState.apply(conf, params, cb),
        ], done);
    }

    iterate(err => {
        if (err) {
            return done(reshapeExceptionError(err));
        }
        setInterval(iterate, refreshInterval, err => {
            if (err) {
                logger.error('error refreshing mgdb', { error:
                  reshapeExceptionError(err) });
            }
        });
        return done();
    });
    return undefined;
}

/**
 *
 * @param {String} serviceName - Name of the service to persist state for
 * @param {Object} zkClient - Zookeeper client
 * @returns {BaseServiceState} - the service state applier
 */
function createServiceState(serviceName, zkClient) { // eslint-disable-line no-unused-vars
    return new PensieveServiceState(serviceName);
}

function getLatestServiceAccountCredentials() {
    return serviceCredentials;
}

module.exports = {
    initManagement,
    getLatestServiceAccountCredentials,
    createServiceState,
};
