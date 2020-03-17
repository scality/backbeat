const async = require('async');
const forge = require('node-forge');
const arsenal = require('arsenal');
const config = require('../Config');
const werelogs = require('werelogs');
const bucketclient = require('bucketclient');

const convertOverlayFormat = require('./convertOverlayFormat');
const convertServiceStateFormat = require('./convertServiceStateFormat');
const getWorkflowUpdates = require('./getWorkflowUpdates');
const applyWorkflowUpdates = require('./applyWorkflowUpdates');
const patchConfiguration = require('./patchConfiguration');
const { reshapeExceptionError } = arsenal.errorUtils;

werelogs.configure({
    level: config.log.logLevel,
    dump: config.log.dumpLevel,
});
const logger = new werelogs.Logger('mdManagement');

const serviceBucket = 'PENSIEVE';
const refreshInterval = 5000;

const Metadata = arsenal.storage.metadata.MetadataWrapper;
let metadata;

let serviceName;
let overlayVersion;

// let initialized = false;
let serviceCredentials = {
    accounts: [],
};

const tokenKey = 'auth/zenko/remote-management-token';

// XXX copy-pasted from S3
function decryptSecret(instanceCredentials, secret) {
    // XXX don't forget to use u.encryptionKeyVersion if present
    const privateKey = forge.pki.privateKeyFromPem(
        instanceCredentials.privateKey);
    const encryptedSecretKey = forge.util.decode64(secret);
    return privateKey.decrypt(encryptedSecretKey, 'RSA-OAEP', {
        md: forge.md.sha256.create(),
    });
}

/**
 * Decrypt encrypted secret key. Needed for ingestion source credentials
 * @param {string} encryptedSecret - encrypted secret key
 * @param {werelogs.Logger} log - Logger object
 * @param {function} cb - callback(error, decryptedKey)
 * @return {undefined}
 */
function decryptLocationSecret(encryptedSecret, log, cb) {
    return metadata.getObjectMD(serviceBucket, tokenKey, {}, log,
    (err, instanceAuth) => {
        if (err) {
            log.error('failed to fetch decryption key from metadata for ' +
            'deciphering authentication secret', {
                tokenKey,
            });
            return cb(err);
        }
        return cb(null, decryptSecret(instanceAuth, encryptedSecret));
    });
}

function saveServiceCredentials(conf, params, instanceAuth) {
    // TODO use a proper account id in arn
    serviceCredentials = {
        accounts: (conf.users || [])
            .filter(u => u.accountType === params.serviceAccount)
            .map(u => ({
                name: u.accountType,
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

function _getServiceStateKey() {
    return `configuration/state/${serviceName}`;
}

function _loadServiceState(cb) {
    logger.debug(`loading ${serviceName} state`);
    metadata.getObjectMD(
        serviceBucket, _getServiceStateKey(),
        {}, logger, (err, currentStateSerialized) => {
            if (err && err.NoSuchKey) {
                return cb(null, {
                    workflows: {},
                    overlayVersion: 0,
                });
            }
            if (err) {
                return cb(err);
            }
            const currentState = JSON.parse(currentStateSerialized);
            const convState = convertServiceStateFormat(currentState);
            logger.debug(`converted ${serviceName} state`,
                         { convState });
            return cb(null, convState);
        });
}

function _saveServiceState(newState, cb) {
    logger.debug(`saving ${serviceName} state`, { newState });
    return metadata.putObjectMD(serviceBucket, _getServiceStateKey(),
                                JSON.stringify(newState), {}, logger, cb);
}

function getServiceWorkflows(serviceName) {
    const workflows = [serviceName];
    // The lifecycle service manager handles both the expiration workflow and
    // the transition workflow.
    if (serviceName === 'lifecycle') {
        workflows.push('transition');
    }
    return workflows;
}

function _applyServiceState(conf, params, currentState, cb) {
    const workflows = getServiceWorkflows(params.serviceName);
    async.each(workflows, (workflow, next) => {
        const configuredWorkflows = conf.workflows && conf.workflows[workflow];
        const currentWorkflows = currentState && currentState.workflows;
        const workflowUpdates =
            getWorkflowUpdates(configuredWorkflows, currentWorkflows);
        logger.debug('applying workflow updates', { workflowUpdates });
        applyWorkflowUpdates(params, conf, currentState, workflowUpdates,
            logger, (err, newState) => {
                if (err) {
                    return next(err);
                }
                if (newState) {
                    return _saveServiceState(newState, next);
                }
                logger.debug('workflows did not change, skip saving ' +
                    `${workflow} state`, { newState });
                return next();
            });
    }, cb);
}

function _refreshServiceState(conf, params, done) {
    if (!params.applyBucketWorkflows) {
        return process.nextTick(done);
    }
    return async.waterfall([
        cb => _loadServiceState(cb),
        (currentState, cb) =>
            _applyServiceState(conf, params, currentState, cb),
    ], done);
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

    const mdParams = {
        bucketdBootstrap: ['localhost'],
        bucketdLog: null,
        https: null,
        metadataClient: {
            host: config.queuePopulator.dmd.host,
            port: config.queuePopulator.dmd.port,
        },
        replicationGroupId:
            config.extensions.replication.replicationStatusProcessor.groupId,
        noDbOpen: null,
        constants: {
            usersBucket: 'users..bucket',
            splitter: '..|..',
        },
        mongodb: {
            replicaSetHosts: config.queuePopulator.mongo.replicaSetHosts,
            writeConcern: config.queuePopulator.mongo.writeConcern,
            replicaSet: config.queuePopulator.mongo.replicaSet,
            readPreference: config.queuePopulator.mongo.readPreference,
            database: config.queuePopulator.mongo.database,
            replicationGroupId:
                config.extensions.replication.replicationStatusProcessor
                    .groupId,
            path: '',
            authCredentials: config.queuePopulator.mongo.authCredentials,
        },
    };
    let setup = false;
    function iterate(done) {
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
            (conf, cb) => metadata.getObjectMD(serviceBucket, tokenKey, {},
            logger, (err, instanceAuth) => {
                if (err) {
                    return cb(err);
                }
                saveServiceCredentials(conf, params, instanceAuth);
                return cb(null, conf, instanceAuth);
            }),
            (conf, instanceAuth, cb) => patchConfiguration(
                overlayVersion, conf, instanceAuth, metadata, ingestionEnabled,
                logger, (err, updatedOverlayVersion) => {
                    if (err) {
                        return cb(err);
                    }
                    if (updatedOverlayVersion) {
                        overlayVersion = updatedOverlayVersion;
                    }
                    return cb(null, conf);
                }),
            (conf, cb) => _refreshServiceState(conf, params, cb),
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

function getLatestServiceAccountCredentials() {
    return serviceCredentials;
}

module.exports = {
    initManagement,
    getLatestServiceAccountCredentials,
    decryptLocationSecret,
};
