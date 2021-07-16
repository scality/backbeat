'use strict'; // eslint-disable-line

const async = require('async');
const werelogs = require('werelogs');
const { errors } = require('arsenal');
// const {
//     DEFAULT_LIVE_ROUTE,
//     DEFAULT_READY_ROUTE,
// } = require('arsenal').network.probe.ProbeServer;

const { initManagement } = require('../../../lib/management/index');
const LifecycleObjectProcessor = require('./LifecycleObjectProcessor');
const { sendSuccess, sendError, startProbeServer } = require('../../../lib/util/probe');
const config = require('../../../lib/Config');

const zkConfig = config.zookeeper;
const kafkaConfig = config.kafka;
const lcConfig = config.extensions.lifecycle;
const s3Config = config.s3;
const transport = config.transport;

const logger = new werelogs.Logger('Backbeat:Lifecycle:Consumer');

const objectProcessor = new LifecycleObjectProcessor(
    zkConfig, kafkaConfig, lcConfig, s3Config, transport);

werelogs.configure({ level: config.log.logLevel,
    dump: config.log.dumpLevel });

function livenessCheck(res, log) {
    if (objectProcessor.isReady()) {
        sendSuccess(res, log);
    } else {
        sendError(res, log, errors.ServiceUnavailable, 'unhealthy');
    }
}

function probeServerSetup(config, done) {
    startProbeServer(config, (err, probeServer) => {
        if (err) {
            return done(err);
        }

        if (!probeServer) {
            logger.info('Skipping lifecycle object processor server setup');
            return done();
        }

        // TODO: update liveness and readiness routes to default routes
        probeServer.addHandler('/_/health/liveness', livenessCheck);
        probeServer.addHandler('/_/health/readiness', livenessCheck);
        logger.info('Starting lifecycle object processor server');
        return done();
    });
}

function init(done) {
    initManagement({
        serviceName: 'lifecycle',
        serviceAccount: lcConfig.auth.account,
    }, error => {
        if (error) {
            logger.error('could not load management db', { error });
            setTimeout(init, 5000, done);
            return;
        }

        logger.info('management init done');
        done();
        return;
    });
}

async.waterfall([
    done => init(done),
    done => objectProcessor.start(err => done(err)),
    done => probeServerSetup(lcConfig.objectProcessor.probeServer, done),
], err => {
    if (err) {
        logger.error('error during lifecycle object processor initialization',
            { error: err.message });
        process.exit(1);
    }
    logger.info('lifecycle object processor running!');
});

process.on('SIGTERM', () => {
    logger.info('received SIGTERM, exiting');
    objectProcessor.close(() => {
        process.exit(0);
    });
});
