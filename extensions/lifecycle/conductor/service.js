'use strict'; // eslint-disable-line

const async = require('async');
const werelogs = require('werelogs');
const { errors } = require('arsenal');
const {
    DEFAULT_LIVE_ROUTE,
    DEFAULT_READY_ROUTE,
} = require('arsenal').network.probe.ProbeServer;

const LifecycleConductor = require('./LifecycleConductor');
const { sendSuccess, sendError, startProbeServer } = require('../../../lib/util/probe');
const config = require('../../../conf/Config');

const zkConfig = config.zookeeper;
const kafkaConfig = config.kafka;
const lcConfig = config.extensions.lifecycle;
const repConfig = config.extensions.replication;

const lcConductor = new LifecycleConductor(
    zkConfig, kafkaConfig, lcConfig, repConfig
);

werelogs.configure({
    level: config.log.logLevel,
    dump: config.log.dumpLevel,
});
const logger = new werelogs.Logger('Backbeat:Lifecycle:Conductor:service');

function livenessCheck(res, log) {
    if (lcConductor.isReady()) {
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
            logger.info('Skipping lifecycle conductor server setup');
            return done();
        }

        probeServer.addHandler(DEFAULT_LIVE_ROUTE, livenessCheck);
        probeServer.addHandler(DEFAULT_READY_ROUTE, livenessCheck);
        logger.info('Starting lifecycle conductor server');
        return done();
    });
}
async.waterfall([
    done => lcConductor.start(err => done(err)),
    done => probeServerSetup(lcConfig.conductor.probeServer, done),
], err => {
    if (err) {
        logger.error('error during lifecycle conductor initialization',
            { error: err.message });
        process.exit(1);
    }
    logger.info('lifecycle conductor running!');
});

process.on('SIGTERM', () => {
    logger.info('received SIGTERM, exiting');
    lcConductor.stop(() => {
        process.exit(0);
    });
});
