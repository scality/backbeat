'use strict'; // eslint-disable-line

const async = require('async');
const werelogs = require('werelogs');
const { errors } = require('arsenal');
const {
    DEFAULT_LIVE_ROUTE,
    DEFAULT_READY_ROUTE,
} = require('arsenal').network.probe.ProbeServer;

const LifecycleBucketProcessor = require('./LifecycleBucketProcessor');
const { sendSuccess, sendError, startProbeServer } = require('../../../lib/util/probe');
const config = require('../../../conf/Config');

const { zookeeper, kafka, extensions, s3, log } = config;
const lcConfig = extensions.lifecycle;
const repConfig = extensions.replication;

werelogs.configure({
    level: log.logLevel,
    dump: log.dumpLevel,
});

const logger = new werelogs.Logger('Backbeat:Lifecycle:Producer');

const bucketProcessor = new LifecycleBucketProcessor(
    zookeeper, kafka, lcConfig, repConfig, s3, lcConfig.transport
);

function livenessCheck(res, log) {
    if (bucketProcessor.isReady()) {
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
            logger.info('Skipping lifecycle bucket processor server setup');
            return done();
        }

        probeServer.addHandler(DEFAULT_LIVE_ROUTE, livenessCheck);
        probeServer.addHandler(DEFAULT_READY_ROUTE, livenessCheck);
        logger.info('Starting lifecycle bucket processor server');
        return done();
    });
}

async.waterfall([
    done => bucketProcessor.start(err => done(err)),
    done => probeServerSetup(lcConfig.bucketProcessor.probeServer, done),
], err => {
    if (err) {
        logger.error('error during lifecycle bucket processor initialization',
            { error: err.message });
        process.exit(1);
    }
    logger.info('lifecycle bucket processor running!');
});

process.on('SIGTERM', () => {
    logger.info('received SIGTERM, exiting');
    bucketProcessor.close(() => {
        process.exit(0);
    });
});
