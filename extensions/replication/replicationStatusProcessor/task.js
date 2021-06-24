'use strict'; // eslint-disable-line

const async = require('async');
const werelogs = require('werelogs');

const ReplicationStatusProcessor = require('./ReplicationStatusProcessor');
const { startProbeServer } = require('../../../lib/util/probe');
const { DEFAULT_LIVE_ROUTE, DEFAULT_METRICS_ROUTE } =
    require('arsenal').network.probe.ProbeServer;

const config = require('../../../conf/Config');
const kafkaConfig = config.kafka;
const repConfig = config.extensions.replication;
const sourceConfig = repConfig.source;
const internalHttpsConfig = config.internalHttps;

const replicationStatusProcessor = new ReplicationStatusProcessor(
    kafkaConfig, sourceConfig, repConfig, internalHttpsConfig
);

const log = new werelogs.Logger('Backbeat:ReplicationStatusProcessor');
werelogs.configure({
    level: config.log.logLevel,
    dump: config.log.dumpLevel,
});

async.waterfall([
    done => startProbeServer(
        repConfig.replicationStatusProcessor.probeServer,
        (err, probeServer) => {
            if (err) {
                log.error('error starting probe server', {
                    error: err,
                    method: 'ReplicationStatusProcessor::startProbeServer',
                });
                done(err);
                return;
            }
            if (probeServer !== undefined) {
                probeServer.addHandler(
                    DEFAULT_LIVE_ROUTE,
                    (res, log) => replicationStatusProcessor.handleLiveness(res, log)
                );
                probeServer.addHandler(
                    DEFAULT_METRICS_ROUTE,
                    (res, log) => replicationStatusProcessor.handleMetrics(res, log)
                );
            }
            done();
        }
    ),
    done => replicationStatusProcessor.start(undefined, done),
], err => {
    if (err) {
        log.error('error during queue processor initialization', {
            method: 'ReplicationStatusProcessor::task',
            error: err,
        });
        process.exit(1);
    }
});

process.on('SIGTERM', () => {
    log.info('received SIGTERM, exiting');
    replicationStatusProcessor.stop(() => {
        process.exit(0);
    });
});
