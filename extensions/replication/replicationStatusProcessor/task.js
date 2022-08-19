'use strict'; // eslint-disable-line

const werelogs = require('werelogs');

const ReplicationStatusProcessor = require('./ReplicationStatusProcessor');
const { startProbeServer } = require('../../../lib/util/probe');
const { DEFAULT_LIVE_ROUTE, DEFAULT_METRICS_ROUTE, DEFAULT_READY_ROUTE } =
    require('arsenal').network.probe.ProbeServer;

const config = require('../../../lib/Config');
const kafkaConfig = config.kafka;
const repConfig = config.extensions.replication;
const notificationConfig = config.extensions.notification;
const sourceConfig = repConfig.source;
const internalHttpsConfig = config.internalHttps;
const mConfig = config.metrics;
const mongoConfig = config.queuePopulator.mongo;

const { initManagement } = require('../../../lib/management/index');

const replicationStatusProcessor = new ReplicationStatusProcessor(
    kafkaConfig, sourceConfig, repConfig, internalHttpsConfig, mConfig,
    notificationConfig, mongoConfig);

werelogs.configure({ level: config.log.logLevel,
     dump: config.log.dumpLevel });

const logger = new werelogs.Logger('backbeat:ReplicationStatusProcessor:Init');

function initAndStart() {
    initManagement({
        serviceName: 'replication',
        serviceAccount: sourceConfig.auth.account,
    }, error => {
        if (error) {
            logger.error('could not load management db', { error });
            setTimeout(initAndStart, 5000);
            return;
        }
        replicationStatusProcessor.start(null, startProbeServer(
            repConfig.replicationStatusProcessor.probeServer,
            (err, probeServer) => {
                if (err) {
                    logger.error('error starting probe server', {
                        error: err,
                        method: 'ReplicationStatusProcessor::startProbeServer',
                    });
                    return;
                }
                if (probeServer !== undefined) {
                    probeServer.addHandler([DEFAULT_LIVE_ROUTE, DEFAULT_READY_ROUTE],
                        (res, log) => replicationStatusProcessor.handleLiveness(res, log)
                    );
                    // TODO: set this variable during deployment
                    // enable metrics route only when it is enabled
                    if (process.env.ENABLE_METRICS_PROBE === 'true') {
                        probeServer.addHandler(
                            DEFAULT_METRICS_ROUTE,
                            (res, log) => replicationStatusProcessor.handleMetrics(res, log)
                        );
                    }
                }
                logger.info('management init done');
            }
        ));
    });
}

initAndStart();

process.on('SIGTERM', () => {
    logger.info('received SIGTERM, exiting');
    replicationStatusProcessor.stop(error => {
        if (error) {
            logger.error('failed to exit properly', {
                error,
            });
            process.exit(1);
        }
    });
});
