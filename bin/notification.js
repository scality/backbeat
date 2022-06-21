const async = require('async');
const schedule = require('node-schedule');

const werelogs = require('werelogs');

const config = require('../lib/Config');
const zkConfig = config.zookeeper;
const kafkaConfig = config.kafka;
const httpsConfig = config.internalHttps;
const mConfig = config.metrics;
const rConfig = config.redis;
const vConfig = config.vaultAdmin;
const qpConfig = config.queuePopulator;
const extConfigs = config.extensions;
const QueuePopulator = require('../lib/queuePopulator/QueuePopulator');
const { startProbeServer } = require('../lib/util/probe');
const { DEFAULT_LIVE_ROUTE, DEFAULT_METRICS_ROUTE, DEFAULT_READY_ROUTE } =
    require('arsenal').network.probe.ProbeServer;
const log = new werelogs.Logger('Backbeat:NotificationQueuePopulator');

werelogs.configure({
    level: config.log.logLevel,
    dump: config.log.dumpLevel,
});

/* eslint-disable no-param-reassign */
function queueBatch(queuePopulator, taskState) {
    if (taskState.batchInProgress) {
        log.debug('skipping notification batch: previous one still in progress');
        return undefined;
    }
    log.debug('start queueing notification batch');
    taskState.batchInProgress = true;
    const maxRead = qpConfig.batchMaxRead;
    const timeoutMs = 1000;
    queuePopulator.processLogEntries({ maxRead, timeoutMs }, err => {
        taskState.batchInProgress = false;
        if (err) {
            log.error('an error occurred during notification', {
                method: 'QueuePopulator::task.queueBatch',
                error: err,
            });
        }
    });
    return undefined;
}
/* eslint-enable no-param-reassign */

const queuePopulator = new QueuePopulator(
    zkConfig, kafkaConfig, qpConfig, httpsConfig, mConfig, rConfig, vConfig, extConfigs);

async.waterfall([
    done => startProbeServer(qpConfig.probeServer, (err, probeServer) => {
        if (err) {
            log.error('error starting probe server', {
                error: err,
                method: 'NotificationQueuePopulator::startProbeServer',
            });
            done(err);
            return;
        }
        if (probeServer !== undefined) {
            probeServer.addHandler([DEFAULT_LIVE_ROUTE, DEFAULT_READY_ROUTE],
                (res, log) => queuePopulator.handleLiveness(res, log)
            );
            if (process.env.ENABLE_METRICS_PROBE === 'true') {
                probeServer.addHandler(
                    DEFAULT_METRICS_ROUTE,
                    (res, log) => queuePopulator.handleMetrics(res, log)
                );
            }
        }
        done();
    }),
    done => queuePopulator.open(done),
    done => {
        const taskState = {
            batchInProgress: false,
        };
        schedule.scheduleJob(qpConfig.cronRule, () => {
            queueBatch(queuePopulator, taskState);
        });
        done();
    },
], err => {
    if (err) {
        log.error('error during queue populator initialization', {
            method: 'bin.notification',
            error: err,
        });
        process.exit(1);
    }
});

process.on('SIGTERM', () => {
    log.info('received SIGTERM, exiting');
    queuePopulator.close(() => {
        process.exit(0);
    });
});
