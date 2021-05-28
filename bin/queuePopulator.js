const async = require('async');
const schedule = require('node-schedule');

const werelogs = require('werelogs');

const config = require('../lib/Config');
const zkConfig = config.zookeeper;
const kafkaConfig = config.kafka;
const extConfigs = config.extensions;
const qpConfig = config.queuePopulator;
const httpsConfig = config.internalHttps;
const mConfig = config.metrics;
const rConfig = config.redis;
const QueuePopulator = require('../lib/queuePopulator/QueuePopulator');
const { ProbeServer, DEFAULT_LIVE_ROUTE, DEFAULT_METRICS_ROUTE } =
    require('arsenal').network.probe.ProbeServer;
const log = new werelogs.Logger('Backbeat:QueuePopulator');

werelogs.configure({ level: config.log.logLevel,
    dump: config.log.dumpLevel });

/* eslint-disable no-param-reassign */
function queueBatch(queuePopulator, taskState) {
    if (taskState.batchInProgress) {
        log.debug('skipping batch: previous one still in progress');
        return undefined;
    }
    log.debug('start queueing batch');
    taskState.batchInProgress = true;
    const maxRead = qpConfig.batchMaxRead;
    const timeoutMs = qpConfig.batchTimeoutMs;
    queuePopulator.processLogEntries({ maxRead, timeoutMs }, err => {
        taskState.batchInProgress = false;
        if (err) {
            log.error('an error occurred during batch processing', {
                method: 'QueuePopulator::task.queueBatch',
                error: err,
            });
            // exit process and let Kubernetes respawn the pod
            process.exit(1);
        }
    });
    return undefined;
}
/* eslint-enable no-param-reassign */

const queuePopulator = new QueuePopulator(zkConfig, kafkaConfig,
    qpConfig, httpsConfig, mConfig, rConfig, extConfigs);

let probeServer;
if (process.env.CRR_METRICS_PROBE === 'true' &&
    qpConfig.probeServer !== undefined) {
    probeServer = new ProbeServer(qpConfig.probeServer);
}

async.waterfall([
    done => {
        if (probeServer === undefined) {
            return done();
        }
        probeServer.addHandler(
            DEFAULT_LIVE_ROUTE,
            (res, log) => queuePopulator.handleLiveness(res, log)
        );
        probeServer.addHandler(
            DEFAULT_METRICS_ROUTE,
            (res, log) => queuePopulator.handleMetrics(res, log)
        );
        probeServer._cbOnListening = done;
        probeServer.start();
        return undefined;
    },
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
            method: 'QueuePopulator::task',
            error: err,
        });
        process.exit(1);
    }
});

process.on('SIGTERM', () => {
    log.info('received SIGTERM, exiting');
    queuePopulator.close(error => {
        if (error) {
            log.error('failed to exit properly', {
                error,
            });
            process.exit(1);
        }
        process.exit(0);
    });
});
