const async = require('async');
const schedule = require('node-schedule');

const werelogs = require('werelogs');

const config = require('../conf/Config');
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
        log.debug('skipping replication batch: previous one still in progress');
        return undefined;
    }
    const onTimeout = () => {
        // reset the flag to allow a new batch to start in case the
        // previous batch timed out
        taskState.batchInProgress = false;
    };
    log.debug('start queueing replication batch');
    taskState.batchInProgress = true;
    const maxRead = qpConfig.batchMaxRead;
    queuePopulator.processAllLogEntries({ maxRead, onTimeout }, err => {
        taskState.batchInProgress = false;
        if (err) {
            log.error('an error occurred during replication', {
                method: 'QueuePopulator::task.queueBatch',
                error: err,
            });
        }
    });
    return undefined;
}
/* eslint-enable no-param-reassign */

const queuePopulator = new QueuePopulator(
    zkConfig, kafkaConfig, qpConfig, httpsConfig, mConfig, rConfig, extConfigs);

let probeServer;
if (process.env.BACKBEAT_ENABLE_PROBE_SERVER === 'true' &&
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
    queuePopulator.close(() => {
        process.exit(0);
    });
});
