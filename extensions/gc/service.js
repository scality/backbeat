'use strict'; // eslint-disable-line

const { errors } = require('arsenal');
const werelogs = require('werelogs');
const {
    DEFAULT_LIVE_ROUTE,
    DEFAULT_READY_ROUTE,
    DEFAULT_METRICS_ROUTE,
} = require('arsenal').network.probe.ProbeServer;
const { sendSuccess, sendError } = require('arsenal').network.probe.Utils;
const { ZenkoMetrics } = require('arsenal').metrics;

const GarbageCollector = require('./GarbageCollector');
const { startProbeServer } = require('../../lib/util/probe');
const { initManagement } = require('../../lib/management');
const config = require('../../lib/Config');

const kafkaConfig = config.kafka;
const s3Config = config.s3;
const gcConfig = config.extensions.gc;
const transport = config.transport;

// TO BE MOVED
const Kafka = require('node-rdkafka');

const client = Kafka.AdminClient.create({
  'client.id': 'kafka-admin',
  'metadata.broker.list': config.kafka.hosts,
});
const RD_KAFKA_RESP_ERR_TOPIC_ALREADY_EXISTS = 36

const garbageCollector = new GarbageCollector({
    kafkaConfig,
    s3Config,
    gcConfig,
    transport,
});

werelogs.configure({ level: config.log.logLevel,
                     dump: config.log.dumpLevel });
const logger = new werelogs.Logger('Backbeat:GC:service');

/**
 * Handle ProbeServer liveness check
 *
 * @param {http.HTTPServerResponse} res - HTTP Response to respond with
 * @param {Logger} log - Logger
 * @returns {undefined}
 */
 function handleLiveness(res, log) {
    if (garbageCollector.isReady()) {
        sendSuccess(res, log);
    } else {
        sendError(res, log, errors.ServiceUnavailable, 'unhealthy');
    }
}

/**
 * Handle ProbeServer metrics
 *
 * @param {http.HTTPServerResponse} res - HTTP Response to respond with
 * @param {Logger} log - Logger
 * @returns {undefined}
 */
 function handleMetrics(res, log) {
    log.debug('metrics requested');
    res.writeHead(200, {
        'Content-Type': ZenkoMetrics.asPrometheusContentType(),
    });
    res.end(ZenkoMetrics.asPrometheus());
}

function initAndStart() {
    initManagement({
        serviceName: 'gc',
        serviceAccount: gcConfig.auth.account,
    }, error => {
        if (error) {
            logger.error('could not load management db', { error });
            setTimeout(initAndStart, 5000);
            return;
        }
        logger.info('management init done');
        client.createTopic({
            topic: gcConfig.topic,
            num_partitions: 5,
            replication_factor: 1
        }, err => {
            if (err && err.code !== RD_KAFKA_RESP_ERR_TOPIC_ALREADY_EXISTS) {
                logger.error('error creating topic', {
                    error: err,
                });
                process.exit(1);
                return;
            }
            logger.info('kafka topic created', {
                topic: gcConfig.topic,
            });
            startProbeServer(gcConfig.probeServer, (err, probeServer) => {
                if (err) {
                    logger.error('error starting probe server', { error: err });
                }
                if (probeServer !== undefined) {
                    // following the same pattern as other extensions, where liveness
                    // and readiness are handled by the same handler
                    probeServer.addHandler([DEFAULT_LIVE_ROUTE, DEFAULT_READY_ROUTE], handleLiveness);
                    // retaining the old route and adding support to new route, until
                    // metrics handling is consolidated
                    probeServer.addHandler(['/_/monitoring/metrics', DEFAULT_METRICS_ROUTE], handleMetrics);
                }
                garbageCollector.start(err => {
                    if (err) {
                        logger.error('error during garbage collector initialization', { error: err.message });
                    } else {
                        logger.info('garbage collector is running');
                    }
                });
            });
        });
    });
}

initAndStart();

process.on('SIGTERM', () => {
    logger.info('received SIGTERM, exiting');
    garbageCollector.stop(() => {
        process.exit(0);
    });
});
