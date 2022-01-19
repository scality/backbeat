'use strict'; // eslint-disable-line
const async = require('async');
const assert = require('assert');
const werelogs = require('werelogs');

const QueueProcessor = require('./QueueProcessor');
const MetricsProducer = require('../../../lib/MetricsProducer');

const config = require('../../../conf/Config');
const kafkaConfig = config.kafka;
const repConfig = config.extensions.replication;
const sourceConfig = repConfig.source;
const httpsConfig = config.https;
const internalHttpsConfig = config.internalHttps;
const mConfig = config.metrics;
const { startProbeServer } = require('../../../lib/util/probe');
const { DEFAULT_LIVE_ROUTE, DEFAULT_METRICS_ROUTE } =
    require('arsenal').network.probe.ProbeServer;

const site = process.argv[2];
assert(site, 'QueueProcessor task must be started with a site as argument');

console.log('site !!!', site);

const bootstrapList = repConfig.destination.bootstrapList
    .filter(item => item.site === site);
assert(bootstrapList.length === 1, 'Invalid site argument. Site must match ' +
    'one of the replication endpoints defined');


const destConfig = Object.assign({}, repConfig.destination);
destConfig.bootstrapList = bootstrapList;

const log = new werelogs.Logger('Backbeat:QueueProcessor:task');
werelogs.configure({ level: config.log.logLevel,
    dump: config.log.dumpLevel });

const metricsProducer = new MetricsProducer(kafkaConfig, mConfig);
const { topic } = repConfig;
const queueProcessor = new QueueProcessor(
    topic, kafkaConfig, sourceConfig, destConfig, repConfig,
    httpsConfig, internalHttpsConfig, site, metricsProducer, null
);

/**
 * Get probe config will pull the configuration for the probe server based on
 * the provided site key.
 *
 * @param {Object} queueProcessorConfig - Configuration of the queue processor that
 *      holds the probe server configs for all sites
 * @param {string} site - Name of the site we are processing
 * @returns {ProbeServerConfig|undefined} Config for site or undefined if not found
 */
function getProbeConfig(queueProcessorConfig, site) {
    return queueProcessorConfig &&
        queueProcessorConfig.probeServer &&
        queueProcessorConfig.probeServer.filter(c => c.site === site)[0];
}

async.waterfall([
    done => startProbeServer(
        getProbeConfig(repConfig.queueProcessor, site),
        (err, probeServer) => {
            if (err) {
                log.error('error starting probe server', {
                    error: err,
                    method: 'QueueProcessor::startProbeServer',
                });
                done(err);
                return;
            }
            if (probeServer !== undefined) {
                probeServer.addHandler(
                    DEFAULT_LIVE_ROUTE,
                    (res, log) => queueProcessor.handleLiveness(res, log)
                );
                probeServer.addHandler(
                    DEFAULT_METRICS_ROUTE,
                    (res, log) => queueProcessor.handleMetrics(res, log)
                );
            }
            done();
        }
    ),
    done => {
        metricsProducer.setupProducer(err => {
            if (err) {
                log.error('error starting metrics producer for queue processor', {
                    error: err,
                    method: 'MetricsProducer::setupProducer',
                });
            }
            done(err);
        });
    },
    done => {
        queueProcessor.on('ready', done);
        queueProcessor.start();
    },
], err => {
    if (err) {
        log.error('error during queue processor initialization', {
            method: 'QueueProcessor::task',
            error: err,
        });
        process.exit(1);
    }
});

process.on('SIGTERM', () => {
    log.info('received SIGTERM, exiting');
    queueProcessor.stop(() => {
        process.exit(0);
    });
});
