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
const { startProbeServer } = require('./Probe');

const site = process.argv[2];
assert(site, 'QueueProcessor task must be started with a site as argument');

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
const queueProcessor = new QueueProcessor(
    kafkaConfig, sourceConfig, destConfig, repConfig,
    httpsConfig, internalHttpsConfig, site, metricsProducer
);

async.waterfall([
    done => startProbeServer(
        queueProcessor,
        repConfig.queueProcessor.probeServer,
        err => {
            if (err) {
                log.error('error starting probe server', {
                    error: err,
                    method: 'QueueProcessor::startProbeServer',
                });
            }
            done(err);
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
