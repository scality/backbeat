'use strict'; // eslint-disable-line

const werelogs = require('werelogs');

const GarbageCollector = require('./GarbageCollector');

const config = require('../../conf/Config');
const kafkaConfig = config.kafka;
const s3Config = config.s3;
const gcConfig = config.extensions.gc;
const transport = config.transport;

const { initManagement } = require('../../lib/management');
const garbageCollector = new GarbageCollector({
    kafkaConfig,
    s3Config,
    gcConfig,
    transport,
});

werelogs.configure({ level: config.log.logLevel,
                     dump: config.log.dumpLevel });
const logger = new werelogs.Logger('Backbeat:GC:service');

function initAndStart() {
    initManagement({
        serviceName: 'gc',
        serviceAccount: gcConfig.auth.account,
    }, error => {
        if (error) {
            logger.error('could not load management db', error);
            setTimeout(initAndStart, 5000);
            return;
        }
        logger.info('management init done');
        garbageCollector.start(err => {
            if (err) {
                logger.error('error during garbage collector initialization',
                             { error: err.message });
            } else {
                logger.info('garbage collector is running');
            }
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
