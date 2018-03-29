'use strict'; // eslint-disable-line

const werelogs = require('werelogs');

const GarbageCollector = require('./GarbageCollector');

const config = require('../../../conf/Config');
const kafkaConfig = config.kafka;
const gcConfig = config.gc;

const garbageCollector = new GarbageCollector(kafkaConfig, gcConfig);

werelogs.configure({ level: config.log.logLevel,
                     dump: config.log.dumpLevel });
const logger = new werelogs.Logger('Backbeat:GC:service');

garbageCollector.start(err => {
    if (err) {
        logger.error('error during garbage collector initialization',
                     { error: err.message });
        return undefined;
    }
    logger.info('garbage collector is running');
    return undefined;
});

process.on('SIGTERM', () => {
    logger.info('received SIGTERM, exiting');
    garbageCollector.stop(() => {
        process.exit(0);
    });
});
