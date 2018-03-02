'use strict'; // eslint-disable-line

const werelogs = require('werelogs');

const LifecycleConductor = require('./LifecycleConductor');

const config = require('../../../conf/Config');
const zkConfig = config.zookeeper;
const lcConfig = config.extensions.lifecycle;

const lcConductor = new LifecycleConductor(zkConfig, lcConfig);

werelogs.configure({ level: config.log.logLevel,
                     dump: config.log.dumpLevel });
const logger = new werelogs.Logger('Backbeat:Lifecycle:Conductor:service');

lcConductor.start(err => {
    if (err) {
        logger.error('error during lifecycle conductor initialization',
                     { error: err.message });
        return undefined;
    }
    logger.info('lifecycle conductor process is running');
    return undefined;
});

process.on('SIGTERM', () => {
    logger.info('received SIGTERM, exiting');
    lcConductor.stop(() => {
        process.exit(0);
    });
});
