'use strict'; // eslint-disable-line strict

const werelogs = require('werelogs');

const runServer = require('./lib/api/BackbeatServer');
const { initManagement } = require('./lib/management/index');

const testIsOn = process.env.CI === 'true';
const config = testIsOn ?
    require('./tests/config.json') : require('./conf/Config');

const Logger = werelogs.Logger;
const log = new Logger('BackbeatServer:index');

werelogs.configure({
    level: config.log.logLevel,
    dump: config.log.dumpLevel,
});

function initAndStart() {
    const repConfig = config.extensions.replication;
    const sourceConfig = repConfig.source;
    initManagement({
        serviceName: 'replication',
        serviceAccount: sourceConfig.auth.account,
    }, error => {
        if (error) {
            log.error('could not load management db', {
                error: error.message,
            });
            setTimeout(initAndStart, 5000);
            return;
        }
        log.info('management init done');

        const bootstrapList = config.getBootstrapList();
        repConfig.destination.bootstrapList = bootstrapList;

        config.on('bootstrap-list-update', () => {
            repConfig.destination.bootstrapList = config.getBootstrapList();
        });

        runServer(config, Logger);
    });
}

if (testIsOn) {
    // skip initManagement
    runServer(config, Logger);
} else {
    initAndStart();
}
