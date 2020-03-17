'use strict'; // eslint-disable-line strict

const werelogs = require('werelogs');

const runServer = require('../lib/api/BackbeatServer');
const { initManagement } = require('../lib/management/index');
const setupIngestionSiteMock = require('../tests/utils/mockIngestionSite');
const config = require('../lib/Config');
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
        enableIngestionUpdates: true,
    }, error => {
        if (error) {
            log.error('could not load management db', { error });
            setTimeout(initAndStart, 5000);
            return;
        }
        log.info('management init done');

        const bootstrapList = config.getBootstrapList();
        repConfig.destination.bootstrapList = bootstrapList;

        config.on('location-constraints-update', () => {
            repConfig.destination.bootstrapList = config.getBootstrapList();
        });

        runServer(config, Logger);
    });
}

if (process.env.CI === 'true') {
    // skip initManagement
    // set mock config ingestion site on start-up
    setupIngestionSiteMock();
    runServer(config, Logger);
} else {
    initAndStart();
}
