'use strict'; // eslint-disable-line
const assert = require('assert');
const werelogs = require('werelogs');

const QueueProcessor = require('./QueueProcessor');

const config = require('../../../conf/Config');
const zkConfig = config.zookeeper;
const repConfig = config.extensions.replication;
const sourceConfig = repConfig.source;

const site = process.argv[2];
assert(site, 'QueueProcessor task must be started with a site as argument');

const bootstrapList = repConfig.destination.bootstrapList
    .filter(item => item.site === site);
assert(bootstrapList.length === 1, 'Invalid site argument. Site must match ' +
    'one of the replication endpoints defined');

const destConfig = Object.assign({}, repConfig.destination);
destConfig.bootstrapList = bootstrapList;
const { initManagement } = require('../../../lib/management');

const queueProcessor = new QueueProcessor(zkConfig,
                                          sourceConfig, destConfig,
                                          repConfig, site);

werelogs.configure({ level: config.log.logLevel,
    dump: config.log.dumpLevel });

function initAndStart() {
    initManagement(error => {
        if (error) {
            console.error('could not load managment db', error);
            setTimeout(initAndStart, 5000);
            return;
        }
        console.log('management init done');
        queueProcessor.start();
        // setInterval(initManagement, 5000, error => {
        //     if (error) {
        //         console.error('could not refresh management db', error);
        //     }
        // });
    });
}

initAndStart();
