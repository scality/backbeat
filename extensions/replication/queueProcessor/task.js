'use strict'; // eslint-disable-line
const werelogs = require('werelogs');
const QueueProcessor = require('./QueueProcessor');
const config = require('../../../conf/Config');
const { initManagement } = require('../../../lib/management');

const zkConfig = config.zookeeper;
const repConfig = config.extensions.replication;
const sourceConfig = repConfig.source;

let replicationEndpoints = config.getReplicationEndpoints();
config.on('replication-endpoints-update', () => {
    replicationEndpoints = config.getReplicationEndpoints();
});

function generateQueueProcessor(replicationEndpoints) {
    const bootstrapList = replicationEndpoints;
    const destConfig = Object.assign({}, repConfig.destination);
    destConfig.bootstrapList = bootstrapList;
    return new QueueProcessor(zkConfig, sourceConfig, destConfig,
      repConfig);
}

werelogs.configure({ level: config.log.logLevel,
    dump: config.log.dumpLevel });

const logger = new werelogs.Logger('queueProcessorInit');
function initAndStart() {
    initManagement(error => {
        if (error) {
            logger.error('could not load managment db', error);
            setTimeout(initAndStart, 5000);
            return;
        }
        logger.info('management init done');
        const queueProcessor = generateQueueProcessor(replicationEndpoints);
        queueProcessor.start();
        // setInterval(initManagement, 5000, error => {
        //     if (error) {
        //         console.error('could not refresh management db', error);
        //     }
        // });
    });
}

initAndStart();
