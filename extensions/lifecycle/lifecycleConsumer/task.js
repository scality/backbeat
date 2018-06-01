'use strict'; // eslint-disable-line
const werelogs = require('werelogs');

const { initManagement } = require('../../../lib/management');
const LifecycleConsumer = require('./LifecycleConsumer');

const config = require('../../../conf/Config');
const zkConfig = config.zookeeper;
const kafkaConfig = config.kafka;
const lcConfig = config.extensions.lifecycle;
const s3Config = config.s3;
const transport = config.transport;

const log = new werelogs.Logger('Backbeat:Lifecycle:Consumer');

const lifecycleConsumer = new LifecycleConsumer(
    zkConfig, kafkaConfig, lcConfig, s3Config, transport);

werelogs.configure({ level: config.log.logLevel,
    dump: config.log.dumpLevel });

function initAndStart() {
    initManagement({
        serviceName: 'lifecycle',
        serviceAccount: lcConfig.auth.account,
    }, error => {
        if (error) {
            log.error('could not load management db',
                      { error: error.message });
            setTimeout(initAndStart, 5000);
            return;
        }
        log.info('management init done');

        lifecycleConsumer.start();
    });
}

initAndStart();

process.on('SIGTERM', () => {
    log.info('received SIGTERM, exiting');
    lifecycleConsumer.close(() => {
        process.exit(0);
    });
});
