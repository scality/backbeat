'use strict'; // eslint-disable-line
const werelogs = require('werelogs');

const LifecycleConsumer = require('./LifecycleConsumer');

const config = require('../../../conf/Config');
const zkConfig = config.zookeeper;
const kafkaConfig = config.kafka;
const lcConfig = config.extensions.lifecycle;
const s3Config = config.s3;
const authConfig = config.auth;
const transport = config.transport;

const log = new werelogs.Logger('Backbeat:Lifecycle:Consumer');

const lifecycleConsumer = new LifecycleConsumer(
    zkConfig, kafkaConfig, lcConfig, s3Config, authConfig, transport);

werelogs.configure({ level: config.log.logLevel,
    dump: config.log.dumpLevel });

lifecycleConsumer.start();

process.on('SIGTERM', () => {
    log.info('received SIGTERM, exiting');
    lifecycleConsumer.close(() => {
        process.exit(0);
    });
});
