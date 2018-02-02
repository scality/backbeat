'use strict'; // eslint-disable-line
const werelogs = require('werelogs');

const LifecycleConsumer = require('./LifecycleConsumer');

const config = require('../../../conf/Config');
const zkConfig = config.zookeeper;
const lcConfig = config.extensions.lifecycle;
const s3Config = config.s3;
const authConfig = config.auth;
const transport = config.transport;

const lifecycleConsumer = new LifecycleConsumer(
    zkConfig, lcConfig, s3Config, authConfig, transport);

werelogs.configure({ level: config.log.logLevel,
    dump: config.log.dumpLevel });

lifecycleConsumer.start();
