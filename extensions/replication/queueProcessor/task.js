'use strict'; // eslint-disable-line
const assert = require('assert');
const werelogs = require('werelogs');

const QueueProcessor = require('./QueueProcessor');

const config = require('../../../conf/Config');
const zkConfig = config.zookeeper;
const repConfig = config.extensions.replication;
const sourceConfig = repConfig.source;
const destConfig = repConfig.destination;

const site = process.argv[2];
assert(site, 'QueueProcessor task must be started with a ' +
    'site as argument');

assert(repConfig.destination.bootstrapList
    .some(item => item.site_name === site), 'Site argument must match ' +
    'a site name in the bootstrapList');

const queueProcessor = new QueueProcessor(zkConfig,
                                          sourceConfig, destConfig,
                                          repConfig, site);

werelogs.configure({ level: config.log.logLevel,
    dump: config.log.dumpLevel });

queueProcessor.start();
