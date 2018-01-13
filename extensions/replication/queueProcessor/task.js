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
const queueProcessor = new QueueProcessor(zkConfig,
                                          sourceConfig, destConfig,
                                          repConfig, site);

werelogs.configure({ level: config.log.logLevel,
    dump: config.log.dumpLevel });

queueProcessor.start();
