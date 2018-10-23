'use strict'; // eslint-disable-line strict

const werelogs = require('werelogs');

const runServer = require('./lib/api/BackbeatServer');

const config = require('./conf/Config');
const Logger = werelogs.Logger;

werelogs.configure({
    level: config.log.logLevel,
    dump: config.log.dumpLevel,
});

runServer(config, Logger);
