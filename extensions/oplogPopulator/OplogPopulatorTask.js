'use strict'; // eslint-disable-line

const werelogs = require('werelogs');
const config = require('../../lib/Config');
const OplogPopulator = require('./OplogPopulator');

const logger = new werelogs.Logger('Backbeat:OplogPopulator');
werelogs.configure({ level: config.log.logLevel,
    dump: config.log.dumpLevel });

const mongoConfig = config.queuePopulator.mongo;
const oplogPopulatorConfig = config.extensions.oplogPopulator;
// Temporary as no extension uses the oplogPopulator for now
const activeExtensions = [];

const oplogPopulator = new OplogPopulator({
    config: oplogPopulatorConfig,
    mongoConfig,
    activeExtensions,
    logger,
});

(async () => {
    try {
        await oplogPopulator.setup();
    } catch (error) {
        logger.error('Error when starting up the oplog populator', {
            method: 'OplogPopulatorTask.setup',
            error: error.description || error.message,
        });
        process.exit(0);
    }
})();
