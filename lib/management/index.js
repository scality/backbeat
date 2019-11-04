const config = require('../Config');
const werelogs = require('werelogs');

werelogs.configure({
    level: config.log.logLevel,
    dump: config.log.dumpLevel,
});

const logger = new werelogs.Logger('management:facade');

function loadManagementBackend() {
    const backendName = process.env.MANAGEMENT_BACKEND;
    const backend = backendName === 'operator' ?
        'operatorBackend' : 'pensieveBackend';

    const mod = require(`./${backend}`);

    if (!mod) {
        logger.error('management backend not found', { backendName });
        process.exit(1);
    }

    return mod;
}

module.exports = loadManagementBackend();
