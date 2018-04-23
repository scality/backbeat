const routes = require('arsenal').backbeat.routes;
const redisKeys = require('../../extensions/replication/constants').redisKeys;

const testIsOn = process.env.TEST_SWITCH === '1';
const config = testIsOn ? require('../../tests/config.json') :
                          require('../../conf/Config');
const boostrapList = config.extensions.replication.destination.bootstrapList;
const allSites = boostrapList.map(item => item.site);

module.exports = routes(redisKeys, allSites);
