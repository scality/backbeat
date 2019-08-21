const { RedisClient } = require('arsenal').metrics;
const config = require('../../../lib/Config');
const werelogs = require('werelogs');

const log = new werelogs.Logger('Backbeat:RedisClient');
werelogs.configure({
    level: config.log.logLevel,
    dump: config.log.dumpLevel,
});

function getRedisClient() {
    const redisConfig = Object.assign({}, config.redis, {
        enableOfflineQueue: false,
    });
    return new RedisClient(redisConfig, log);
}

module.exports = getRedisClient;
