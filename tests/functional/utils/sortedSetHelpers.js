const { StatsModel } = require('arsenal').metrics;

const TEST_REDIS_KEY_FAILED_CRR = 'test:bb:crr:failed';

function addMembers(redisClient, site, members, cb) {
    const statsClient = new StatsModel(redisClient);
    const epoch = Date.now();
    const twelveHoursAgo = epoch - (60 * 60 * 1000) * 12;
    let score = statsClient.normalizeTimestampByHour(new Date(twelveHoursAgo));
    const key = `${TEST_REDIS_KEY_FAILED_CRR}:${site}:${score}`;
    const cmds = members.map(member => (['zadd', key, ++score, member]));
    redisClient.batch(cmds, cb);
}

function addManyMembers(redisClient, site, members, normalizedScore, score,
    cb) {
    let givenScore = score;
    const key = `${TEST_REDIS_KEY_FAILED_CRR}:${site}:${normalizedScore}`;
    const cmds = members.map(member => (['zadd', key, ++givenScore, member]));
    redisClient.batch(cmds, cb);
}

module.exports = { addMembers, addManyMembers };
