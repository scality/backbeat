const assert = require('assert');
const { redisKeys } = require('../../extensions/replication/constants');

const getFailedCRRKey = require('../../lib/util/getFailedCRRKey');

describe('getFailedCRRKey', () => {
    it('should return the correct Redis key schema', () => {
        const key = getFailedCRRKey('a', 'b', 'c', 'd');
        assert.strictEqual(key, `${redisKeys.failedCRR}:a:b:c:d`);
    });
});
