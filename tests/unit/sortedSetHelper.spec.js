const assert = require('assert');
const { redisKeys } = require('../../extensions/replication/constants');

const {
    getSortedSetMember,
    getSortedSetKey,
} = require('../../lib/util/sortedSetHelper');

describe('sorted set helper methods', () => {
    describe('getSortedSetMember', () => {
        it('should return the correct Redis sorted set member schema when ' +
        'using a version', () => {
            const member = getSortedSetMember('a', 'b', 'c');
            assert.strictEqual(member, 'a:b:c');
        });

        it('should return the correct Redis sorted set member schema with no ' +
        'version', () => {
            const member = getSortedSetMember('a', 'b');
            assert.strictEqual(member, 'a:b:');
        });
    });

    describe('getSortedSetKey', () => {
        it('should return the correct Redis sorted set key schema', () => {
            const key = getSortedSetKey('a', 'b', 'c');
            assert.strictEqual(key, `${redisKeys.failedCRR}:a:b`);
        });
    });
});
