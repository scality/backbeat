const assert = require('assert');
const { redisKeys } = require('../../extensions/replication/constants');

const {
    getSortedSetMember,
    getSortedSetKey,
    getObjectBytesKey,
    getObjectBytesDoneKey,
} = require('../../lib/util/getRedisKeys');

describe('get Redis keys helper methods', () => {
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

    describe('getObjectBytesKey', () => {
        it('should return the correct Redis object-level bytes key schema',
        () => {
            const key = getObjectBytesKey('a', 'b', 'c', 'd');
            assert.strictEqual(key, `a:b:c:d:${redisKeys.objectBytes}`);
        });
    });

    describe('getObjectBytesDoneKey', () => {
        it('should return the correct Redis object-level bytes done key schema',
        () => {
            const key = getObjectBytesDoneKey('a', 'b', 'c', 'd');
            assert.strictEqual(key, `a:b:c:d:${redisKeys.objectBytesDone}`);
        });
    });
});
