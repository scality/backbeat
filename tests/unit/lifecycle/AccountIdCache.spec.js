'use strict';

const assert = require('assert');
const sinon = require('sinon');
const { AccountIdCache } = require('../../../extensions/utils/AccountIdCache');

describe('accound id cache', () => {
    it('should store values', () => {
        const cache = new AccountIdCache(2);

        cache.set('a', 1);
        assert.deepStrictEqual(cache.get('a'), 1);
    });

    it('should expose its current size', () => {
        const cache = new AccountIdCache(2);

        cache.set('a', 1);
        cache.set('b', 2);
        cache.set('c', 3);

        assert.deepStrictEqual(cache.size, 3);
    });

    it('should expire oldest values', () => {
        const cache = new AccountIdCache(2);

        cache.set('a', 1);
        cache.set('b', 2);
        cache.set('c', 3);
        cache.set('d', 4);

        cache.expireOldest();

        assert.deepStrictEqual(cache.size, 2);
        assert.deepStrictEqual(cache.get('a'), undefined);
        assert.deepStrictEqual(cache.get('b'), undefined);
        assert.deepStrictEqual(cache.get('c'), 3);
        assert.deepStrictEqual(cache.get('d'), 4);
    });

    it('should not expire if not full', () => {
        const cache = new AccountIdCache(3);

        cache.set('a', 1);
        cache.set('b', 2);
        cache.set('c', 3);

        cache.expireOldest();

        assert.deepStrictEqual(cache.size, 3);
        assert.deepStrictEqual(cache.get('a'), 1);
        assert.deepStrictEqual(cache.get('b'), 2);
        assert.deepStrictEqual(cache.get('c'), 3);
    });

    it('should store misses', () => {
        const cache = new AccountIdCache(1);

        cache.miss('abc');
        assert.deepStrictEqual(cache.isMiss('abc'), true);
    });

    it('should not store non-misses', () => {
        const cache = new AccountIdCache(1);

        cache.miss('abc');
        assert.deepStrictEqual(cache.isMiss('def'), false);
    });

    it('should know misses', () => {
        const cache = new AccountIdCache(1);

        cache.miss('abc');
        assert.deepStrictEqual(cache.isKnown('abc'), true);
    });

    it('should know cache entries', () => {
        const cache = new AccountIdCache(1);

        cache.set('a', 1);
        assert.deepStrictEqual(cache.isKnown('a'), true);
    });

    it('should not know random keys', () => {
        const cache = new AccountIdCache(1);

        assert.deepStrictEqual(cache.isKnown('a'), false);
    });

    it('should dump misses', () => {
        const cache = new AccountIdCache(2);

        cache.miss('def');
        cache.miss('abc');

        assert.deepStrictEqual(cache.getMisses(), ['abc', 'def']);
    });

    describe('miss expiration', () => {
        let clock;

        beforeEach(() => {
            clock = sinon.useFakeTimers();
        });

        afterEach(() => {
            clock.restore();
        });

        it('should forget misses after the TTL', () => {
            const cache = new AccountIdCache(1, 1000);

            cache.miss('abc');
            clock.tick(1001);

            assert.deepStrictEqual(cache.isMiss('abc'), false);
            assert.deepStrictEqual(cache.isKnown('abc'), false);
            assert.deepStrictEqual(cache.getMisses(), []);
        });

        it('should keep misses until the TTL', () => {
            const cache = new AccountIdCache(1, 1000);

            cache.miss('abc');
            clock.tick(999);

            assert.deepStrictEqual(cache.isMiss('abc'), true);
            assert.deepStrictEqual(cache.getMisses(), ['abc']);
        });

        it('should refresh the TTL on a new miss', () => {
            const cache = new AccountIdCache(1, 1000);

            cache.miss('abc');
            clock.tick(999);
            cache.miss('abc');
            clock.tick(999);

            assert.deepStrictEqual(cache.isMiss('abc'), true);
        });

        it('should drop expired misses on expireOldest', () => {
            const cache = new AccountIdCache(1, 1000);

            cache.miss('abc');
            clock.tick(1001);
            cache.miss('def');
            cache.expireOldest();

            assert.deepStrictEqual([...cache.misses.keys()], ['def']);
        });

        it('should bound the number of misses kept', () => {
            const cache = new AccountIdCache(1, 1000, 2);

            cache.miss('abc');
            cache.miss('def');
            cache.miss('ghi');

            assert.deepStrictEqual([...cache.misses.keys()], ['def', 'ghi']);
        });
    });
});
