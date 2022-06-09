'use strict'; // eslint-disable-line

const assert = require('assert');
const { AccountIdCache } = require('../../../extensions/lifecycle/util/AccountIdCache');

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
        const cache = new AccountIdCache(1);

        cache.miss('def');
        cache.miss('abc');

        assert.deepStrictEqual(cache.getMisses(), ['abc', 'def']);
    });
});
