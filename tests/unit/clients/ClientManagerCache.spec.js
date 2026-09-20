const assert = require('assert');
const sinon = require('sinon');
const { EventEmitter } = require('events');

const ClientManagerCache = require('../../../lib/clients/ClientManagerCache');

function fakeManager() {
    return Object.assign(new EventEmitter(), { close: sinon.stub() });
}

describe('ClientManagerCache', () => {
    let cache;

    beforeEach(() => {
        cache = new ClientManagerCache();
    });

    describe('getOrCreate', () => {
        it('should build the manager on first use only', () => {
            const manager = fakeManager();
            const create = sinon.stub().returns(manager);

            assert.strictEqual(cache.getOrCreate('key', create), manager);
            assert.strictEqual(cache.getOrCreate('key', create), manager);
            assert(create.calledOnce);
            assert.strictEqual(cache.size, 1);
        });

        it('should hold one manager per key', () => {
            const first = cache.getOrCreate('read-role', fakeManager);
            const second = cache.getOrCreate('write-role', fakeManager);

            assert.notStrictEqual(first, second);
            assert.strictEqual(cache.size, 2);
        });
    });

    describe('idle managers', () => {
        it('should drop and close a manager that went idle', () => {
            const manager = cache.getOrCreate('key', fakeManager);

            manager.emit('idle');

            assert.strictEqual(cache.size, 0);
            assert(manager.close.calledOnce);
        });

        it('should build a fresh manager once the idle one was dropped', () => {
            const first = cache.getOrCreate('key', fakeManager);
            first.emit('idle');

            const second = cache.getOrCreate('key', fakeManager);

            assert.notStrictEqual(second, first);
            assert.strictEqual(cache.size, 1);
        });

        it('should keep the manager cached under a key it no longer owns', () => {
            const first = cache.getOrCreate('key', fakeManager);
            first.emit('idle');
            const second = cache.getOrCreate('key', fakeManager);

            first.emit('idle');

            assert.strictEqual(cache.getOrCreate('key', fakeManager), second);
            assert.strictEqual(cache.size, 1);
        });
    });

    describe('close', () => {
        it('should close every manager and forget them', () => {
            const first = cache.getOrCreate('read-role', fakeManager);
            const second = cache.getOrCreate('write-role', fakeManager);

            cache.close();

            assert(first.close.calledOnce);
            assert(second.close.calledOnce);
            assert.strictEqual(cache.size, 0);
        });

        it('should be safe to call on an empty cache', () => {
            assert.doesNotThrow(() => cache.close());
        });
    });
});
