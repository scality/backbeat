'use strict';

const assert = require('assert');

const {
    OVERRIDES_ENV,
    applyMergePatch,
    applyConfigOverrides,
} = require('../../../../lib/config/configOverrides');

describe('config/configOverrides', () => {
    describe('applyMergePatch (RFC 7386)', () => {
        it('merges nested objects recursively', () => {
            const target = { kafka: { hosts: 'a:9092', producerParams: { acks: 1 } } };
            const patch = { kafka: { producerParams: { 'linger.ms': 50 } } };
            const result = applyMergePatch(target, patch);
            assert.deepStrictEqual(result, {
                kafka: {
                    hosts: 'a:9092',
                    producerParams: { 'acks': 1, 'linger.ms': 50 },
                },
            });
        });

        it('replaces arrays wholesale (arrays are not merged)', () => {
            const target = { servers: ['a', 'b'], nested: { list: [1, 2, 3] } };
            const patch = { servers: ['c'], nested: { list: [9] } };
            const result = applyMergePatch(target, patch);
            assert.deepStrictEqual(result, {
                servers: ['c'],
                nested: { list: [9] },
            });
        });

        it('replaces scalars', () => {
            const target = { port: 8080, name: 'old' };
            const patch = { port: 9090, name: 'new' };
            assert.deepStrictEqual(applyMergePatch(target, patch), { port: 9090, name: 'new' });
        });

        it('deletes keys when the patch value is null', () => {
            const target = { a: 1, b: { c: 2, d: 3 } };
            const patch = { a: null, b: { c: null } };
            const result = applyMergePatch(target, patch);
            assert.deepStrictEqual(result, { b: { d: 3 } });
        });

        it('adds keys that do not exist in target', () => {
            const target = { a: 1 };
            const patch = { b: { c: 2 } };
            assert.deepStrictEqual(applyMergePatch(target, patch), { a: 1, b: { c: 2 } });
        });

        it('replaces target when patch is not an object', () => {
            assert.strictEqual(applyMergePatch({ a: 1 }, 42), 42);
            assert.strictEqual(applyMergePatch({ a: 1 }, 'str'), 'str');
            assert.deepStrictEqual(applyMergePatch({ a: 1 }, [1, 2]), [1, 2]);
        });

        it('replaces a scalar/array target when patch is an object', () => {
            assert.deepStrictEqual(applyMergePatch(5, { a: 1 }), { a: 1 });
            assert.deepStrictEqual(applyMergePatch([1, 2], { a: 1 }), { a: 1 });
        });

        it('ignores a deletion when the key is absent from the target', () => {
            const target = { a: 1 };
            const patch = { b: null };
            assert.deepStrictEqual(applyMergePatch(target, patch), { a: 1 });
        });

        it('handles dotted keys as plain object keys (BB-685 style)', () => {
            const target = { kafka: { producerParams: { 'compression.type': 'none' } } };
            const patch = {
                kafka: {
                    producerParams: {
                        'compression.type': 'snappy',
                        'batch.size': 16384,
                    },
                },
            };
            const result = applyMergePatch(target, patch);
            assert.deepStrictEqual(result.kafka.producerParams, {
                'compression.type': 'snappy',
                'batch.size': 16384,
            });
        });

        it('ignores reserved keys to prevent prototype pollution', () => {
            const target = {};
            const patch = JSON.parse(
                '{"__proto__": {"polluted": true},'
                + ' "constructor": {"prototype": {"polluted2": true}}}');
            applyMergePatch(target, patch);
            assert.strictEqual({}.polluted, undefined);
            assert.strictEqual({}.polluted2, undefined);
        });
    });

    describe('applyConfigOverrides', () => {
        it('returns config unchanged when the env var is not set', () => {
            const cfg = { a: 1 };
            assert.strictEqual(applyConfigOverrides(cfg, {}), cfg);
        });

        it('returns config unchanged when the env var is an empty string', () => {
            const cfg = { a: 1 };
            assert.strictEqual(applyConfigOverrides(cfg, { [OVERRIDES_ENV]: '' }), cfg);
        });

        it('applies a JSON merge patch from the env var', () => {
            const cfg = { kafka: { hosts: 'a:9092' }, extensions: { replication: { enabled: true } } };
            const env = {
                [OVERRIDES_ENV]: JSON.stringify({
                    kafka: { hosts: 'b:9092' },
                    extensions: { replication: { enabled: null } },
                }),
            };
            const result = applyConfigOverrides(cfg, env);
            assert.deepStrictEqual(result, {
                kafka: { hosts: 'b:9092' },
                extensions: { replication: {} },
            });
        });

        it('overrides take precedence over file values', () => {
            const cfg = { server: { port: 8080 } };
            const env = { [OVERRIDES_ENV]: JSON.stringify({ server: { port: 9090 } }) };
            const result = applyConfigOverrides(cfg, env);
            assert.strictEqual(result.server.port, 9090);
        });

        it('throws a helpful error on invalid JSON', () => {
            assert.throws(
                () => applyConfigOverrides({}, { [OVERRIDES_ENV]: '{not-json' }),
                /could not parse BACKBEAT_CONFIG_OVERRIDES as JSON/,
            );
        });

        it('defaults to process.env when no env argument is provided', () => {
            const previous = process.env[OVERRIDES_ENV];
            process.env[OVERRIDES_ENV] = JSON.stringify({ added: true });
            try {
                const result = applyConfigOverrides({ existing: 1 });
                assert.deepStrictEqual(result, { existing: 1, added: true });
            } finally {
                if (previous === undefined) {
                    delete process.env[OVERRIDES_ENV];
                } else {
                    process.env[OVERRIDES_ENV] = previous;
                }
            }
        });
    });
});
