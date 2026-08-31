'use strict';

const assert = require('assert');

const { applyConfigOverrides, mergePatch } = require('../../../../lib/config/configOverrides');

const CONFIG_OVERRIDES = 'BACKBEAT_CONFIG_OVERRIDES';

describe('config overrides', () => {
    describe('mergePatch', () => {
        // the test cases of RFC 7386, appendix A
        [
            [{ a: 'b' }, { a: 'c' }, { a: 'c' }],
            [{ a: 'b' }, { b: 'c' }, { a: 'b', b: 'c' }],
            [{ a: 'b' }, { a: null }, {}],
            [{ a: 'b', b: 'c' }, { a: null }, { b: 'c' }],
            [{ a: ['b'] }, { a: 'c' }, { a: 'c' }],
            [{ a: 'c' }, { a: ['b'] }, { a: ['b'] }],
            [{ a: { b: 'c' } }, { a: { b: 'd', c: null } }, { a: { b: 'd' } }],
            [{ a: [{ b: 'c' }] }, { a: [1] }, { a: [1] }],
            [['a', 'b'], ['c', 'd'], ['c', 'd']],
            [{ a: 'b' }, ['c'], ['c']],
            [{ a: 'foo' }, null, null],
            [{ a: 'foo' }, 'bar', 'bar'],
            [{ e: null }, { a: 1 }, { e: null, a: 1 }],
            [[1, 2], { a: 'b', c: null }, { a: 'b' }],
            [{}, { a: { bb: { ccc: null } } }, { a: { bb: {} } }],
        ].forEach(([target, patch, expected]) => {
            const title = `${JSON.stringify(target)} + ${JSON.stringify(patch)} ` +
                          `= ${JSON.stringify(expected)}`;
            it(`should merge ${title}`, () => {
                assert.deepStrictEqual(mergePatch(target, patch), expected);
            });
        });

        it('should merge nested objects recursively', () => {
            assert.deepStrictEqual(
                mergePatch({ a: { b: 1, c: { d: 2, e: 3 } } },
                           { a: { c: { e: 4 } } }),
                { a: { b: 1, c: { d: 2, e: 4 } } });
        });

        it('should replace an array instead of merging it element-wise', () => {
            assert.deepStrictEqual(
                mergePatch({ list: [1, 2, 3] }, { list: [9] }),
                { list: [9] });
        });

        it('should replace a scalar with an object', () => {
            assert.deepStrictEqual(
                mergePatch({ a: 'scalar' }, { a: { b: 1 } }),
                { a: { b: 1 } });
        });

        it('should create the missing intermediate nodes', () => {
            assert.deepStrictEqual(
                mergePatch({}, { a: { b: { c: 1 } } }),
                { a: { b: { c: 1 } } });
        });

        it('should ignore the deletion of a field that is not set', () => {
            assert.deepStrictEqual(mergePatch({ a: 1 }, { b: null }), { a: 1 });
        });

        it('should update the target in place, so callers keep their reference', () => {
            const target = { a: { b: 1 } };
            const { a } = target;
            mergePatch(target, { a: { c: 2 } });
            assert.strictEqual(target.a, a);
            assert.deepStrictEqual(target, { a: { b: 1, c: 2 } });
        });

        it('should ignore a `__proto__` key instead of reaching the prototype', () => {
            // an object literal would set the prototype rather than a member,
            // so the patch has to be spelled out as JSON
            const patch = JSON.parse('{"a":1,"__proto__":{"polluted":"yes"}}');
            try {
                assert.deepStrictEqual(mergePatch({}, patch), { a: 1 });
                assert.strictEqual({}.polluted, undefined);
            } finally {
                delete Object.prototype.polluted;
            }
        });

        it('should not share the patch structure with the merged result', () => {
            const patch = { a: { b: 1 } };
            const target = mergePatch({}, patch);
            patch.a.b = 2;
            assert.strictEqual(target.a.b, 1);
        });
    });

    describe('applyConfigOverrides', () => {
        const overrides = patch => ({ [CONFIG_OVERRIDES]: JSON.stringify(patch) });

        it('should leave the config alone when the env var is not set', () => {
            const config = { kafka: { hosts: 'localhost:9092' } };
            assert.deepStrictEqual(applyConfigOverrides(config, [], {}),
                                   { kafka: { hosts: 'localhost:9092' } });
        });

        it('should leave the config alone when the env var is empty', () => {
            const config = { kafka: { hosts: 'localhost:9092' } };
            assert.deepStrictEqual(
                applyConfigOverrides(config, [], { [CONFIG_OVERRIDES]: '' }),
                { kafka: { hosts: 'localhost:9092' } });
        });

        it('should reject an invalid JSON document', () => {
            assert.throws(
                () => applyConfigOverrides({}, [], { [CONFIG_OVERRIDES]: '{oops' }),
                /invalid JSON value for BACKBEAT_CONFIG_OVERRIDES/);
        });

        it('should reject a patch that is not an object', () => {
            ['"a string"', '42', 'null', '["a", "list"]'].forEach(patch => {
                assert.throws(
                    () => applyConfigOverrides({}, [], { [CONFIG_OVERRIDES]: patch }),
                    /BACKBEAT_CONFIG_OVERRIDES must hold a JSON object/);
            });
        });

        it('should ignore a `__proto__` key instead of polluting the prototype', () => {
            // an object literal would set the prototype rather than a member,
            // so the patch has to be spelled out as JSON
            const patch = '{"kafka":{"hosts":"other:9092","__proto__":{"nested":"pollution"}},' +
                          '"__proto__":{"polluted":"yes"}}';
            const config = { kafka: { hosts: 'localhost:9092' } };
            try {
                applyConfigOverrides(config, [], { [CONFIG_OVERRIDES]: patch });
                assert.deepStrictEqual(config, { kafka: { hosts: 'other:9092' } });
                assert.strictEqual({}.polluted, undefined);
                assert.strictEqual({}.nested, undefined);
            } finally {
                delete Object.prototype.polluted;
                delete Object.prototype.nested;
            }
        });

        it('should apply the whole patch without a prefix', () => {
            const config = { kafka: { hosts: 'localhost:9092', site: 'here' } };
            applyConfigOverrides(config, [], overrides({ kafka: { hosts: 'other:9092' } }));
            assert.deepStrictEqual(config, { kafka: { hosts: 'other:9092', site: 'here' } });
        });

        it('should apply only the fraction covered by the prefix', () => {
            const extConfig = { topic: 'gc', concurrency: 10 };
            applyConfigOverrides(extConfig, ['extensions', 'gc'], overrides({
                kafka: { hosts: 'other:9092' },
                extensions: {
                    gc: { topic: 'patched-gc' },
                    lifecycle: { zookeeperPath: '/patched' },
                },
            }));
            assert.deepStrictEqual(extConfig, { topic: 'patched-gc', concurrency: 10 });
        });

        it('should leave the config alone when the prefix is not covered', () => {
            const extConfig = { topic: 'gc' };
            applyConfigOverrides(extConfig, ['extensions', 'gc'],
                                 overrides({ extensions: { lifecycle: { zookeeperPath: '/p' } } }));
            assert.deepStrictEqual(extConfig, { topic: 'gc' });
        });

        it('should return the config when the prefix is not covered', () => {
            const extConfig = { topic: 'gc' };
            const returned = applyConfigOverrides(extConfig, ['extensions', 'gc'],
                                                  overrides({ kafka: { hosts: 'other:9092' } }));
            assert.strictEqual(returned, extConfig);
        });

        it('should leave the config alone when the prefix breaks early', () => {
            const extConfig = { topic: 'gc' };
            applyConfigOverrides(extConfig, ['extensions', 'gc'],
                                 overrides({ kafka: { hosts: 'other:9092' } }));
            assert.deepStrictEqual(extConfig, { topic: 'gc' });
        });

        it('should return the replacement when the fraction is not an object', () => {
            // such a fraction replaces the section instead of updating it, so it
            // cannot be applied in place: the schema reports whatever it holds
            ['nonsense', 42, null, ['a', 'list']].forEach(gc => {
                assert.deepStrictEqual(
                    applyConfigOverrides({ topic: 'gc' }, ['extensions', 'gc'],
                        overrides({ extensions: { gc } })),
                    gc);
            });
        });

        it('should update the config in place', () => {
            const config = { kafka: { hosts: 'localhost:9092' } };
            applyConfigOverrides(config, [], overrides({ kafka: { hosts: 'other:9092' } }));
            assert.strictEqual(config.kafka.hosts, 'other:9092');
        });

        it('should read the overrides from the process environment by default', () => {
            process.env[CONFIG_OVERRIDES] = '{"kafka":{"hosts":"from-process-env"}}';
            try {
                const config = { kafka: { hosts: 'localhost:9092' } };
                applyConfigOverrides(config);
                assert.strictEqual(config.kafka.hosts, 'from-process-env');
            } finally {
                delete process.env[CONFIG_OVERRIDES];
            }
        });
    });
});
