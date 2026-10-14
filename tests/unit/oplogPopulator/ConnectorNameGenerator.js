const assert = require('assert');

const ConnectorNameGenerator =
    require('../../../extensions/oplogPopulator/modules/ConnectorNameGenerator');

describe('ConnectorNameGenerator', () => {
    const generator = new ConnectorNameGenerator('backbeat-oplog-');

    it('should name connectors after a short hash of their pipeline', () => {
        const name = pipeline => generator.hashedName({ nbConnectors: 1, index: 0, pipeline });
        assert.match(name('[]'), /^backbeat-oplog-source-connector-1-0-[0-9a-f]{8}$/);
        assert.strictEqual(name('[]'), name('[]'));
        assert.notStrictEqual(name('[]'), name('[{}]'));
    });

    it('should round-trip hashed connector names', () => {
        const name = generator.hashedName({ nbConnectors: 4, index: 3, pipeline: '[]' });
        assert.match(name, /^backbeat-oplog-source-connector-4-3-[0-9a-f]{8}$/);
        assert.deepStrictEqual(generator.parseHashed(name),
            { nbConnectors: 4, index: 3, hash: name.slice(-8) });
    });

    [
        ['hashed connector', 'backbeat-oplog-source-connector-4-0-0123abcd', true],
        ['per-bucket connector', 'backbeat-oplog-source-connector-3f1c7a52-9b1e-4c1d-8f2e-6a7b8c9d0e1f', true],
        ['connector with another prefix', 'other-source-connector-4-0-0123abcd', false],
        ['foreign connector', 'mongo-sink', false],
    ].forEach(([desc, name, owned]) => {
        it(`should ${owned ? '' : 'not '}own a ${desc}`, () => {
            assert.strictEqual(generator.isOwned(name), owned);
        });
    });

    [
        ['per-bucket connector', 'backbeat-oplog-source-connector-3f1c7a52-9b1e-4c1d-8f2e-6a7b8c9d0e1f'],
        ['other prefix', 'other-source-connector-4-0-0123abcd'],
        ['index out of range', 'backbeat-oplog-source-connector-4-4-0123abcd'],
        ['no connectors', 'backbeat-oplog-source-connector-0-0-0123abcd'],
        ['bad hash', 'backbeat-oplog-source-connector-4-0-0123ABCD'],
        ['trailing characters', 'backbeat-oplog-source-connector-4-0-0123abcd-x'],
        ['generation-named connector', 'backbeat-oplog-source-connector-g1-4-0-0123abcd'],
    ].forEach(([desc, name]) => {
        it(`should not parse a ${desc} as a hashed connector`, () => {
            assert.strictEqual(generator.parseHashed(name), null);
        });
    });

    it('should name connectors without a prefix', () => {
        const unprefixed = new ConnectorNameGenerator();
        const name = unprefixed.hashedName({ nbConnectors: 2, index: 1, pipeline: '[]' });
        assert.match(name, /^source-connector-2-1-[0-9a-f]{8}$/);
        assert(unprefixed.isOwned(name));
        assert.deepStrictEqual(unprefixed.parseHashed(name),
            { nbConnectors: 2, index: 1, hash: name.slice(-8) });
        assert(!generator.isOwned(name));
    });
});
