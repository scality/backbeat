const assert = require('assert');

const {
    ownedConnectorPrefix,
    hashedConnectorName,
    parseHashedConnectorName,
} = require('../../../extensions/oplogPopulator/modules/connectorNaming');

describe('connectorNaming', () => {
    const prefix = 'backbeat-oplog-';

    it('should name connectors after a short hash of their pipeline', () => {
        const name = pipeline => hashedConnectorName({ prefix, generation: 1, nbConnectors: 1, index: 0, pipeline });
        assert.match(name('[]'), /^backbeat-oplog-source-connector-g1-1-0-[0-9a-f]{8}$/);
        assert.notStrictEqual(name('[]'), name('[{}]'));
    });

    it('should prefix owned connectors', () => {
        assert.strictEqual(ownedConnectorPrefix(prefix), 'backbeat-oplog-source-connector-');
    });

    it('should round-trip hashed connector names', () => {
        const name = hashedConnectorName({ prefix, generation: 2, nbConnectors: 4, index: 3, pipeline: '[]' });
        assert.match(name, /^backbeat-oplog-source-connector-g2-4-3-[0-9a-f]{8}$/);
        assert.deepStrictEqual(parseHashedConnectorName(prefix, name),
            { generation: 2, nbConnectors: 4, index: 3, hash: name.slice(-8) });
    });

    [
        ['per-bucket connector', 'backbeat-oplog-source-connector-3f1c7a52-9b1e-4c1d-8f2e-6a7b8c9d0e1f'],
        ['other prefix', 'other-source-connector-g1-4-0-0123abcd'],
        ['index out of range', 'backbeat-oplog-source-connector-g1-4-4-0123abcd'],
        ['no connectors', 'backbeat-oplog-source-connector-g1-0-0-0123abcd'],
        ['bad hash', 'backbeat-oplog-source-connector-g1-4-0-0123ABCD'],
        ['trailing characters', 'backbeat-oplog-source-connector-g1-4-0-0123abcd-x'],
    ].forEach(([desc, name]) => {
        it(`should not parse a ${desc} as a hashed connector`, () => {
            assert.strictEqual(parseHashedConnectorName(prefix, name), null);
        });
    });
});
