const assert = require('assert');

const { transformKey } = require('../../../../lib/util/entry');

describe('transformKey', () => {
    it('should not modify v0 keys', done => {
        const entryMasterKey = 'test-object';
        const entryVersionKey = 'test-object\u0000versionId';
        assert.strictEqual(transformKey(entryMasterKey), entryMasterKey);
        assert.strictEqual(transformKey(entryVersionKey), entryVersionKey);
        return done();
    });

    it('should strip prefix from v1 keys', done => {
        const entryMasterKey = '\x7fMtest-object';
        const entryVersionKey = '\x7fVtest-object\u0000versionId';
        assert.strictEqual(transformKey(entryMasterKey), 'test-object');
        assert.strictEqual(transformKey(entryVersionKey), 'test-object\u0000versionId');
        return done();
    });
});
