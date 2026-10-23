const assert = require('assert');

const { extractVersionId, hasVersionKey, isShadowedMaster } =
    require('../../../../lib/util/versioning');
const { listEntries } = require('../../../utils/nullVersionRows');

describe('versioning utils', () => {
    describe('extractVersionId', () => {
        it('should return null for a master key', () => {
            assert.strictEqual(extractVersionId('obj'), null);
        });

        it('should return the version id of a version key', () => {
            assert.strictEqual(extractVersionId('obj\x00vid'), 'vid');
        });

        it('should return an empty string for a null key', () => {
            assert.strictEqual(extractVersionId('obj\x00'), '');
        });
    });

    describe('hasVersionKey', () => {
        it('should be false without a version id', () => {
            assert.strictEqual(hasVersionKey({}), false);
        });

        it('should be true for a versioned object', () => {
            assert.strictEqual(hasVersionKey({ versionId: 'vid' }), true);
        });

        it('should be false for a null version', () => {
            assert.strictEqual(hasVersionKey({ versionId: 'vid', isNull: true }), false);
        });
    });

    describe('isShadowedMaster', () => {
        listEntries().forEach(({ desc, key, value, shadowed }) => {
            it(`should be ${shadowed} for ${desc}`, () => {
                assert.strictEqual(isShadowedMaster(key, value), shadowed);
            });
        });
    });
});
