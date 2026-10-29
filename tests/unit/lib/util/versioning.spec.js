const assert = require('assert');
const { models: { ObjectMD }, versioning } = require('@scality/arsenal');

const { extractVersionId, getEncodedVersionId, hasVersionKey, isShadowedMaster } =
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

    describe('getEncodedVersionId', () => {
        const versionId = '98500086134471999999RG001  0';

        [
            { desc: 'a non-versioned object', value: {}, expected: undefined },
            { desc: 'a version', value: { versionId }, expected: versioning.VersionID.encode(versionId) },
            { desc: 'a null version', value: { versionId, isNull: true }, expected: 'null' },
        ].forEach(({ desc, value, expected }) => {
            it(`should return ${expected} for ${desc}`, () => {
                assert.strictEqual(getEncodedVersionId(value), expected);
            });

            it(`should match ObjectMD.getEncodedVersionId() for ${desc}`, () => {
                assert.strictEqual(getEncodedVersionId(value), new ObjectMD(value).getEncodedVersionId());
            });
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
