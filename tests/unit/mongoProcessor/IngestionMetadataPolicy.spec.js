'use strict';

const assert = require('assert');

const IngestionMetadataPolicy =
    require('../../../extensions/mongoProcessor/metadataPolicy/IngestionMetadataPolicy');
const ObjectQueueEntry = require('../../../lib/models/ObjectQueueEntry');
const { listEntries } = require('../../utils/nullVersionRows');

const bucketInfo = {
    getOwner: () => 'owner-id',
    getOwnerDisplayName: () => 'owner',
};

describe('IngestionMetadataPolicy', () => {
    const policy = new IngestionMetadataPolicy();

    describe('apply: written version', () => {
        // a null version is put as the master, with no version id, like a
        // non-versioned object: only an object living in a version key is
        // put as a version
        listEntries({ 'content-length': 128, 'key': 'obj' }).forEach(({ desc, key, value }) => {
            const expected = value.versionId && !value.isNull ? value.versionId : undefined;
            it(`should return ${expected ? 'the version id' : 'no version id'} for ${desc}`, () => {
                const entry = new ObjectQueueEntry('bucket', key, value);
                const applied = policy.apply(entry, null, 'location', bucketInfo);
                assert.strictEqual(applied.versionId, expected);
            });
        });
    });
});
