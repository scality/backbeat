const assert = require('assert');

const ObjectFailureEntry =
    require('../../../extensions/replication/utils/ObjectFailureEntry');

describe('ObjectFailureEntry helper class', () => {
    const member = 'test-bucket:test-key:test-versionId';
    const sitename = 'test-site';
    const entry = new ObjectFailureEntry(member, sitename);

    it('should get the bucket', () =>
        assert.strictEqual(entry.getBucket(), 'test-bucket'));

    it('should get the object key', () =>
        assert.strictEqual(entry.getObjectKey(), 'test-key'));

    it('should get the encoded version id', () =>
        assert.strictEqual(entry.getEncodedVersionId(), 'test-versionId'));

    it('should get the site', () =>
        assert.strictEqual(entry.getSite(), sitename));

    it('should get the member', () =>
        assert.strictEqual(entry.getMember(), member));

    it('should get the log info', () =>
        assert.deepStrictEqual(entry.getLogInfo(), {
            bucket: 'test-bucket',
            objectKey: 'test-key',
            encodedVersionId: 'test-versionId',
        }));
});
