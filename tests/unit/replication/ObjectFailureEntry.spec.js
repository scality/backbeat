const assert = require('assert');

const ObjectFailureEntry =
    require('../../../extensions/replication/utils/ObjectFailureEntry');

describe('ObjectFailureEntry helper class', () => {
    const key = 'bb:crr:failed:test-bucket:test-key:test-versionId:test-site:' +
        '0123456789';
    const role = 'arn:aws:iam::604563867484:test-role';
    const entry = new ObjectFailureEntry(key, role);

    it('should get the redis key', () =>
        assert.strictEqual(entry.getRedisKey(), key));

    it('should get the service', () =>
        assert.strictEqual(entry.getService(), 'bb'));

    it('should get the extension', () =>
        assert.strictEqual(entry.getExtension(), 'crr'));

    it('should get the status', () =>
        assert.strictEqual(entry.getStatus(), 'failed'));

    it('should get the bucket', () =>
        assert.strictEqual(entry.getBucket(), 'test-bucket'));

    it('should get the object key', () =>
        assert.strictEqual(entry.getObjectKey(), 'test-key'));

    it('should get the encoded version id', () =>
        assert.strictEqual(entry.getEncodedVersionId(), 'test-versionId'));

    it('should get the site', () =>
        assert.strictEqual(entry.getSite(), 'test-site'));

    it('should get the timestamp', () =>
        assert.strictEqual(entry.getTimestamp(), '0123456789'));

    it('should get the replication roles', () =>
        assert.strictEqual(entry.getReplicationRoles(), role));

    it('should get the log info', () =>
        assert.deepStrictEqual(entry.getLogInfo(), {
            bucket: 'test-bucket',
            objectKey: 'test-key',
            encodedVersionId: 'test-versionId',
        }));
});
