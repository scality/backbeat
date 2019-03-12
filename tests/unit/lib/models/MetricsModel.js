const assert = require('assert');

const MetricsModel = require('../../../../lib/models/MetricsModel');

const mock = {
    ops: 1,
    bytes: 1,
    extension: 'a',
    type: 'b',
    site: 'c',
    bucketName: 'd',
    objectKey: 'e',
    versionId: 'f',
};

describe('MetricsModel', () => {
    it('should set a timestamp when instantiated', () => {
        const metrics = new MetricsModel();
        const data = JSON.parse(metrics.serialize());
        assert(data.timestamp);
    });

    it('should serialize data when values passed to constructor', () => {
        const metrics = new MetricsModel(
            mock.ops,
            mock.bytes,
            mock.extension,
            mock.type,
            mock.site,
            mock.bucketName,
            mock.objectKey,
            mock.versionId);
        const data = JSON.parse(metrics.serialize());
        Object.keys(mock)
            .forEach(key => assert.strictEqual(data[key], mock[key]));
    });

    it('should serialize data when values passed to setters', () => {
        const metrics = new MetricsModel()
            .withOps(mock.ops)
            .withBytes(mock.bytes)
            .withExtension(mock.extension)
            .withMetricType(mock.type)
            .withSite(mock.site)
            .withBucketName(mock.bucketName)
            .withObjectKey(mock.objectKey)
            .withVersionId(mock.versionId);
        const data = JSON.parse(metrics.serialize());
        Object.keys(mock)
            .forEach(key => assert.strictEqual(data[key], mock[key]));
    });
});
