const assert = require('assert');

const ShadowedMasterFilter = require('../../../../lib/queuePopulator/ShadowedMasterFilter');

const value = JSON.stringify({ key: 'obj', versionId: 'vid', isNull: true });
const version = { type: 'put', bucket: 'bucket', key: 'obj\x00vid', value };
const master = { type: 'put', bucket: 'bucket', key: 'obj', value };

describe('ShadowedMasterFilter', () => {
    let filter;

    beforeEach(() => {
        filter = new ShadowedMasterFilter({ capacity: 3 });
    });

    it('should drop the master following its identical version entry', () => {
        assert.strictEqual(filter.isShadowed(version), false);
        assert.strictEqual(filter.isShadowed(master), true);
    });

    it('should keep a master read twice', () => {
        assert.strictEqual(filter.isShadowed(master), false);
        assert.strictEqual(filter.isShadowed(master), false);
    });

    it('should never drop a version entry', () => {
        assert.strictEqual(filter.isShadowed(master), false);
        assert.strictEqual(filter.isShadowed(version), false);
        assert.strictEqual(filter.isShadowed(version), false);
    });

    it('should keep the master of a regular version', () => {
        const regular = JSON.stringify({ key: 'obj', versionId: 'vid' });
        filter.isShadowed({ ...version, value: regular });
        assert.strictEqual(filter.isShadowed({ ...master, value: regular }), false);
    });

    it('should keep a master whose value differs', () => {
        filter.isShadowed(version);
        const other = JSON.stringify({ key: 'obj', versionId: 'vid', isNull: true, tags: { a: 'b' } });
        assert.strictEqual(filter.isShadowed({ ...master, value: other }), false);
    });

    it('should keep a master whose type differs', () => {
        filter.isShadowed(version);
        assert.strictEqual(filter.isShadowed({ ...master, type: 'delete' }), false);
    });

    it('should keep a master of another bucket', () => {
        filter.isShadowed(version);
        assert.strictEqual(filter.isShadowed({ ...master, bucket: 'other' }), false);
    });

    it('should keep a master of another object', () => {
        filter.isShadowed(version);
        assert.strictEqual(filter.isShadowed({ ...master, key: 'obj2' }), false);
    });

    it('should drop the master even with other objects in between', () => {
        filter.isShadowed(version);
        filter.isShadowed({ ...master, key: 'obj2' });
        filter.isShadowed({ ...master, key: 'obj3' });
        assert.strictEqual(filter.isShadowed(master), true);
    });

    it('should forget the oldest object beyond its capacity', () => {
        filter.isShadowed(version);
        filter.isShadowed({ ...master, key: 'obj2' });
        filter.isShadowed({ ...master, key: 'obj3' });
        filter.isShadowed({ ...master, key: 'obj4' });
        assert.strictEqual(filter.isShadowed(master), false);
    });

    it('should ignore entries without a value or a key', () => {
        filter.isShadowed(version);
        assert.strictEqual(filter.isShadowed({ type: 'del', bucket: 'bucket', key: 'obj' }), false);
        assert.strictEqual(filter.isShadowed({ type: 'put', bucket: 'bucket', value }), false);
        assert.strictEqual(filter.isShadowed(master), true);
    });
});
