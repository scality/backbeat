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

    it('should keep a master whose version id differs', () => {
        filter.isShadowed(version);
        const other = JSON.stringify({ key: 'obj', versionId: 'vid2', isNull: true });
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

    describe('delete of a null version', () => {
        const versionDelete = { ...version, type: 'delete' };
        const phdValue = JSON.stringify({ key: 'obj', versionId: 'phd', isNull: true, isPHD: true });
        const masterDelete = { ...master, type: 'delete', value: phdValue };

        it('should drop the placeholder delete following the version delete', () => {
            assert.strictEqual(filter.isShadowed(versionDelete), false);
            assert.strictEqual(filter.isShadowed(masterDelete), true);
        });

        it('should keep the placeholder delete following a version put', () => {
            filter.isShadowed(version);
            assert.strictEqual(filter.isShadowed(masterDelete), false);
        });

        it('should keep the placeholder delete following a master put', () => {
            filter.isShadowed(master);
            assert.strictEqual(filter.isShadowed(masterDelete), false);
        });

        it('should keep the placeholder delete following a regular version delete', () => {
            const regular = JSON.stringify({ key: 'obj', versionId: 'vid' });
            filter.isShadowed({ ...versionDelete, value: regular });
            assert.strictEqual(filter.isShadowed(masterDelete), false);
        });

        it('should keep the placeholder delete following the version delete of another object', () => {
            filter.isShadowed({ ...versionDelete, key: 'obj2\x00vid' });
            assert.strictEqual(filter.isShadowed(masterDelete), false);
        });

        it('should keep a master delete which is not a placeholder', () => {
            filter.isShadowed(versionDelete);
            assert.strictEqual(filter.isShadowed({ ...master, type: 'delete' }), false);
        });

        it('should drop the placeholder delete of a regular version', () => {
            filter.isShadowed({ ...versionDelete, value: JSON.stringify({ key: 'obj', versionId: 'vid' }) });
            const regularPhd = JSON.stringify({ key: 'obj', versionId: 'phd', isPHD: true });
            assert.strictEqual(filter.isShadowed({ ...masterDelete, value: regularPhd }), true);
        });

        it('should keep a placeholder delete whose other fields differ', () => {
            filter.isShadowed(versionDelete);
            const other = JSON.stringify({ key: 'obj', versionId: 'phd', isNull: true, isPHD: true, tags: { a: 'b' } });
            assert.strictEqual(filter.isShadowed({ ...masterDelete, value: other }), false);
        });

        it('should drop the placeholder delete even with other objects in between', () => {
            filter.isShadowed(versionDelete);
            filter.isShadowed({ ...master, key: 'obj2' });
            assert.strictEqual(filter.isShadowed(masterDelete), true);
        });
    });

    it('should ignore entries without a value or a key', () => {
        filter.isShadowed(version);
        assert.strictEqual(filter.isShadowed({ type: 'del', bucket: 'bucket', key: 'obj' }), false);
        assert.strictEqual(filter.isShadowed({ type: 'put', bucket: 'bucket', value }), false);
        assert.strictEqual(filter.isShadowed(master), true);
    });
});
