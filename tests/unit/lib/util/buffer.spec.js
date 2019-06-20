const assert = require('assert');

const { readUInt64BE } = require('../../../../lib/util/buffer');

describe('readUInt64BE', () => {
    it('should parse a small long number in big endian', () => {
        const tsBuf = new Buffer([0, 0, 0, 0, 0, 0, 0, 3]);
        const value = readUInt64BE(tsBuf);
        assert.strictEqual(value, 3);
    });

    it('should parse a long number greater than 2**32 in big endian', () => {
        // taken from an actual timestamp value from a zookeeper node
        const tsBuf = new Buffer([0, 0, 1, 107, 118, 199, 180, 164]);
        const value = readUInt64BE(tsBuf);
        assert.strictEqual(value, 1561065927844);
    });
});
