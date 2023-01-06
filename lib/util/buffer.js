/**
 * Convert buffer contents as a long integer in big endian format into
 * a Number
 *
 * Note that the function does not check for overflow if buf contains
 * a number greater than 2**53.
 *
 * @param {Buffer} buf - input buffer
 * @return {Number} - input buffer converted to a plain javascript Number
 */
function readUInt64BE(buf) {
    // node 8.x Buffer does not have readBigUInt64BE() function, so we
    // parse the upper half and bottom half separately, then join them
    // into a single Number.
    const msb = buf.readUInt32BE(0);
    const lsb = buf.readUInt32BE(4);
    return msb * (2 ** 32) + lsb;
}

/**
 * calculate the size of a string in bytes
 * @param {string} str string
 * @returns {number} size of string in bytes
 */
function getStringSizeInBytes(str) {
    return Buffer.byteLength(str);
}

module.exports = {
    readUInt64BE,
    getStringSizeInBytes,
};
