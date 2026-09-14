/**
 * Convert buffer contents as a long integer in big endian format into
 * a Number
 *
 * Note that the function does not check for overflow if buf contains
 * a number greater than 2**53.
 */
function readUInt64BE(buf: Buffer): number {
    // readBigUInt64BE() hands back a BigInt, so the upper and bottom halves are
    // parsed separately and joined into a single Number.
    const msb = buf.readUInt32BE(0);
    const lsb = buf.readUInt32BE(4);
    return msb * (2 ** 32) + lsb;
}

/**
 * calculate the size of a string in bytes
 */
function getStringSizeInBytes(str: string): number {
    return Buffer.byteLength(str);
}

export = {
    readUInt64BE,
    getStringSizeInBytes,
};
