const querystring = require('querystring');

const { redisKeys } = require('../../extensions/replication/constants');

/**
 * Returns the schema used for failed CRR entry Redis sorted set member.
 * @param {String} bucket - The name of the bucket
 * @param {String} key - The name of the key
 * @param {String|undefined} [versionId] - The encoded version ID
 * @param {String|undefined} [role] - The source role used for CRR
 * @return {String} - The sorted set member used for the failed CRR entry
 */
function getSortedSetMember(bucket, key, versionId, role) {
    // If the original CRR was on a bucket without versioning enabled (i.e. an
    // NFS bucket), maintain the Redis key schema by using and empty string.
    const schemaVersionId = versionId === undefined ? '' : versionId;
    if (!role) {
        return `${bucket}:${key}:${schemaVersionId}`;
    }
    const schemaRole = querystring.escape(role);
    return `${bucket}:${key}:${schemaVersionId}:${schemaRole}`;
}

/**
 * Returns the sorted set key.
 * @param {String} storageClass - The storage class of the object
 * @param {Number} timestamp - The normalized timestamp
 * @return {String} - The sorted set key name
 */
function getSortedSetKey(storageClass, timestamp) {
    const { failedCRR } = redisKeys;
    return `${failedCRR}:${storageClass}:${timestamp}`;
}

module.exports = {
    getSortedSetMember,
    getSortedSetKey,
};
