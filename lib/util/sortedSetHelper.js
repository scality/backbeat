const { redisKeys } = require('../../extensions/replication/constants');

/**
 * Returns the schema used for failed CRR entry Redis sorted set member.
 * @param {String} bucket - The name of the bucket
 * @param {String} key - The name of the key
 * @param {String|undefined} [versionId] - The encoded version ID
 * @return {String} - The sorted set member used for the failed CRR entry
 */
function getSortedSetMember(bucket, key, versionId, role) {
    // If the original CRR was on a bucket without versioning enabled (i.e. an
    // NFS bucket), maintain the Redis key schema by using and empty string.
    const schemaVersionId = versionId === undefined ? '' : versionId;
    return `${bucket}:${key}:${schemaVersionId}:${role}`;
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
