const { redisKeys } = require('../../extensions/replication/constants');

/**
 * Returns the schema used for failed CRR entry Redis sorted set member.
 * @param {String} bucket - The name of the bucket
 * @param {String} key - The name of the key
 * @param {String|undefined} [versionId] - The encoded version ID
 * @return {String} - The sorted set member used for the failed CRR entry
 */
function getSortedSetMember(bucket, key, versionId) {
    // If the original CRR was on a bucket without versioning enabled (i.e. an
    // NFS bucket), maintain the Redis key schema by using an empty string.
    const schemaVersionId = versionId === undefined ? '' : versionId;
    return `${bucket}:${key}:${schemaVersionId}`;
}

/**
 * Returns the sorted set key.
 * @param {String} site - The site of the object
 * @param {Number} timestamp - The normalized timestamp
 * @return {String} - The sorted set key name
 */
function getSortedSetKey(site, timestamp) {
    const { failedCRR } = redisKeys;
    return `${failedCRR}:${site}:${timestamp}`;
}

/**
 * Gets the object-level CRR metrics bytes Redis key.
 * @param {String} site - The site of the CRR operation
 * @param {String} bucket - The bucket that the key belongs to
 * @param {String} key - The key
 * @param {String} versionId - The key's version ID
 * @return {String} - The Redis key name
 */
function getObjectBytesKey(site, bucket, key, versionId) {
    return `${site}:${bucket}:${key}:${versionId}:${redisKeys.objectBytes}`;
}

/**
 * Gets the object-level CRR metrics bytes done Redis key.
 * @param {String} site - The site of the CRR operation
 * @param {String} bucket - The bucket that the key belongs to
 * @param {String} key - The key
 * @param {String} versionId - The key's version ID
 * @return {String} - The Redis key name
 */
function getObjectBytesDoneKey(site, bucket, key, versionId) {
    return `${site}:${bucket}:${key}:${versionId}:${redisKeys.objectBytesDone}`;
}

module.exports = {
    getSortedSetMember,
    getSortedSetKey,
    getObjectBytesKey,
    getObjectBytesDoneKey,
};
