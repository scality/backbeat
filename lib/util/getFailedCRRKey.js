const { redisKeys } = require('../../extensions/replication/constants');

/**
 * Returns the schema used for failed CRR entry Redis keys.
 * @param {String} bucket - The name of the bucket
 * @param {String} key - The name of the key
 * @param {String|undefined} [versionId] - The encoded version ID
 * @param {String} storageClass - The storage class of the object
 * @return {String} - The Redis key used for the failed CRR entry
 */
function getFailedCRRKey(bucket, key, versionId, storageClass) {
    const { failedCRR } = redisKeys;
    // If the original CRR was on a bucket without versioning enabled (i.e. an
    // NFS bucket), maintain the Redis key schema by using and empty string.
    const schemaVersionId = versionId === undefined ? '' : versionId;
    return `${failedCRR}:${bucket}:${key}:${schemaVersionId}:${storageClass}`;
}

module.exports = getFailedCRRKey;
