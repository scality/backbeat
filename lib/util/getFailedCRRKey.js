const { redisKeys } = require('../../extensions/replication/constants');

/**
 * Returns the schema used for failed CRR entry Redis keys.
 * @param {String} bucket - The name of the bucket
 * @param {String} key - The name of the key
 * @param {String} versionId - The encoded version ID
 * @param {String} storageClass - The storage class of the object
 * @return {String} - The Redis key used for the failed CRR entry
 */
function getFailedCRRKey(bucket, key, versionId, storageClass) {
    const { failedCRR } = redisKeys;
    return `${failedCRR}:${bucket}:${key}:${versionId}:${storageClass}`;
}

module.exports = getFailedCRRKey;
