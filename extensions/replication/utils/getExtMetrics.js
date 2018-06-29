/**
 * Get the object form to send to the CRR metrics topic.
 * @param {String} site - The site of the destination object
 * @param {Number} bytes - The size of the metrics being set in Redis
 * @param {ObjectQueueEntry} entry - The object queue entry for the object being
 * monitored
 * @return {Object} - The object to send to the CRR metrics topic as a message
 */
function getExtMetrics(site, bytes, entry) {
    const extMetrics = {};
    extMetrics[site] = {
        bytes,
        bucketName: entry.getBucket(),
        objectKey: entry.getObjectKey(),
        versionId: entry.getEncodedVersionId(),
    };
    return extMetrics;
}

module.exports = getExtMetrics;
