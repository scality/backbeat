const crypto = require('crypto');

/**
 * Builds the delivery-topic record key for an addressed notification.
 * The key is stable across processes and reruns: same destination and
 * object always map to the same key, hence the same partition.
 *
 * @param {Object} destination - destination config entry
 * @param {String} bucket - bucket name
 * @param {String} objectKey - object key
 * @return {String} record key
 */
function buildDeliveryKey(destination, bucket, objectKey) {
    const m = destination.spreadFactor || 1;
    if (m <= 1) {
        return destination.resource;
    }
    const h = crypto.createHash('md5')
        .update(`${bucket}/${objectKey}`)
        .digest()
        .readUInt32BE(0);
    return `${destination.resource}|${h % m}`;
}

module.exports = { buildDeliveryKey };
