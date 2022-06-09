const OplogPopulatorUtils = require('../../lib/util/OplogPopulatorUtils');

class NotificationOplogPopulatorUtils extends OplogPopulatorUtils {
    /**
     * Get extension specific MongoDB filter
     * to get buckets with that have the notification
     * extension enabled
     * @returns {Object} MongoDB filter
     */
     static getExtensionMongoDBFilter() {
        return {
            'value.notificationConfiguration': {
                $type: 3
            }
        };
    }

    /**
     * Check if bucket has the notification extension
     * active
     * @param {Object} bucketMD bucket metadata
     * @returns {boolean} true if the bucket has the
     * current extension enabled
     */
    static isBucketExtensionEnabled(bucketMD) {
        return Boolean(bucketMD.notificationConfiguration !== null);
    }
}
module.exports = NotificationOplogPopulatorUtils;
