const { errors } = require('arsenal');

class OplogPopulatorUtils {
    /**
     * Get extension specific MongoDB filter
     * to get buckets with that have the current
     * extension enabled
     * @returns {Object} MongoDB filter
     */
    static getExtensionMongoDBFilter() {
        throw errors.NotImplemented;
    }

    /**
     * Check if bucket has the current extension
     * active
     * @param {Object} bucketMD bucket metadata
     * @returns {boolean} true if the bucket has the
     * current extension enabled
     */
    static isBucketExtensionEnabled(bucketMD) { // eslint-disable-line no-unused-vars
        throw errors.NotImplemented;
    }
}

module.exports = OplogPopulatorUtils;
