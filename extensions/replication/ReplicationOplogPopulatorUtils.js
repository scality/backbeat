const OplogPopulatorUtils = require('../../lib/util/OplogPopulatorUtils');

class ReplicationOplogPopulatorUtils extends OplogPopulatorUtils {
    /**
     * Get extension specific MongoDB filter
     * to get buckets with that have the replication
     * extension enabled
     * @returns {Object} MongoDB filter
     */
     static getExtensionMongoDBFilter() {
        // getting buckets with at least one replication
        // rule enabled
        return {
            'value.replicationConfiguration.rules': {
                $elemMatch: {
                    enabled: true,
                },
            }
        };
    }

    /**
     * Check if bucket has the replication extension
     * active
     * @param {Object} bucketMD bucket metadata
     * @returns {boolean} true if the bucket has the
     * current extension enabled
     */
    static isBucketExtensionEnabled(bucketMD) {
        const rules = bucketMD.replicationConfiguration
            && bucketMD.replicationConfiguration.rules;
        if (!rules || rules.length === 0) {
            return false;
        } else {
            // return true if at least one replication
            // rule is enabled
            return rules.some(rule =>
                rule.enabled === true);
        }
    }
}
module.exports = ReplicationOplogPopulatorUtils;
