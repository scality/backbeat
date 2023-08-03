const OplogPopulatorUtils = require('../../lib/util/OplogPopulatorUtils');

class LifecycleOplogPopulatorUtils extends OplogPopulatorUtils {
    /**
     * Get extension specific MongoDB filter
     * to get buckets that have the lifecycle
     * extension enabled
     * @returns {Object} MongoDB filter
     */
     static getExtensionMongoDBFilter() {
        // getting buckets with at least one lifecycle
        // rule enabled
        return {
            'value.lifecycleConfiguration.rules': {
                $elemMatch: {
                    ruleStatus: 'Enabled',
                    actions: {
                        $elemMatch: {
                            $or: [
                                { actionName: 'Transition' },
                                { actionName: 'NoncurrentVersionTransition' },
                            ],
                        },
                    },
                },
            }
        };
    }

    /**
     * Check if bucket has the lifecycle extension
     * active
     * @param {Object} bucketMD bucket metadata
     * @returns {boolean} true if the bucket has the
     * current extension enabled
     */
    static isBucketExtensionEnabled(bucketMD) {
        const rules = bucketMD.lifecycleConfiguration
            && bucketMD.lifecycleConfiguration.rules;
        if (!rules || rules.length === 0) {
            return false;
        } else {
            // return true if at least one lifecycle
            // rule is enabled
            return rules.some(rule =>
                rule.ruleStatus === 'Enabled' &&
                rule.actions.some(
                    action => ['Transition', 'NoncurrentVersionTransition'].includes(action.actionName)
                )
            );
        }
    }
}
module.exports = LifecycleOplogPopulatorUtils;
