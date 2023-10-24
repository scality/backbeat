const OplogPopulatorUtils = require('../../lib/util/OplogPopulatorUtils');
const locations = require('../../conf/locationConfig.json');

class LifecycleOplogPopulatorUtils extends OplogPopulatorUtils {

    /**
     * Get extension specific MongoDB filter
     * to get buckets that have the lifecycle
     * extension enabled
     * @returns {Object} MongoDB filter
     */
     static getExtensionMongoDBFilter() {
        const coldLocations = Object.keys(locations).filter(loc => locations[loc].isCold);
        // getting buckets with at least one lifecycle transition rule enabled
        // or buckets with disabled lifecycle transition rules to cold locations (needed for restores)
        return {
            'value.lifecycleConfiguration.rules': {
                $elemMatch: {
                    $and: [
                        {
                            actions: {
                                $elemMatch: {
                                    actionName: {
                                        $in: ['Transition', 'NoncurrentVersionTransition'],
                                    },
                                },
                            },
                        }, {
                            $or: [
                                {
                                    ruleStatus: 'Enabled',
                                },
                                {
                                    actions: {
                                        $elemMatch: {
                                            transition: {
                                                $elemMatch: {
                                                    storageClass: {
                                                        $in: coldLocations,
                                                    },
                                                },
                                            },
                                        },
                                    },
                                },
                                {
                                    actions: {
                                        $elemMatch: {
                                            nonCurrentVersionTransition: {
                                                $elemMatch: {
                                                    storageClass: {
                                                        $in: coldLocations,
                                                    }
                                                },
                                            },
                                        },
                                    }
                                },
                            ],
                        },
                    ],
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
            // return true if at least one lifecycle transition rule is enabled
            // or if buckets have transition rules to cold locations (needed for restores)
            return rules.some(rule => {
                if (rule.ruleStatus === 'Enabled') {
                    return rule.actions.some(
                        action => ['Transition', 'NoncurrentVersionTransition']
                            .includes(action.actionName)
                    );
                }
                return rule.actions.some(action => {
                    if (action.actionName === 'Transition') {
                        return action.transition.some(
                            transition => locations[transition.storageClass]?.isCold
                        );
                    } else if (action.actionName === 'NoncurrentVersionTransition') {
                        return action.nonCurrentVersionTransition.some(
                            transition => locations[transition.storageClass]?.isCold
                        );
                    }
                    return false;
                });
            });
        }
    }
}
module.exports = LifecycleOplogPopulatorUtils;
