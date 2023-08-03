const assert = require('assert');
const LifecycleOplogPopulatorUtils = require('../../../extensions/lifecycle/LifecycleOplogPopulatorUtils');

const currentTransitionRule = {
    actionName: 'Transition',
    transition: [
        {
            days: 0,
            storageClass: 'dmf',
        },
    ],
};

const nonCurrentTransitionRule = {
    actionName: 'NoncurrentVersionTransition',
    nonCurrentVersionTransition: [
        {
            noncurrentDays: 0,
            storageClass: 'dmf',
        },
    ],
};

const currentExpirationRule = {
    actionName: 'Expiration',
    days: 7,
};

const nonCurrentExpirationRule = {
    actionName: 'NoncurrentVersionExpiration',
    days: 7,
};

const abortMpuRule = {
    actionName: 'AbortIncompleteMultipartUpload',
    days: 7,
};

const removeDeleteMarkersRule = {
    actionName: 'Expiration',
    deleteMarker: 'true',
};

function buildLifecycleRule(actions, enabled = true) {
    return {
        ruleID: 'b44cb303-b8b2-4161-b886-5dd392b0e7c2',
        ruleStatus: enabled ? 'Enabled' : 'Disabled',
        actions: [...actions],
    };
}

describe('LifecycleOplogPopulatorUtils', () => {
    describe('isBucketExtensionEnabled', () => {
        [
            {
                it: 'should return false when there are only expiration rules',
                bucketMd: {
                    lifecycleConfiguration: {
                        rules: [
                            buildLifecycleRule([
                                currentExpirationRule,
                                nonCurrentExpirationRule,
                                abortMpuRule,
                                removeDeleteMarkersRule,
                            ]),
                        ],
                    },
                },
                expectedReturn: false,
            },
            {
                it: 'should return true when there is at least one transition rule (current)',
                bucketMd: {
                    lifecycleConfiguration: {
                        rules: [
                            buildLifecycleRule([
                                currentTransitionRule,
                            ]),
                            buildLifecycleRule([
                                nonCurrentTransitionRule,
                            ], false),
                            buildLifecycleRule([
                                currentExpirationRule,
                                nonCurrentExpirationRule,
                                abortMpuRule,
                                removeDeleteMarkersRule,
                            ]),
                        ],
                    },
                },
                expectedReturn: true,
            },
            {
                it: 'should return true when there is at least one transition rule (non current)',
                bucketMd: {
                    lifecycleConfiguration: {
                        rules: [
                            buildLifecycleRule([
                                nonCurrentTransitionRule,
                            ]),
                            buildLifecycleRule([
                                currentTransitionRule,
                            ], false),
                            buildLifecycleRule([
                                currentExpirationRule,
                                nonCurrentExpirationRule,
                                abortMpuRule,
                                removeDeleteMarkersRule,
                            ]),
                        ],
                    },
                },
                expectedReturn: true,
            },
            {
                it: 'should return true when having multiple transition rules',
                bucketMd: {
                    lifecycleConfiguration: {
                        rules: [
                            buildLifecycleRule([
                                currentTransitionRule,
                                nonCurrentTransitionRule,
                            ]),
                            buildLifecycleRule([
                                currentExpirationRule,
                                nonCurrentExpirationRule,
                                abortMpuRule,
                                removeDeleteMarkersRule,
                            ]),
                        ],
                    },
                },
                expectedReturn: true,
            },
            {
                it: 'should return false when transition rules are disabled',
                bucketMd: {
                    lifecycleConfiguration: {
                        rules: [
                            buildLifecycleRule([
                                currentTransitionRule,
                                nonCurrentTransitionRule,
                            ], false),
                            buildLifecycleRule([
                                currentExpirationRule,
                                nonCurrentExpirationRule,
                                abortMpuRule,
                                removeDeleteMarkersRule,
                            ]),
                        ],
                    },
                },
                expectedReturn: false,
            },
        ].forEach(params => {
            it(params.it, () => {
                const enabled = LifecycleOplogPopulatorUtils.isBucketExtensionEnabled(params.bucketMd);
                assert.strictEqual(enabled, params.expectedReturn);
            });
        });
    });
});
