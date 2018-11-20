'use strict'; // eslint-disable-line

const assert = require('assert');

const LifecycleTask = require(
    '../../../extensions/lifecycle/tasks/LifecycleTask');
const Rule = require('../../utils/Rule');

// 5 days prior to CURRENT
const PAST = new Date(2018, 1, 5);
const CURRENT = new Date(2018, 1, 10);
// 5 days after CURRENT
const FUTURE = new Date(2018, 1, 15);

const fakeLogger = {
    trace: () => {},
    error: () => {},
    info: () => {},
    debug: () => {},
};

// LifecycleBucketProcessor mini Mock
const lp = {
    getStateVars: () => (
        {
            enabledRules: {
                expiration: { enabled: true },
                transitions: { enabled: true },
                noncurrentVersionExpiration: { enabled: true },
                abortIncompleteMultipartUpload: { enabled: true },
            },
        }
    ),
};

// Get the date from the number of days given.
function getDate(params) {
    const numberOfDaysFromNow = params.numberOfDaysFromNow || 0;
    const oneDay = 24 * 60 * 60 * 1000; // Milliseconds in a day.
    const milliseconds = numberOfDaysFromNow * oneDay;
    const timestamp = Date.now() + milliseconds;
    return new Date(timestamp);
}

// Get the metadata object.
function getMetadataObject(lastModified, storageClass) {
    return {
        LastModified: lastModified,
        StorageClass: storageClass || 'STANDARD',
    };
}

// get all rule ID's
function getRuleIDs(rules) {
    return rules.map(rule => rule.ID).sort();
}

describe('lifecycle task helper methods', () => {
    let lct;

    before(() => {
        lct = new LifecycleTask(lp);
    });

    describe('_filterRules for listObjects contents',
    () => {
        it('should filter out Status disabled rules', () => {
            const mBucketRules = [
                new Rule().addID('task-1').build(),
                new Rule().addID('task-2').disable().build(),
                new Rule().addID('task-3').build(),
                new Rule().addID('task-4').build(),
                new Rule().addID('task-2').disable().build(),
            ];
            const item = {
                Key: 'example-item',
                LastModified: PAST,
            };
            const objTags = { TagSet: [] };

            const res = lct._filterRules(mBucketRules, item, objTags);
            const expected = mBucketRules.filter(rule =>
                rule.Status === 'Enabled');
            assert.deepStrictEqual(getRuleIDs(res), getRuleIDs(expected));
        });

        it('should filter out unmatched prefixes', () => {
            const mBucketRules = [
                new Rule().addID('task-1').addPrefix('atask/').build(),
                new Rule().addID('task-2').addPrefix('atasker/').build(),
                new Rule().addID('task-3').addPrefix('cat-').build(),
                new Rule().addID('task-4').addPrefix('xatask/').build(),
                new Rule().addID('task-5').addPrefix('atask').build(),
                new Rule().addID('task-6').addPrefix('Atask/').build(),
                new Rule().addID('task-7').addPrefix('atAsk/').build(),
                new Rule().addID('task-8').build(),
            ];
            const item1 = {
                Key: 'atask/example-item',
                LastModified: CURRENT,
            };
            const item2 = {
                Key: 'cat-test',
                LastModified: CURRENT,
            };
            const objTags = { TagSet: [] };

            const res1 = lct._filterRules(mBucketRules, item1, objTags);
            assert.strictEqual(res1.length, 3);
            const expRes1 = getRuleIDs(mBucketRules.filter(rule => {
                if (!rule.Filter.Prefix) {
                    return true;
                }
                if (item1.Key.startsWith(rule.Filter.Prefix)) {
                    return true;
                }
                return false;
            }));
            assert.deepStrictEqual(expRes1, getRuleIDs(res1));

            const res2 = lct._filterRules(mBucketRules, item2, objTags);
            assert.strictEqual(res2.length, 2);
            const expRes2 = getRuleIDs(mBucketRules.filter(rule =>
                (rule.Filter.Prefix && rule.Filter.Prefix.startsWith('cat-'))
                || !rule.Filter.Prefix));
            assert.deepStrictEqual(expRes2, getRuleIDs(res2));
        });

        it('should filter out unmatched single tags', () => {
            const mBucketRules = [
                new Rule().addID('task-1').addTag('tag1', 'val1').build(),
                new Rule().addID('task-2').addTag('tag3-1', 'val3')
                    .addTag('tag3-2', 'val3').build(),
                new Rule().addID('task-3').addTag('tag3-1', 'val3').build(),
                new Rule().addID('task-4').addTag('tag1', 'val1').build(),
                new Rule().addID('task-5').addTag('tag3-2', 'val3')
                    .addTag('tag3-1', 'false').build(),
                new Rule().addID('task-6').addTag('tag3-2', 'val3')
                    .addTag('tag3-1', 'val3').build(),
            ];
            const item = {
                Key: 'example-item',
                LastModified: CURRENT,
            };
            const objTags1 = { TagSet: [{ Key: 'tag1', Value: 'val1' }] };
            const res1 = lct._filterRules(mBucketRules, item, objTags1);
            assert.strictEqual(res1.length, 2);
            const expRes1 = getRuleIDs(mBucketRules.filter(rule =>
                (rule.Filter && rule.Filter.Tag &&
                rule.Filter.Tag.Key === 'tag1' &&
                rule.Filter.Tag.Value === 'val1')
            ));
            assert.deepStrictEqual(expRes1, getRuleIDs(res1));

            const objTags2 = { TagSet: [{ Key: 'tag3-1', Value: 'val3' }] };
            const res2 = lct._filterRules(mBucketRules, item, objTags2);
            assert.strictEqual(res2.length, 1);
            const expRes2 = getRuleIDs(mBucketRules.filter(rule =>
                rule.Filter && rule.Filter.Tag &&
                rule.Filter.Tag.Key === 'tag3-1' &&
                rule.Filter.Tag.Value === 'val3'
            ));
            assert.deepStrictEqual(expRes2, getRuleIDs(res2));
        });

        it('should filter out unmatched multiple tags', () => {
            const mBucketRules = [
                new Rule().addID('task-1').addTag('tag1', 'val1')
                    .addTag('tag2', 'val1').build(),
                new Rule().addID('task-2').addTag('tag1', 'val1').build(),
                new Rule().addID('task-3').addTag('tag2', 'val1').build(),
                new Rule().addID('task-4').addTag('tag2', 'false').build(),
                new Rule().addID('task-5').addTag('tag2', 'val1')
                    .addTag('tag1', 'false').build(),
                new Rule().addID('task-6').addTag('tag2', 'val1')
                    .addTag('tag1', 'val1').build(),
                new Rule().addID('task-7').addTag('tag2', 'val1')
                    .addTag('tag1', 'val1').addTag('tag3', 'val1').build(),
                new Rule().addID('task-8').addTag('tag2', 'val1')
                    .addTag('tag1', 'val1').addTag('tag3', 'false').build(),
                new Rule().addID('task-9').build(),
            ];
            const item = {
                Key: 'example-item',
                LastModified: CURRENT,
            };
            const objTags1 = { TagSet: [
                { Key: 'tag1', Value: 'val1' },
                { Key: 'tag2', Value: 'val1' },
            ] };
            const res1 = lct._filterRules(mBucketRules, item, objTags1);
            assert.strictEqual(res1.length, 5);
            assert.deepStrictEqual(getRuleIDs(res1), ['task-1', 'task-2',
                'task-3', 'task-6', 'task-9']);

            const objTags2 = { TagSet: [
                { Key: 'tag2', Value: 'val1' },
            ] };
            const res2 = lct._filterRules(mBucketRules, item, objTags2);
            assert.strictEqual(res2.length, 2);
            assert.deepStrictEqual(getRuleIDs(res2), ['task-3', 'task-9']);

            const objTags3 = { TagSet: [
                { Key: 'tag2', Value: 'val1' },
                { Key: 'tag1', Value: 'val1' },
                { Key: 'tag3', Value: 'val1' },
            ] };
            const res3 = lct._filterRules(mBucketRules, item, objTags3);
            assert.strictEqual(res3.length, 6);
            assert.deepStrictEqual(getRuleIDs(res3), ['task-1', 'task-2',
                'task-3', 'task-6', 'task-7', 'task-9']);
        });

        it('should filter correctly for an object with no tags', () => {
            const mBucketRules = [
                new Rule().addID('task-1').addTag('tag1', 'val1')
                    .addTag('tag2', 'val1').build(),
                new Rule().addID('task-2').addTag('tag1', 'val1').build(),
                new Rule().addID('task-3').addTag('tag2', 'val1').build(),
                new Rule().addID('task-4').addTag('tag2', 'false').build(),
                new Rule().addID('task-5').addTag('tag2', 'val1')
                    .addTag('tag1', 'false').build(),
                new Rule().addID('task-6').addTag('tag2', 'val1')
                    .addTag('tag1', 'val1').build(),
                new Rule().addID('task-7').addTag('tag2', 'val1')
                    .addTag('tag1', 'val1').addTag('tag3', 'val1').build(),
                new Rule().addID('task-8').addTag('tag2', 'val1')
                    .addTag('tag1', 'val1').addTag('tag3', 'false').build(),
                new Rule().addID('task-9').build(),
            ];
            const item = {
                Key: 'example-item',
                LastModified: CURRENT,
            };
            const objTags = { TagSet: [] };
            const objNoTagSet = {};
            [objTags, objNoTagSet].forEach(obj => {
                const res = lct._filterRules(mBucketRules, item, obj);
                assert.strictEqual(res.length, 1);
                assert.deepStrictEqual(getRuleIDs(res), ['task-9']);
            });
        });
    });

    describe('_getApplicableRules', () => {
        it('should return earliest applicable expirations', () => {
            const filteredRules = [
                new Rule().addID('task-1').addExpiration('Date', FUTURE)
                    .build(),
                new Rule().addID('task-2').addExpiration('Days', 10).build(),
                new Rule().addID('task-3').addExpiration('Date', PAST)
                    .build(),
                new Rule().addID('task-4').addExpiration('Date', CURRENT)
                    .build(),
                new Rule().addID('task-5').addExpiration('Days', 5).build(),
            ];

            const res1 = lct._getApplicableRules(filteredRules);
            assert.strictEqual(res1.Expiration.Date, PAST);
            assert.strictEqual(res1.Expiration.Days, 5);

            // remove `PAST` from rules
            filteredRules.splice(2, 1);
            const res2 = lct._getApplicableRules(filteredRules);
            assert.strictEqual(res2.Expiration.Date, CURRENT);
        });

        it('should return earliest applicable rules', () => {
            const filteredRules = [
                new Rule().addID('task-1').addExpiration('Date', FUTURE)
                    .build(),
                new Rule().addID('task-2').addAbortMPU(18).build(),
                new Rule().addID('task-3').addExpiration('Date', PAST)
                    .build(),
                new Rule().addID('task-4').addNCVExpiration(3).build(),
                new Rule().addID('task-5').addNCVExpiration(12).build(),
                new Rule().addID('task-6').addExpiration('Date', CURRENT)
                    .build(),
                new Rule().addID('task-7').addNCVExpiration(7).build(),
                new Rule().addID('task-8').addAbortMPU(4).build(),
                new Rule().addID('task-9').addAbortMPU(22).build(),
            ];

            const res = lct._getApplicableRules(filteredRules);
            assert.deepStrictEqual(Object.keys(res.Expiration), ['Date']);
            assert.deepStrictEqual(res.Expiration, { Date: PAST });
            assert.strictEqual(
                res.AbortIncompleteMultipartUpload.DaysAfterInitiation, 4);
            assert.strictEqual(
                res.NoncurrentVersionExpiration.NoncurrentDays, 3);
        });

        it('should return Transition with Days', () => {
            const applicableRules = [
                new Rule()
                    .addTransitions([
                        {
                            Days: 1,
                            StorageClass: 'zenko',
                        },
                    ])
                    .build(),
            ];
            const lastModified = getDate({ numberOfDaysFromNow: -2 });
            const object = getMetadataObject(lastModified);
            const rules = lct._getApplicableRules(applicableRules, object);
            assert.deepStrictEqual(rules, {
                Transition: {
                    Days: 1,
                    StorageClass: 'zenko',
                },
            });
        });

        it('should return Transition when multiple rule transitions', () => {
            const applicableRules = [
                new Rule()
                    .addTransitions([
                        {
                            Days: 1,
                            StorageClass: 'zenko-1',
                        },
                        {
                            Days: 3,
                            StorageClass: 'zenko-3',
                        },
                    ])
                    .build(),
            ];
            const lastModified = getDate({ numberOfDaysFromNow: -4 });
            const object = getMetadataObject(lastModified);
            const rules = lct._getApplicableRules(applicableRules, object);
            assert.deepStrictEqual(rules, {
                Transition: {
                    Days: 3,
                    StorageClass: 'zenko-3',
                },
            });
        });

        it('should return Transition with Date', () => {
            const applicableRules = [
                new Rule()
                    .addTransitions([{
                        Date: 0,
                        StorageClass: 'zenko',
                    }])
                    .build(),
            ];
            const lastModified = getDate({ numberOfDaysFromNow: -1 });
            const object = getMetadataObject(lastModified);
            const rules = lct._getApplicableRules(applicableRules, object);
            assert.deepStrictEqual(rules, {
                Transition: {
                    Date: 0,
                    StorageClass: 'zenko',
                },
            });
        });

        it('should return Transition across many rules: first rule', () => {
            const applicableRules = [
                new Rule()
                    .addTransitions([{
                        Days: 1,
                        StorageClass: 'zenko-1',
                    }])
                    .build(),
                new Rule()
                    .addTransitions([{
                        Days: 3,
                        StorageClass: 'zenko-3',
                    }])
                    .build(),
            ];
            const lastModified = getDate({ numberOfDaysFromNow: -2 });
            const object = getMetadataObject(lastModified);
            const rules = lct._getApplicableRules(applicableRules, object);
            assert.deepStrictEqual(rules, {
                Transition: {
                    Days: 1,
                    StorageClass: 'zenko-1',
                },
            });
        });

        it('should return Transition across many rules: second rule', () => {
            const applicableRules = [
                new Rule()
                    .addTransitions([{
                        Days: 1,
                        StorageClass: 'zenko-1',
                    }])
                    .build(),
                new Rule()
                    .addTransitions([{
                        Days: 3,
                        StorageClass: 'zenko-3',
                    }])
                    .build(),
            ];
            const lastModified = getDate({ numberOfDaysFromNow: -4 });
            const object = getMetadataObject(lastModified);
            const rules = lct._getApplicableRules(applicableRules, object);
            assert.deepStrictEqual(rules, {
                Transition: {
                    Days: 3,
                    StorageClass: 'zenko-3',
                },
            });
        });

        it('should return Transition across many rules: first rule with ' +
        'multiple transitions', () => {
            const applicableRules = [
                new Rule()
                    .addTransitions([{
                        Days: 1,
                        StorageClass: 'zenko-1',
                    }, {
                        Days: 3,
                        StorageClass: 'zenko-3',
                    }])
                    .build(),
                new Rule()
                    .addTransitions([{
                        Days: 4,
                        StorageClass: 'zenko-4',
                    }])
                    .build(),
            ];
            const lastModified = getDate({ numberOfDaysFromNow: -2 });
            const object = getMetadataObject(lastModified);
            const rules = lct._getApplicableRules(applicableRules, object);
            assert.deepStrictEqual(rules, {
                Transition: {
                    Days: 1,
                    StorageClass: 'zenko-1',
                },
            });
        });

        it('should return Transition across many rules: second rule with ' +
        'multiple transitions', () => {
            const applicableRules = [
                new Rule()
                    .addTransitions([{
                        Days: 1,
                        StorageClass: 'zenko-1',
                    }, {
                        Days: 3,
                        StorageClass: 'zenko-3',
                    }])
                    .build(),
                new Rule()
                    .addTransitions([{
                        Days: 4,
                        StorageClass: 'zenko-4',
                    }, {
                        Days: 6,
                        StorageClass: 'zenko-6',
                    }])
                    .build(),
            ];
            const lastModified = getDate({ numberOfDaysFromNow: -5 });
            const object = getMetadataObject(lastModified);
            const rules = lct._getApplicableRules(applicableRules, object);
            assert.deepStrictEqual(rules, {
                Transition: {
                    Days: 4,
                    StorageClass: 'zenko-4',
                },
            });
        });

        it('should return Transition across many rules: combination of Date ' +
        'and Days gets Date result', () => {
            const applicableDate = getDate({ numberOfDaysFromNow: -1 });
            const applicableRules = [
                new Rule()
                    .addTransitions([{
                        Days: 1,
                        StorageClass: 'zenko-1',
                    }])
                    .build(),
                new Rule()
                    .addTransitions([{
                        Date: applicableDate,
                        StorageClass: 'zenko-3',
                    }])
                    .build(),
            ];
            const lastModified = getDate({ numberOfDaysFromNow: -4 });
            const object = getMetadataObject(lastModified);
            const rules = lct._getApplicableRules(applicableRules, object);
            assert.deepStrictEqual(rules, {
                Transition: {
                    Date: applicableDate,
                    StorageClass: 'zenko-3',
                },
            });
        });

        it('should return Transition across many rules: combination of Date ' +
        'and Days gets Days result', () => {
            const applicableRules = [
                new Rule()
                    .addTransitions([{
                        Days: 3,
                        StorageClass: 'zenko-1',
                    }])
                    .build(),
                new Rule()
                    .addTransitions([{
                        Date: getDate({ numberOfDaysFromNow: -4 }),
                        StorageClass: 'zenko-3',
                    }])
                    .build(),
            ];
            const lastModified = getDate({ numberOfDaysFromNow: -4 });
            const object = getMetadataObject(lastModified);
            const rules = lct._getApplicableRules(applicableRules, object);
            assert.deepStrictEqual(rules, {
                Transition: {
                    Days: 3,
                    StorageClass: 'zenko-1',
                },
            });
        });

        it('should not return transition when Transitions has no applicable ' +
        'rule: Days', () => {
            const applicableRules = [
                new Rule()
                    .addTransitions([
                        {
                            Days: 3,
                            StorageClass: 'zenko',
                        },
                    ])
                    .build(),
            ];
            const lastModified = getDate({ numberOfDaysFromNow: -2 });
            const object = getMetadataObject(lastModified);
            const rules = lct._getApplicableRules(applicableRules, object);
            assert.strictEqual(rules.Transition, undefined);
        });

        it('should not return transition when Transitions has no applicable ' +
        'rule: Date', () => {
            const applicableRules = [
                new Rule()
                    .addTransitions([{
                        Date: new Date(getDate({ numberOfDaysFromNow: 1 })),
                        StorageClass: 'zenko',
                    }])
                    .build(),
            ];
            const lastModified = getDate({ numberOfDaysFromNow: 0 });
            const object = getMetadataObject(lastModified);
            const rules = lct._getApplicableRules(applicableRules, object);
            assert.strictEqual(rules.Transition, undefined);
        });

        it('should not return transition when Transitions is an empty ' +
        'array', () => {
            const applicableRules = [
                new Rule()
                    .addTransitions([])
                    .build(),
            ];
            const rules = lct._getApplicableRules(applicableRules, {});
            assert.strictEqual(rules.Transition, undefined);
        });

        it('should not return transition when Transitions is undefined', () => {
            const applicableRules = [
                new Rule()
                    .addExpiration('Days', 1)
                    .build(),
            ];
            const rules = lct._getApplicableRules(applicableRules, {});
            assert.strictEqual(rules.Transition, undefined);
        });

        describe('transitioning to the same storage class', () => {
            it('should not return transition when applicable transition is ' +
            'already stored at the destination', () => {
                const applicableRules = [
                    new Rule()
                        .addTransitions([
                            {
                                Days: 1,
                                StorageClass: 'zenko',
                            },
                        ])
                        .build(),
                ];
                const lastModified = getDate({ numberOfDaysFromNow: -2 });
                const object = getMetadataObject(lastModified, 'zenko');
                const rules = lct._getApplicableRules(applicableRules, object);
                assert.strictEqual(rules.Transition, undefined);
            });

            it('should not return transition when applicable transition is ' +
            'already stored at the destination: multiple rules', () => {
                const applicableRules = [
                    new Rule()
                        .addTransitions([
                            {
                                Days: 2,
                                StorageClass: 'zenko',
                            },
                        ])
                        .build(),
                    new Rule()
                        .addTransitions([
                            {
                                Days: 1,
                                StorageClass: 'STANDARD',
                            },
                        ])
                        .build(),
                ];
                const lastModified = getDate({ numberOfDaysFromNow: -3 });
                const object = getMetadataObject(lastModified, 'zenko');
                const rules = lct._getApplicableRules(applicableRules, object);
                assert.strictEqual(rules.Transition, undefined);
            });
        });
    });

    describe('_getApplicableTransition', () => {
        describe('using Days time type', () => {
            it('should return undefined if no rules given', () => {
                const result = lct._getApplicableTransition({
                    transitions: [],
                    currentDate: '1970-01-03T00:00:00.000Z',
                    lastModified: '1970-01-01T00:00:00.000Z',
                    store: {},
                });
                assert.deepStrictEqual(result, undefined);
            });

            it('should return undefined when no rule applies', () => {
                const result = lct._getApplicableTransition({
                    transitions: [
                        {
                            Days: 1,
                            StorageClass: 'zenko',
                        },
                    ],
                    currentDate: '1970-01-01T23:59:59.999Z',
                    lastModified: '1970-01-01T00:00:00.000Z',
                    store: {},
                });
                assert.deepStrictEqual(result, undefined);
            });

            it('should return a single rule if it applies', () => {
                const result = lct._getApplicableTransition({
                    transitions: [
                        {
                            Days: 1,
                            StorageClass: 'zenko',
                        },
                    ],
                    currentDate: '1970-01-02T00:00:00.000Z',
                    lastModified: '1970-01-01T00:00:00.000Z',
                    store: {},
                });
                const expected = {
                    Days: 1,
                    StorageClass: 'zenko',
                };
                assert.deepStrictEqual(result, expected);
            });

            it('should return the most applicable rule: last rule', () => {
                const result = lct._getApplicableTransition({
                    transitions: [
                        {
                            Days: 1,
                            StorageClass: 'zenko',
                        },
                        {
                            Days: 10,
                            StorageClass: 'zenko',
                        },
                    ],
                    currentDate: '1970-01-11T00:00:00.000Z',
                    lastModified: '1970-01-01T00:00:00.000Z',
                    store: {},
                });
                const expected = {
                    Days: 10,
                    StorageClass: 'zenko',
                };
                assert.deepStrictEqual(result, expected);
            });

            it('should return the most applicable rule: middle rule', () => {
                const result = lct._getApplicableTransition({
                    transitions: [
                        {
                            Days: 1,
                            StorageClass: 'zenko',
                        },
                        {
                            Days: 4,
                            StorageClass: 'zenko',
                        },
                        {
                            Days: 10,
                            StorageClass: 'zenko',
                        },
                    ],
                    currentDate: '1970-01-05T00:00:00.000Z',
                    lastModified: '1970-01-01T00:00:00.000Z',
                    store: {},
                });
                const expected = {
                    Days: 4,
                    StorageClass: 'zenko',
                };
                assert.deepStrictEqual(result, expected);
            });
        });

        describe('using Date time type', () => {
            it('should return undefined if no rules given', () => {
                const result = lct._getApplicableTransition({
                    transitions: [],
                    currentDate: '1970-01-03T00:00:00.000Z',
                    lastModified: '1970-01-01T00:00:00.000Z',
                    store: {},
                });
                assert.deepStrictEqual(result, undefined);
            });

            it('should return undefined when no rule applies', () => {
                const result = lct._getApplicableTransition({
                    transitions: [
                        {
                            Date: '1970-01-02T00:00:00.000Z',
                            StorageClass: 'zenko',
                        },
                    ],
                    currentDate: '1970-01-01T23:59:59.999Z',
                    lastModified: '1970-01-01T00:00:00.000Z',
                    store: {},
                });
                assert.deepStrictEqual(result, undefined);
            });

            it('should return a single rule if it applies', () => {
                const result = lct._getApplicableTransition({
                    transitions: [
                        {
                            Date: '1970-01-02T00:00:00.000Z',
                            StorageClass: 'zenko',
                        },
                    ],
                    currentDate: '1970-01-02T00:00:00.000Z',
                    lastModified: '1970-01-01T00:00:00.000Z',
                    store: {},
                });
                const expected = {
                    Date: '1970-01-02T00:00:00.000Z',
                    StorageClass: 'zenko',
                };
                assert.deepStrictEqual(result, expected);
            });

            it('should return the most applicable rule', () => {
                const result = lct._getApplicableTransition({
                    transitions: [
                        {
                            Date: '1970-01-02T00:00:00.000Z',
                            StorageClass: 'zenko',
                        },
                        {
                            Date: '1970-01-10T00:00:00.000Z',
                            StorageClass: 'zenko',
                        },
                    ],
                    currentDate: '1970-01-11T00:00:00.000Z',
                    lastModified: '1970-01-01T00:00:00.000Z',
                    store: {},
                });
                const expected = {
                    Date: '1970-01-10T00:00:00.000Z',
                    StorageClass: 'zenko',
                };
                assert.deepStrictEqual(result, expected);
            });
        });
    });

    describe('_compareTransitions', () => {
        it('should return undefined if no rules given', () => {
            const result = lct._compareTransitions({ });
            assert.strictEqual(result, undefined);
        });

        it('should return first rule if second rule is not given', () => {
            const transition1 = {
                Days: 1,
                StorageClass: 'zenko',
            };
            const result = lct._compareTransitions({ transition1 });
            assert.deepStrictEqual(result, transition1);
        });

        it('should return second rule if first rule is not given', () => {
            const transition2 = {
                Days: 1,
                StorageClass: 'zenko',
            };
            const result = lct._compareTransitions({ transition2 });
            assert.deepStrictEqual(result, transition2);
        });

        it('should return the first rule if older than the second rule', () => {
            const transition1 = {
                Days: 2,
                StorageClass: 'zenko',
            };
            const transition2 = {
                Days: 1,
                StorageClass: 'zenko',
            };
            const result = lct._compareTransitions({
                transition1,
                transition2,
                lastModified: '1970-01-01T00:00:00.000Z',
            });
            assert.deepStrictEqual(result, transition1);
        });

        it('should return the second rule if older than the first rule', () => {
            const transition1 = {
                Days: 1,
                StorageClass: 'zenko',
            };
            const transition2 = {
                Days: 2,
                StorageClass: 'zenko',
            };
            const result = lct._compareTransitions({
                transition1,
                transition2,
                lastModified: '1970-01-01T00:00:00.000Z',
            });
            assert.deepStrictEqual(result, transition2);
        });
    });

    describe('_mergeSortedVersionsAndDeleteMarkers', () => {
        it('should merge and sort arrays based on Key names and then by ' +
        'LastModified times', () => {
            // Both arrays should be sorted respective to their own arrays.
            // This method should stable sort and merge both arrays
            const versions = [
                {
                    Key: 'obj-1',
                    VersionId:
                    '834373636323233323831333639393939393952473030312020363030',
                    IsLatest: true,
                    LastModified: '2018-04-04T23:16:46.000Z',
                },
                // LastModified matches with a delete marker
                {
                    Key: 'obj-1',
                    VersionId:
                    '834373636323233323831333639393939393952473030312020363035',
                    IsLatest: false,
                    LastModified: '2018-04-04T23:16:44.000Z',
                },
                {
                    Key: 'obj-1',
                    VersionId:
                    '834373636323233323831333639393939393952473030312020363040',
                    IsLatest: false,
                    LastModified: '2018-04-04T23:16:41.000Z',
                },
                {
                    Key: 'obj-1',
                    VersionId:
                    '834373636323233323831333639393939393952473030312020363045',
                    IsLatest: false,
                    LastModified: '2018-04-04T23:16:32.000Z',
                },
            ];
            const dms = [
                {
                    Key: 'obj-1',
                    IsLatest: false,
                    VersionId:
                    '834373636323233323831333639393939393952473030312020363036',
                    LastModified: '2018-04-04T23:16:44.000Z',
                },
                {
                    Key: 'obj-1',
                    IsLatest: false,
                    VersionId:
                    '834373636323233323831333639393939393952473030312020363042',
                    LastModified: '2018-04-04T23:16:34.000Z',
                },
            ];

            // Can only do this since I set VersionId in the expected order
            // Normally, when dealing with multiple objects, we wouldn't be able
            // to sort by just VersionId, since they could be intertwined.
            const expected = [...versions, ...dms].sort((a, b) => (
                a.VersionId > b.VersionId
            ));
            const res = lct._mergeSortedVersionsAndDeleteMarkers(versions, dms);

            assert.deepStrictEqual(expected, res);
        });

        it('should merge and sort arrays that include a null version id ' +
        'version', () => {
            const versions = [
                {
                    Key: 'obj-1',
                    VersionId:
                    '834373636323233323831343639393939393952473030312020363030',
                    IsLatest: true,
                    LastModified: '2018-04-04T23:16:47.000Z',
                },
                {
                    Key: 'obj-1',
                    VersionId: 'null',
                    IsLatest: false,
                    LastModified: '2018-04-04T23:16:46.000Z',
                },
            ];
            const dms = [
                {
                    Key: 'obj-1',
                    VersionId:
                    '834373636323233323831343639393939393952473030312020363035',
                    IsLatest: false,
                    LastModified: '2018-04-04T23:16:47.000Z',
                },
                {
                    Key: 'obj-1',
                    VersionId:
                    '834373636323233323831343639393939393952473030312020363040',
                    IsLatest: false,
                    LastModified: '2018-04-04T23:16:40.000Z',
                },
            ];

            const expected = [versions[0], dms[0], versions[1], dms[1]];
            const res = lct._mergeSortedVersionsAndDeleteMarkers(versions, dms);

            assert.deepStrictEqual(expected, res);
        });
    });

    describe('_addStaleDateToVersions', () => {
        const list = [
            {
                Key: 'obj-1',
                VersionId:
                '834373731313631393339313839393939393952473030312020353833',
                IsLatest: true,
                LastModified: '2018-04-04T23:16:46.000Z',
            },
            {
                Key: 'obj-1',
                VersionId:
                '834373731313631393339313839393939393952473030312020353830',
                IsLatest: false,
                LastModified: '2018-04-04T23:16:44.000Z',
            },
            {
                Key: 'obj-1',
                VersionId:
                '834373731313631393339313839393939393952473030312020353827',
                IsLatest: false,
                LastModified: '2018-04-04T23:16:41.000Z',
            },
            {
                Key: 'obj-1',
                VersionId:
                '834373731313631393339313839393939393952473030312020353823',
                IsLatest: false,
                LastModified: '2018-04-04T23:16:32.000Z',
            },
            // intertwine version id's with second object
            {
                Key: 'obj-2',
                IsLatest: true,
                VersionId:
                '834373731313631393339313839393939393952473030312020353832',
                LastModified: '2018-04-04T23:16:44.000Z',
            },
            {
                Key: 'obj-2',
                VersionId:
                '834373731313631393339313839393939393952473030312020353831',
                IsLatest: false,
                LastModified: '2018-04-04T23:16:44.000Z',
            },
            {
                Key: 'obj-2',
                IsLatest: false,
                VersionId:
                '834373731313631393339313839393939393952473030312020353825',
                LastModified: '2018-04-04T23:16:34.000Z',
            },
            {
                Key: 'obj-2',
                VersionId:
                '834373731313631393339313839393939393952473030312020353824',
                IsLatest: false,
                LastModified: '2018-04-04T23:16:19.000Z',
            },
        ];

        it('should apply a staleDate property on each version on a list ' +
        'of versions', () => {
            const dupelist = list.map(i => Object.assign({}, i));
            const bucketDetails = {};
            const res = lct._addStaleDateToVersions(bucketDetails, dupelist);

            assert(res.every(v => 'staleDate' in v));
            for (let i = 0; i < res.length - 1; i++) {
                if (res[i + 1].IsLatest) {
                    assert.equal(res[i + 1].staleDate, undefined);
                } else {
                    assert.equal(res[i].LastModified, res[i + 1].staleDate);
                }
            }
        });

        it('should use bucket details if applies', () => {
            const dupelist = list.map(i => Object.assign({}, i));

            // override existing `IsLatest`
            dupelist[0].IsLatest = false;

            const bucketDetails = {
                keyMarker: 'obj-1',
                prevDate: '2018-04-04T23:16:55.000Z',
            };
            const res = lct._addStaleDateToVersions(bucketDetails, dupelist);

            assert.equal(res[0].staleDate, '2018-04-04T23:16:55.000Z');
        });
    });

    describe('_checkAndApplyEODMRule', () => {
        let lct2;

        const bucketData = {
            target: {
                owner: 'test-user',
                bucket: 'test-bucket',
            },
        };
        // lifecycle service account created delete marker
        const deleteMarkerLC = {
            Owner: {
                DisplayName: 'Lifecycle Service Account',
                ID: '86346e5bda4c2158985574c9942089c36ca650dc509/lifecycle',
            },
            Key: 'test-key',
            VersionId:
            '834373731313631393339313839393939393952473030312020353820',
        };
        // user created delete marker
        const deleteMarkerNotLC = {
            Owner: {
                DisplayName: 'Not Lifecycle Service Account',
                ID: '86346e5bda4c2158985574c9942089c36ca650dc509',
            },
            Key: 'test-key',
            VersionId:
            '834373731313631393339313839393939393952473030312020353820',
        };
        const listOfVersions = [
            {
                IsLatest: false,
                Key: 'another-test-key',
                VersionId:
                '834373731313631393339313839393939393952473030312020353815',
            },
        ];

        before(() => {
            // overwrite sendObjectEntry to read entry sent
            class LifecycleTaskMock extends LifecycleTask {
                sendObjectEntry(entry, cb) {
                    this.latestEntry = entry;
                    return cb();
                }

                getLatestEntry() {
                    return this.latestEntry;
                }

                reset() {
                    this.latestEntry = undefined;
                }
            }
            lct2 = new LifecycleTaskMock(lp);
        });

        afterEach(() => {
            lct2.reset();
        });

        it('should send an entry to Kafka when ExpiredObjectDeleteMarker is ' +
        'enabled and delete marker was created by the lifecycle service ' +
        'account', () => {
            const rules = {
                Expiration: { ExpiredObjectDeleteMarker: true },
            };

            lct2._checkAndApplyEODMRule(bucketData, deleteMarkerLC,
            listOfVersions, rules, fakeLogger, err => {
                assert.ifError(err);

                const latestEntry = lct2.getLatestEntry();

                const expectedTarget = Object.assign({}, bucketData.target, {
                    key: deleteMarkerLC.Key,
                    version: deleteMarkerLC.VersionId,
                });
                const expected = {
                    action: 'deleteObject',
                    target: expectedTarget,
                };
                assert.deepStrictEqual(latestEntry, expected);
            });
        });

        it('should send an entry to Kafka when the delete marker was created ' +
        'by the lifecycle service account and ExpiredObjectDeleteMarker rule ' +
        'was NOT explicitly set to false', () => {
            const rules = {};

            lct2._checkAndApplyEODMRule(bucketData, deleteMarkerLC,
            listOfVersions, rules, fakeLogger, err => {
                assert.ifError(err);

                const latestEntry = lct2.getLatestEntry();
                const expectedTarget = Object.assign({}, bucketData.target, {
                    key: deleteMarkerLC.Key,
                    version: deleteMarkerLC.VersionId,
                });
                const expected = {
                    action: 'deleteObject',
                    target: expectedTarget,
                };
                assert.deepStrictEqual(latestEntry, expected);
            });
        });

        it('should NOT send any entry to Kafka when Expiration rule is not ' +
        'enabled and the delete marker was not created by the lifecycle ' +
        'service account', () => {
            const rules = {};

            lct2._checkAndApplyEODMRule(bucketData, deleteMarkerNotLC,
            listOfVersions, rules, fakeLogger, err => {
                assert.ifError(err);

                const latestEntry = lct2.getLatestEntry();
                assert.equal(latestEntry, undefined);
            });
        });

        it('should NOT send an entry to Kafka when the delete marker was ' +
        'created by the lifecycle service account but ' +
        'ExpiredObjectDeleteMarker rule is explicitly set to false', () => {
            const rules = {
                Expiration: { ExpiredObjectDeleteMarker: false },
            };

            lct2._checkAndApplyEODMRule(bucketData, deleteMarkerLC,
            listOfVersions, rules, fakeLogger, err => {
                assert.ifError(err);

                const latestEntry = lct2.getLatestEntry();
                assert.equal(latestEntry, undefined);
            });
        });

        it('should send an entry to Kafka when ExpiredObjectDeleteMarker is ' +
        'enabled and delete marker was not created by the lifecycle service ' +
        'account', () => {
            const rules = {
                Expiration: { ExpiredObjectDeleteMarker: true },
            };

            lct2._checkAndApplyEODMRule(bucketData, deleteMarkerNotLC,
            listOfVersions, rules, fakeLogger, err => {
                assert.ifError(err);

                const latestEntry = lct2.getLatestEntry();

                const expectedTarget = Object.assign({}, bucketData.target, {
                    key: deleteMarkerNotLC.Key,
                    version: deleteMarkerNotLC.VersionId,
                });
                const expected = {
                    action: 'deleteObject',
                    target: expectedTarget,
                };
                assert.deepStrictEqual(latestEntry, expected);
            });
        });
    });
});
