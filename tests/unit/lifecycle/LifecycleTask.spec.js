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

// get all rule ID's
function getRuleIDs(rules) {
    return rules.map(rule => rule.ID).sort();
}

describe('lifecycle task helper methods', () => {
    let lct;

    before(() => {
        const lp = {
            getStateVars: () => (
                {
                    enabledRules: {
                        expiration: { enabled: true },
                        transitions: { enabled: true },
                        noncurrentVersionTransitions: { enabled: true },
                        noncurrentVersionExpiration: { enabled: true },
                        abortIncompleteMultipartUpload: { enabled: true },
                    },
                }
            ),
        };
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
                new Rule().addID('task-1').addPrefix('atask').build(),
                new Rule().addID('task-2').addPrefix('atasker').build(),
                new Rule().addID('task-3').addPrefix('cat-').build(),
                new Rule().addID('task-4').addPrefix('xatask').build(),
                new Rule().addID('task-5').addPrefix('atasr').build(),
                new Rule().addID('task-6').addPrefix('Atask').build(),
                new Rule().addID('task-7').addPrefix('atAsk').build(),
                new Rule().addID('task-8').build(),
            ];
            const item1 = {
                Key: 'atasker-example-item',
                LastModified: CURRENT,
            };
            const item2 = {
                Key: 'cat-test',
                LastModified: CURRENT,
            };
            const objTags = { TagSet: [] };

            const res1 = lct._filterRules(mBucketRules, item1, objTags);
            assert.strictEqual(res1.length, 3);
            const expRes1 = getRuleIDs(mBucketRules.filter(rule =>
                (rule.Filter.Prefix && rule.Filter.Prefix.startsWith('atask'))
                || !rule.Filter.Prefix));
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
        const res = lct._filterRules(mBucketRules, item, objTags);
        assert.strictEqual(res.length, 1);
        assert.deepStrictEqual(getRuleIDs(res), ['task-9']);
    });

    // describe('_filterRules for listObjectVersions versions',
    // () => {
    //     it('should ')
    // });

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
    });
});
