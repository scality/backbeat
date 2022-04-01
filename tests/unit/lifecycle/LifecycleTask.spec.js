'use strict'; // eslint-disable-line

const assert = require('assert');

const LifecycleTask = require(
    '../../../extensions/lifecycle/tasks/LifecycleTask');
const fakeLogger = require('../../utils/fakeLogger');

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

describe('lifecycle task helper methods', () => {
    let lct;

    before(() => {
        lct = new LifecycleTask(lp);
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
            const expected = [...versions, ...dms].sort((a, b) => {
                if (a.VersionId < b.VersionId) return -1;
                if (a.VersionId > b.VersionId) return 1;
                return 0;
            });
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
            // overwrite _sendObjectAction to read entry sent
            class LifecycleTaskMock extends LifecycleTask {
                _sendObjectAction(entry, cb) {
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
                assert.strictEqual(latestEntry.getActionType(), 'deleteObject');
                assert.deepStrictEqual(
                    latestEntry.getAttribute('target'), expectedTarget);
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
                assert.strictEqual(latestEntry.getActionType(), 'deleteObject');
                assert.deepStrictEqual(
                    latestEntry.getAttribute('target'), expectedTarget);
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
                assert.strictEqual(latestEntry.getActionType(), 'deleteObject');
                assert.deepStrictEqual(
                    latestEntry.getAttribute('target'), expectedTarget);
            });
        });
    });

    describe('_rulesHaveNoTag', () => {
        it('should return true if rule has a prefix and tag', () => {
            const rules = [
                {
                    ID: 'test-id',
                    Status: 'Enabled',
                    Filter: {
                        And: {
                            Prefix: 'prefix',
                            Tags: [
                                { Key: 'key', Value: 'val' },
                            ],
                        },
                    },
                    Expiration: { Days: 1 },
                },
            ];
            const result = lct._rulesHaveNoTag(rules);
            assert.equal(result, true);
        });

        it('should return true if rule has a prefix and multiple tags', () => {
            const rules = [
                {
                    ID: 'test-id',
                    Status: 'Enabled',
                    Filter: {
                        And: {
                            Prefix: 'prefix',
                            Tags: [
                                { Key: 'key', Value: 'val' },
                                { Key: 'key2', Value: 'val2' },
                            ],
                        },
                    },
                    Expiration: { Days: 1 },
                },
            ];
            const result = lct._rulesHaveNoTag(rules);
            assert.equal(result, true);
        });

        it('should return true if rule has a tag', () => {
            const rules = [
                {
                    ID: 'test-id',
                    Status: 'Enabled',
                    Filter: {
                        Tag: [
                            { Key: 'key', Value: 'val' },
                        ],

                    },
                    Expiration: { Days: 1 },
                },
            ];
            const result = lct._rulesHaveNoTag(rules);
            assert.equal(result, true);
        });

        it('should return true if rule has multiple tags', () => {
            const rules = [
                {
                    ID: 'test-id',
                    Status: 'Enabled',
                    Filter: {
                        Tag: [
                            { Key: 'key', Value: 'val' },
                            { Key: 'key2', Value: 'val2' },
                        ],

                    },
                    Expiration: { Days: 1 },
                },
            ];
            const result = lct._rulesHaveNoTag(rules);
            assert.equal(result, true);
        });
    });
});
