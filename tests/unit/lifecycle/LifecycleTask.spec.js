'use strict'; // eslint-disable-line

const assert = require('assert');

const LifecycleTask = require(
    '../../../extensions/lifecycle/tasks/LifecycleTask');
const fakeLogger = require('../../utils/fakeLogger');

const HOUR = 1000 * 60 * 60;
const DAY = 24 * HOUR;

const OBJECT = {
    Key: 'key1',
    LastModified: '2018-03-30T22:22:34.384Z',
    ETag: '"d41d8cd98f00b204e9800998ecf8427e"',
    Size: 10,
    StorageClass: 'STANDARD',
    Owner: {
        DisplayName: 'bart',
        ID: '02055fe9b403138416f06fb1331e9aea9b4adb55f9b59157a5d2d881086afd5c',
    },
};

const LATEST_VERSION = {
    ETag: '"d41d8cd98f00b204e9800998ecf8427e"',
    Size: 0,
    StorageClass: 'STANDARD',
    Key: 'key2',
    VersionId: '39383334393935303033383637313939393939395247303031202031',
    IsLatest: true,
    LastModified: '2018-03-30T22:22:34.384Z',
    Owner: {
        DisplayName: 'bart',
        ID: '02055fe9b403138416f06fb1331e9aea9b4adb55f9b59157a5d2d881086afd5c',
    },
};

const NON_CURRENT_VERSION = {
    ETag: '"d41d8cd98f00b204e9800998ecf8427e"',
    Size: 0,
    StorageClass: 'STANDARD',
    Key: 'key2',
    VersionId: '39383334393935303033383637313939393939395247303031202031',
    IsLatest: false,
    LastModified: '2018-03-30T22:22:34.384Z',
    Owner: {
        DisplayName: 'bart',
        ID: '02055fe9b403138416f06fb1331e9aea9b4adb55f9b59157a5d2d881086afd5c',
    },
    staleDate: '2019-03-30T22:22:34.384Z',
};

const LATEST_DELETE_MARKER = {
    Owner: {
        DisplayName: 'bart',
        ID: '02055fe9b403138416f06fb1331e9aea9b4adb55f9b59157a5d2d881086afd5c',
    },
    Key: 'key1',
    VersionId: '39383334383931323137363631303939393939395247303031202033',
    IsLatest: true,
    LastModified: '2018-03-30T22:22:34.384Z',
};

const NON_CURRENT_DELETE_MARKER = {
    Owner: {
        DisplayName: 'bart',
        ID: '02055fe9b403138416f06fb1331e9aea9b4adb55f9b59157a5d2d881086afd5c',
    },
    Key: 'key1',
    VersionId: '39383334383931323137363631303939393939395247303031202033',
    IsLatest: false,
    LastModified: '2018-03-30T22:22:34.384Z',
    staleDate: '2019-03-30T22:22:34.384Z',
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

describe('lifecycle task helper methods', () => {
    let lct;

    before(() => {
        lct = new LifecycleTask(lp);
        lct.setSupportedRules([
            'expiration',
            'noncurrentVersionExpiration',
            'abortIncompleteMultipartUpload',
            'transitions',
            'noncurrentVersionTransitions',
        ]);
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

    describe('_rulesHaveTag', () => {
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
            const result = lct._rulesHaveTag(rules);
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
            const result = lct._rulesHaveTag(rules);
            assert.equal(result, true);
        });

        it('should return true if rule has a tag', () => {
            const rules = [
                {
                    ID: 'test-id',
                    Status: 'Enabled',
                    Filter: {
                        Tag: { Key: 'key', Value: 'val' },
                    },
                    Expiration: { Days: 1 },
                },
            ];
            const result = lct._rulesHaveTag(rules);
            assert.equal(result, true);
        });

        it('should return true if rule has multiple tags', () => {
            const rules = [
                {
                    ID: 'test-id',
                    Status: 'Enabled',
                    Filter: {
                        And: {
                            Tags: [
                                { Key: 'key1', Value: 'val' },
                                { Key: 'key2', Value: 'val' },
                            ],
                        },
                    },
                    Expiration: { Days: 1 },
                },
            ];
            const result = lct._rulesHaveTag(rules);
            assert.equal(result, true);
        });

        it('should return true if one of the rules has tags', () => {
            const rules = [
                {
                    ID: 'test-id',
                    Status: 'Enabled',
                    Prefix: '',
                    Expiration: { Days: 1 },
                },
                {
                    ID: 'test-id2',
                    Status: 'Enabled',
                    Filter: {
                        And: {
                            Tags: [
                                { Key: 'key1', Value: 'val' },
                                { Key: 'key2', Value: 'val' },
                            ],
                        },
                    },
                    Transitions: [{ Days: 0, StorageClass: 'us-east-2' }],
                },
            ];
            const result = lct._rulesHaveTag(rules);
            assert.equal(result, true);
        });

        it('should return true if both of the rules has tags', () => {
            const rules = [
                {
                    ID: 'test-id',
                    Status: 'Enabled',
                    Filter: {
                        And: {
                            Tags: [
                                { Key: 'key1', Value: 'val' },
                                { Key: 'key2', Value: 'val' },
                            ],
                        },
                    },
                    Expiration: { Days: 1 },
                },
                {
                    ID: 'test-id2',
                    Status: 'Enabled',
                    Filter: {
                        Tag: { Key: 'key', Value: 'val' },
                    },
                    Transitions: [{ Days: 0, StorageClass: 'us-east-2' }],
                },
            ];
            const result = lct._rulesHaveTag(rules);
            assert.equal(result, true);
        });

        it('should return false if none of the rules has tags', () => {
            const rules = [
                {
                    ID: 'test-id',
                    Status: 'Enabled',
                    Prefix: '',
                    Expiration: { Days: 1 },
                },
                {
                    ID: 'test-id2',
                    Status: 'Enabled',
                    Prefix: '',
                    Transitions: [{ Days: 0, StorageClass: 'us-east-2' }],
                },
            ];
            const result = lct._rulesHaveTag(rules);
            assert.equal(result, false);
        });

        it('should return false if rule has no tag', () => {
            const rules = [
                {
                    ID: 'test-id',
                    Status: 'Enabled',
                    Prefix: '',
                    Expiration: { Days: 1 },
                },
            ];
            const result = lct._rulesHaveTag(rules);
            assert.equal(result, false);
        });

        it('should return false if rule has prefix but no tag', () => {
            const rules = [
                {
                    ID: 'b6138dd9-8557-416f-b860-66d7156f57a3',
                    Status: 'Enabled',
                    Transitions: [{ Days: 0, StorageClass: 'us-east-2' }],
                    Filter: { Prefix: 'test/' },
                },
            ];
            const result = lct._rulesHaveTag(rules);
            assert.equal(result, false);
        });
    });

    describe('isEntityEligible', () => {
        let object = null;
        let latestVersion = null;
        let nonCurrentVersion = null;
        let latestDeleteMarker = null;
        let nonCurrentDeleteMarker = null;

        beforeEach(() => {
            // shallow copy entities to start "clean".
            object = { ...OBJECT };
            latestVersion = { ...LATEST_VERSION };
            nonCurrentVersion = { ...NON_CURRENT_VERSION };
            latestDeleteMarker = { ...LATEST_DELETE_MARKER };
            nonCurrentDeleteMarker = { ...NON_CURRENT_DELETE_MARKER };
        });

        // Test non-versioned object
        it('should return true if 1 day expiration rule on 1 day old non-versioned object', () => {
            const rules = [
                {
                    Expiration: { Days: 1 },
                    ID: 'id1',
                    Prefix: '',
                    Status: 'Enabled',
                    Transitions: [],
                    NoncurrentVersionTransitions: [],
                },
            ];
            object.LastModified = new Date(Date.now() - DAY).toISOString();
            const versioningStatus = 'Disabled';

            const result = lct._isEntityEligible(rules, object, versioningStatus);
            assert.strictEqual(result, true);
        });

        it('should return false if 1 day expiration rule on 0 day old non-versioned object', () => {
            const rules = [
                {
                    Expiration: { Days: 1 },
                    ID: 'id1',
                    Prefix: '',
                    Status: 'Enabled',
                    Transitions: [],
                    NoncurrentVersionTransitions: [],
                },
            ];
            object.LastModified = new Date(Date.now()).toISOString();
            const versioningStatus = 'Disabled';

            const result = lct._isEntityEligible(rules, object, versioningStatus);
            assert.strictEqual(result, false);
        });

        it('should return true if expiration date has passed for non-versioned object', () => {
            const rules = [
                {
                    Expiration: { Date: new Date(Date.now() - HOUR) },
                    ID: 'id1',
                    Prefix: '',
                    Status: 'Enabled',
                    Transitions: [],
                    NoncurrentVersionTransitions: [],
                },
            ];
            const versioningStatus = 'Disabled';

            const result = lct._isEntityEligible(rules, object, versioningStatus);
            assert.strictEqual(result, true);
        });

        it('should return false if expiration date has not passed for non-versioned object', () => {
            const rules = [
                {
                    Expiration: { Date: new Date(Date.now() + HOUR) },
                    ID: 'id1',
                    Prefix: '',
                    Status: 'Enabled',
                    Transitions: [],
                    NoncurrentVersionTransitions: [],
                },
            ];
            const versioningStatus = 'Disabled';

            const result = lct._isEntityEligible(rules, object, versioningStatus);
            assert.strictEqual(result, false);
        });

        it('should return true if at least one rule is eligible', () => {
            const rules = [
                {
                    Expiration: { Days: 1 },
                    ID: 'id1',
                    Prefix: '',
                    Status: 'Enabled',
                    Transitions: [],
                    NoncurrentVersionTransitions: [],
                },
                {
                    Expiration: { Days: 2 },
                    ID: 'id2',
                    Prefix: '',
                    Status: 'Enabled',
                    Transitions: [],
                    NoncurrentVersionTransitions: [],
                },
            ];
            object.LastModified = new Date(Date.now() - DAY).toISOString();
            const versioningStatus = 'Disabled';

            const result = lct._isEntityEligible(rules, object, versioningStatus);
            assert.strictEqual(result, true);
        });

        it('should return false if no rule is eligible', () => {
            const rules = [
                {
                    Expiration: { Days: 1 },
                    ID: 'id1',
                    Prefix: '',
                    Status: 'Enabled',
                    Transitions: [],
                    NoncurrentVersionTransitions: [],
                },
                {
                    Expiration: { Days: 1 },
                    ID: 'id2',
                    Filter: {
                        Tag: { Key: 'key', Value: 'val' },
                    },
                    Status: 'Enabled',
                    Transitions: [],
                    NoncurrentVersionTransitions: [],
                },
            ];
            object.LastModified = new Date(Date.now()).toISOString();
            const versioningStatus = 'Disabled';

            const result = lct._isEntityEligible(rules, object, versioningStatus);
            assert.strictEqual(result, false);
        });

        it('should return false if potential eligible rules are disabled', () => {
            const rules = [
                {
                    Expiration: { Days: 1 },
                    ID: 'id1',
                    Prefix: '',
                    Status: 'Disabled',
                    Transitions: [],
                    NoncurrentVersionTransitions: [],
                },
                {
                    Expiration: { Days: 1 },
                    ID: 'id2',
                    Filter: {
                        Tag: { Key: 'key', Value: 'val' },
                    },
                    Status: 'Disabled',
                    Transitions: [],
                    NoncurrentVersionTransitions: [],
                },
            ];
            object.LastModified = new Date(Date.now() - DAY).toISOString();
            const versioningStatus = 'Disabled';

            const result = lct._isEntityEligible(rules, object, versioningStatus);
            assert.strictEqual(result, false);
        });

        it('should return true if 1 day transition rule on 1 day old non-versioned object', () => {
            const rules = [
                {
                    ID: 'id1',
                    Prefix: '',
                    Status: 'Enabled',
                    Transitions: [{ Days: 1, StorageClass: 'us-east-2' }],
                    NoncurrentVersionTransitions: [],
                },
            ];
            object.LastModified = new Date(Date.now() - DAY).toISOString();
            const versioningStatus = 'Disabled';

            const result = lct._isEntityEligible(rules, object, versioningStatus);
            assert.strictEqual(result, true);
        });

        it('should return false if 1 day transition rule on 0 day old non-versioned object', () => {
            const rules = [
                {
                    ID: 'id1',
                    Prefix: '',
                    Status: 'Enabled',
                    Transitions: [{ Days: 1, StorageClass: 'us-east-2' }],
                    NoncurrentVersionTransitions: [],
                },
            ];
            object.LastModified = new Date(Date.now()).toISOString();
            const versioningStatus = 'Disabled';

            const result = lct._isEntityEligible(rules, object, versioningStatus);
            assert.strictEqual(result, false);
        });

        it('should return true if transition date has passed for non-versioned object', () => {
            const rules = [
                {
                    ID: 'id1',
                    Prefix: '',
                    Status: 'Enabled',
                    Transitions: [{
                        Date: new Date(Date.now() - HOUR),
                        StorageClass: 'us-east-2',
                    }],
                    NoncurrentVersionTransitions: [],
                },
            ];
            const versioningStatus = 'Disabled';

            const result = lct._isEntityEligible(rules, object, versioningStatus);
            assert.strictEqual(result, true);
        });

        it('should return false if transition date has not passed for non-versioned object', () => {
            const rules = [
                {
                    ID: 'id1',
                    Prefix: '',
                    Status: 'Enabled',
                    Transitions: [{
                        Date: new Date(Date.now() + HOUR),
                        StorageClass: 'us-east-2',
                    }],
                    NoncurrentVersionTransitions: [],
                },
            ];
            const versioningStatus = 'Disabled';

            const result = lct._isEntityEligible(rules, object, versioningStatus);
            assert.strictEqual(result, false);
        });

        it('should return false if non-current version expiration rule on a non-versioned object', () => {
            const rules = [
                {
                    NoncurrentVersionExpiration: { NoncurrentDays: 1 },
                    ID: 'id1',
                    Prefix: '',
                    Status: 'Enabled',
                    Transitions: [],
                    NoncurrentVersionTransitions: [],
                },
            ];
            object.LastModified = new Date(Date.now() - DAY).toISOString();
            const versioningStatus = 'Disabled';

            const result = lct._isEntityEligible(rules, object, versioningStatus);
            assert.strictEqual(result, false);
        });

        // Test latest version
        it('should return true if 1 day expiration rule on 1 day old latest version', () => {
            const rules = [
                {
                    Expiration: { Days: 1 },
                    ID: 'id1',
                    Prefix: '',
                    Status: 'Enabled',
                    Transitions: [],
                    NoncurrentVersionTransitions: [],
                },
            ];
            latestVersion.LastModified = new Date(Date.now() - DAY).toISOString();
            const versioningStatus = 'Enabled';

            const result = lct._isEntityEligible(rules, latestVersion, versioningStatus);
            assert.strictEqual(result, true);
        });

        it('should return false if 1 day expiration rule on 0 day old latest version', () => {
            const rules = [
                {
                    Expiration: { Days: 1 },
                    ID: 'id1',
                    Prefix: '',
                    Status: 'Enabled',
                    Transitions: [],
                    NoncurrentVersionTransitions: [],
                },
            ];
            latestVersion.LastModified = new Date(Date.now()).toISOString();
            const versioningStatus = 'Enabled';

            const result = lct._isEntityEligible(rules, latestVersion, versioningStatus);
            assert.strictEqual(result, false);
        });

        it('should return true if expiration date has passed for latest version', () => {
            const rules = [
                {
                    Expiration: { Date: new Date(Date.now() - HOUR) },
                    ID: 'id1',
                    Prefix: '',
                    Status: 'Enabled',
                    Transitions: [],
                    NoncurrentVersionTransitions: [],
                },
            ];
            const versioningStatus = 'Enabled';

            const result = lct._isEntityEligible(rules, latestVersion, versioningStatus);
            assert.strictEqual(result, true);
        });

        it('should return false if expiration date has not passed for latest version', () => {
            const rules = [
                {
                    Expiration: { Date: new Date(Date.now() + HOUR) },
                    ID: 'id1',
                    Prefix: '',
                    Status: 'Enabled',
                    Transitions: [],
                    NoncurrentVersionTransitions: [],
                },
            ];
            const versioningStatus = 'Enabled';

            const result = lct._isEntityEligible(rules, latestVersion, versioningStatus);
            assert.strictEqual(result, false);
        });

        it('should return true if 1 day transition rule on 1 day old latest version', () => {
            const rules = [
                {
                    ID: 'id1',
                    Prefix: '',
                    Status: 'Enabled',
                    Transitions: [{ Days: 1, StorageClass: 'us-east-2' }],
                    NoncurrentVersionTransitions: [],
                },
            ];
            latestVersion.LastModified = new Date(Date.now() - DAY).toISOString();
            const versioningStatus = 'Enabled';

            const result = lct._isEntityEligible(rules, latestVersion, versioningStatus);
            assert.strictEqual(result, true);
        });

        it('should return false if 1 day transition rule on 0 day old latest version', () => {
            const rules = [
                {
                    ID: 'id1',
                    Prefix: '',
                    Status: 'Enabled',
                    Transitions: [{ Days: 1, StorageClass: 'us-east-2' }],
                    NoncurrentVersionTransitions: [],
                },
            ];
            latestVersion.LastModified = new Date(Date.now()).toISOString();
            const versioningStatus = 'Enabled';

            const result = lct._isEntityEligible(rules, latestVersion, versioningStatus);
            assert.strictEqual(result, false);
        });

        it('should return true if transition date has passed for latest version', () => {
            const rules = [
                {
                    ID: 'id1',
                    Prefix: '',
                    Status: 'Enabled',
                    Transitions: [{
                        Date: new Date(Date.now() - HOUR),
                        StorageClass: 'us-east-2',
                    }],
                    NoncurrentVersionTransitions: [],
                },
            ];
            const versioningStatus = 'Enabled';

            const result = lct._isEntityEligible(rules, latestVersion, versioningStatus);
            assert.strictEqual(result, true);
        });

        it('should return false if transition date has not passed for latest version', () => {
            const rules = [
                {
                    ID: 'id1',
                    Prefix: '',
                    Status: 'Enabled',
                    Transitions: [{
                        Date: new Date(Date.now() + HOUR),
                        StorageClass: 'us-east-2',
                    }],
                    NoncurrentVersionTransitions: [],
                },
            ];
            const versioningStatus = 'Enabled';

            const result = lct._isEntityEligible(rules, latestVersion, versioningStatus);
            assert.strictEqual(result, false);
        });

        it('should return false if non-current version expiration rule on a latest version', () => {
            const rules = [
                {
                    NoncurrentVersionExpiration: { NoncurrentDays: 1 },
                    ID: 'id1',
                    Prefix: '',
                    Status: 'Enabled',
                    Transitions: [],
                    NoncurrentVersionTransitions: [],
                },
            ];
            latestVersion.LastModified = new Date(Date.now() - DAY).toISOString();
            const versioningStatus = 'Enabled';

            const result = lct._isEntityEligible(rules, latestVersion, versioningStatus);
            assert.strictEqual(result, false);
        });

        // Test non current version
        it('should return false if expiration rule with days is set for a non-current version', () => {
            const rules = [
                {
                    Expiration: { Days: 1 },
                    ID: 'id1',
                    Prefix: '',
                    Status: 'Enabled',
                    Transitions: [],
                    NoncurrentVersionTransitions: [],
                },
            ];
            nonCurrentVersion.LastModified = new Date(Date.now() - DAY).toISOString();
            const versioningStatus = 'Enabled';

            const result = lct._isEntityEligible(rules, nonCurrentVersion, versioningStatus);
            assert.strictEqual(result, false);
        });


        it('should return false if expiration rule with date is set for a non-current version', () => {
            const rules = [
                {
                    Expiration: { Date: new Date(Date.now()) },
                    ID: 'id1',
                    Prefix: '',
                    Status: 'Enabled',
                    Transitions: [],
                    NoncurrentVersionTransitions: [],
                },
            ];
            const versioningStatus = 'Enabled';

            const result = lct._isEntityEligible(rules, nonCurrentVersion, versioningStatus);
            assert.strictEqual(result, false);
        });

        it('should return false if transition rule with days is set for a non-current version', () => {
            const rules = [
                {
                    ID: 'id1',
                    Prefix: '',
                    Status: 'Enabled',
                    Transitions: [{ Days: 1, StorageClass: 'us-east-2' }],
                    NoncurrentVersionTransitions: [],
                },
            ];
            nonCurrentVersion.LastModified = new Date(Date.now() - DAY).toISOString();
            const versioningStatus = 'Enabled';

            const result = lct._isEntityEligible(rules, nonCurrentVersion, versioningStatus);
            assert.strictEqual(result, false);
        });

        it('should return false if transition rule with date is set for a non-current version', () => {
            const rules = [
                {
                    ID: 'id1',
                    Prefix: '',
                    Status: 'Enabled',
                    Transitions: [{
                        Date: new Date(Date.now()),
                        StorageClass: 'us-east-2',
                    }],
                    NoncurrentVersionTransitions: [],
                },
            ];
            const versioningStatus = 'Enabled';

            const result = lct._isEntityEligible(rules, nonCurrentVersion, versioningStatus);
            assert.strictEqual(result, false);
        });

        it('should return true if 1 day non-current version expiration rule on 1 day old non-current version', () => {
            const rules = [
                {
                    NoncurrentVersionExpiration: { NoncurrentDays: 1 },
                    ID: 'id1',
                    Prefix: '',
                    Status: 'Enabled',
                    Transitions: [],
                    NoncurrentVersionTransitions: [],
                },
            ];
            nonCurrentVersion.staleDate = new Date(Date.now() - DAY).toISOString();
            const versioningStatus = 'Enabled';

            const result = lct._isEntityEligible(rules, nonCurrentVersion, versioningStatus);
            assert.strictEqual(result, true);
        });

        it('should return false if 1 day non-current version expiration rule on 0 day old non-current version', () => {
            const rules = [
                {
                    NoncurrentVersionExpiration: { NoncurrentDays: 1 },
                    ID: 'id1',
                    Prefix: '',
                    Status: 'Enabled',
                    Transitions: [],
                    NoncurrentVersionTransitions: [],
                },
            ];
            nonCurrentVersion.staleDate = new Date(Date.now()).toISOString();
            const versioningStatus = 'Enabled';

            const result = lct._isEntityEligible(rules, nonCurrentVersion, versioningStatus);
            assert.strictEqual(result, false);
        });

        // Test latest delete marker
        it('should return true even if misapplied expiration rule on a latest delete marker', () => {
            const rules = [
                {
                    Expiration: { Days: 1 },
                    ID: 'id1',
                    Prefix: '',
                    Status: 'Enabled',
                    Transitions: [],
                    NoncurrentVersionTransitions: [],
                },
            ];
            latestDeleteMarker.LastModified = new Date(Date.now()).toISOString();
            const versioningStatus = 'Enabled';

            const result = lct._isEntityEligible(rules, latestDeleteMarker, versioningStatus);
            assert.strictEqual(result, true);
        });

        it('should return true even if misapplied transition rule on a latest delete marker', () => {
            const rules = [
                {
                    ID: 'id1',
                    Prefix: '',
                    Status: 'Enabled',
                    Transitions: [{ Days: 1, StorageClass: 'us-east-2' }],
                    NoncurrentVersionTransitions: [],
                },
            ];
            latestDeleteMarker.LastModified = new Date(Date.now()).toISOString();
            const versioningStatus = 'Enabled';

            const result = lct._isEntityEligible(rules, latestDeleteMarker, versioningStatus);
            assert.strictEqual(result, true);
        });

        it('should return true even if misapplied non-current version expiration rule on latest delete marker', () => {
            const rules = [
                {
                    NoncurrentVersionExpiration: { NoncurrentDays: 1 },
                    ID: 'id1',
                    Prefix: '',
                    Status: 'Enabled',
                    Transitions: [],
                    NoncurrentVersionTransitions: [],
                },
            ];
            latestDeleteMarker.LastModified = new Date(Date.now()).toISOString();
            const versioningStatus = 'Enabled';

            const result = lct._isEntityEligible(rules, latestDeleteMarker, versioningStatus);
            assert.strictEqual(result, true);
        });

        // Test non-current delete marker
        it('should return false if expiration rule on a non-current delete marker', () => {
            const rules = [
                {
                    Expiration: { Days: 1 },
                    ID: 'id1',
                    Prefix: '',
                    Status: 'Enabled',
                    Transitions: [],
                    NoncurrentVersionTransitions: [],
                },
            ];
            nonCurrentDeleteMarker.staleDate = new Date(Date.now() - DAY).toISOString();
            const versioningStatus = 'Enabled';

            const result = lct._isEntityEligible(rules, nonCurrentDeleteMarker, versioningStatus);
            assert.strictEqual(result, false);
        });

        it('should return false if transition rule on a non-current delete marker', () => {
            const rules = [
                {
                    ID: 'id1',
                    Prefix: '',
                    Status: 'Enabled',
                    Transitions: [{ Days: 1, StorageClass: 'us-east-2' }],
                    NoncurrentVersionTransitions: [],
                },
            ];
            nonCurrentDeleteMarker.staleDate = new Date(Date.now() - DAY).toISOString();
            const versioningStatus = 'Enabled';

            const result = lct._isEntityEligible(rules, nonCurrentDeleteMarker, versioningStatus);
            assert.strictEqual(result, false);
        });

        it('should return false if misapplied non-current version expiration rule on non-current delete marker', () => {
            const rules = [
                {
                    NoncurrentVersionExpiration: { NoncurrentDays: 1 },
                    ID: 'id1',
                    Prefix: '',
                    Status: 'Enabled',
                    Transitions: [],
                    NoncurrentVersionTransitions: [],
                },
            ];
            nonCurrentDeleteMarker.staleDate = new Date(Date.now()).toISOString();
            const versioningStatus = 'Enabled';

            const result = lct._isEntityEligible(rules, nonCurrentDeleteMarker, versioningStatus);
            assert.strictEqual(result, false);
        });

        it('should return true even if legit non-current version expiration rule on non-current delete marker', () => {
            const rules = [
                {
                    NoncurrentVersionExpiration: { NoncurrentDays: 1 },
                    ID: 'id1',
                    Prefix: '',
                    Status: 'Enabled',
                    Transitions: [],
                    NoncurrentVersionTransitions: [],
                },
            ];
            nonCurrentDeleteMarker.staleDate = new Date(Date.now() - DAY).toISOString();
            const versioningStatus = 'Enabled';

            const result = lct._isEntityEligible(rules, nonCurrentDeleteMarker, versioningStatus);
            assert.strictEqual(result, true);
        });
    });
});
