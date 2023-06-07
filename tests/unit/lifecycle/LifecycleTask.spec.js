'use strict'; // eslint-disable-line

const assert = require('assert');

const LifecycleTask = require(
    '../../../extensions/lifecycle/tasks/LifecycleTask');
const fakeLogger = require('../../utils/fakeLogger');
const { timeOptions } = require('../../functional/lifecycle/configObjects');

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
            ncvHeap: new Map(),
            lcOptions: timeOptions,
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

        const oldLastModified = new Date(Date.now() - (2 * DAY)).toISOString();
        const lastModified = new Date(Date.now()).toISOString();

        const bucketData = {
            target: {
                owner: 'test-user',
                bucket: 'test-bucket',
            },
        };
        // user created delete marker
        const deleteMarker = {
            Owner: {
                DisplayName: 'Not Lifecycle Service Account',
                ID: '86346e5bda4c2158985574c9942089c36ca650dc509',
            },
            Key: 'test-key',
            VersionId:
            '834373731313631393339313839393939393952473030312020353820',
            LastModified: lastModified,
        };
        const deleteMarkerOld = {
            Owner: {
                DisplayName: 'Not Lifecycle Service Account',
                ID: '86346e5bda4c2158985574c9942089c36ca650dc509',
            },
            Key: 'test-key',
            VersionId:
            '834373731313631393339313839393939393952473030312020353820',
            LastModified: oldLastModified,
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

        it('should NOT send any entry to Kafka when delete marker is not eligible based on its age', () => {
            const rules = {
                Expiration: { Days: 5 },
            };

            lct2._checkAndApplyEODMRule(bucketData, deleteMarker,
            listOfVersions, rules, fakeLogger, err => {
                assert.ifError(err);

                const latestEntry = lct2.getLatestEntry();
                assert.equal(latestEntry, undefined);
            });
        });

        it('should send any entry to Kafka when delete marker meets the age criteria and ' +
        'ExpiredObjectDeleteMarker is not set', () => {
            const rules = {
                Expiration: { Days: 1 },
            };

            lct2._checkAndApplyEODMRule(bucketData, deleteMarkerOld,
            listOfVersions, rules, fakeLogger, err => {
                assert.ifError(err);

                const latestEntry = lct2.getLatestEntry();
                const expectedTarget = Object.assign({}, bucketData.target, {
                    key: deleteMarkerOld.Key,
                    version: deleteMarkerOld.VersionId,
                });
                assert(latestEntry, 'entry has not been sent');
                assert.strictEqual(latestEntry.getActionType(), 'deleteObject');
                assert.deepStrictEqual(
                    latestEntry.getAttribute('target'), expectedTarget);
            });
        });

        it('should send any entry to Kafka when delete marker meets the age criteria and ' +
        'ExpiredObjectDeleteMarker is set to false', () => {
            const rules = {
                Expiration: { Days: 1, ExpiredObjectDeleteMarker: false },
            };

            lct2._checkAndApplyEODMRule(bucketData, deleteMarkerOld,
            listOfVersions, rules, fakeLogger, err => {
                assert.ifError(err);

                const latestEntry = lct2.getLatestEntry();
                const expectedTarget = Object.assign({}, bucketData.target, {
                    key: deleteMarkerOld.Key,
                    version: deleteMarkerOld.VersionId,
                });
                assert(latestEntry, 'entry has not been sent');
                assert.strictEqual(latestEntry.getActionType(), 'deleteObject');
                assert.deepStrictEqual(
                    latestEntry.getAttribute('target'), expectedTarget);
            });
        });

        it('should send an entry to Kafka when ExpiredObjectDeleteMarker is enabled', () => {
            const rules = {
                Expiration: { ExpiredObjectDeleteMarker: true },
            };

            lct2._checkAndApplyEODMRule(bucketData, deleteMarker,
            listOfVersions, rules, fakeLogger, err => {
                assert.ifError(err);

                const latestEntry = lct2.getLatestEntry();
                const expectedTarget = Object.assign({}, bucketData.target, {
                    key: deleteMarker.Key,
                    version: deleteMarker.VersionId,
                });
                assert(latestEntry, 'entry has not been sent');
                assert.strictEqual(latestEntry.getActionType(), 'deleteObject');
                assert.deepStrictEqual(
                    latestEntry.getAttribute('target'), expectedTarget);
            });
        });

        it('should send an entry to Kafka when ExpiredObjectDeleteMarker is ' +
        'enabled and delete marker is not eligible based on its age', () => {
            const rules = {
                Expiration: { Days: 5, ExpiredObjectDeleteMarker: true },
            };

            lct2._checkAndApplyEODMRule(bucketData, deleteMarker,
            listOfVersions, rules, fakeLogger, err => {
                assert.ifError(err);

                const latestEntry = lct2.getLatestEntry();
                const expectedTarget = Object.assign({}, bucketData.target, {
                    key: deleteMarker.Key,
                    version: deleteMarker.VersionId,
                });
                assert(latestEntry, 'entry has not been sent');
                assert.strictEqual(latestEntry.getActionType(), 'deleteObject');
                assert.deepStrictEqual(
                    latestEntry.getAttribute('target'), expectedTarget);
            });
        });

        it('should NOT send an entry to Kafka when no Expiration rule is set', () => {
            const rules = {};

            lct2._checkAndApplyEODMRule(bucketData, deleteMarker,
            listOfVersions, rules, fakeLogger, err => {
                assert.ifError(err);

                const latestEntry = lct2.getLatestEntry();
                assert.equal(latestEntry, undefined);
            });
        });

        it('should NOT send an entry to Kafka if ExpiredObjectDeleteMarker rule is explicitly set to false', () => {
            const rules = {
                Expiration: { ExpiredObjectDeleteMarker: false },
            };

            lct2._checkAndApplyEODMRule(bucketData, deleteMarker,
            listOfVersions, rules, fakeLogger, err => {
                assert.ifError(err);

                const latestEntry = lct2.getLatestEntry();
                assert.equal(latestEntry, undefined);
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

    describe('_checkAndApplyNCVExpirationRule', () => {
        let lct2;

        const bucketData = {
            target: {
                owner: 'test-user',
                bucket: 'test-bucket',
            },
        };
        const testDate = Date.now();
        const versions = [
            {
                Key: 'testkey',
                VersionId: '4',
                staleDate: new Date(testDate - (1 * DAY)).toISOString(),
            },
            {
                Key: 'testkey',
                VersionId: '3',
                staleDate: new Date(testDate - (2 * DAY)).toISOString(),
            },
            {
                Key: 'testkey',
                VersionId: '1',
                staleDate: new Date(testDate - (4 * DAY)).toISOString(),
            },
            {
                Key: 'testkey',
                VersionId: '2',
                staleDate: new Date(testDate - (3 * DAY)).toISOString(),
            },
            {
                Key: 'testkey',
                VersionId: '0',
                staleDate: new Date(testDate - (5 * DAY)).toISOString(),
            },
        ];

        before(() => {
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
                    this.ncvHeap = new Map();
                }
            }
            lct2 = new LifecycleTaskMock(lp);
        });

        afterEach(() => {
            lct2.reset();
        });

        describe('when NewerNoncurrentVersion field is present', () => {
            it('should executed expected behavior', () => {
                const rules = {
                    Id: 'rule_name',
                    NoncurrentVersionExpiration: {
                        NoncurrentDays: 1,
                        NewerNoncurrentVersions: 3,
                    },
                };

                const expectedEntries = [
                    undefined,
                    undefined,
                    undefined,
                    versions[2], // VersionId == 1
                    versions[4], // VersionId == 0
                ];

                versions.forEach((v, idx) => {
                    lct2._checkAndApplyNCVExpirationRule(bucketData, versions[idx], rules, fakeLogger);
                    const latestEntry = lct2.getLatestEntry();

                    if (!expectedEntries[idx]) {
                        assert.strictEqual(latestEntry, undefined);
                        return;
                    }

                    const expectedTarget = Object.assign(
                        {},
                        bucketData.target,
                        {
                            key: expectedEntries[idx].Key,
                            version: expectedEntries[idx].VersionId,
                        }
                    );

                    assert.strictEqual(latestEntry.getActionType(), 'deleteObject');
                    assert.deepStrictEqual(
                        latestEntry.getAttribute('target'), expectedTarget);
                });
            });
        });

        describe('when NewerNoncurrentVersion field is not present', () => {
            it('should send all versions for expiration', () => {
                const rules = {
                    Id: 'rule_name',
                    NoncurrentVersionExpiration: {
                        NoncurrentDays: 1,
                    },
                };

                const expectedEntries = [
                    versions[0],
                    versions[1],
                    versions[2],
                    versions[3],
                    versions[4],
                ];

                versions.forEach((v, idx) => {
                    lct2._checkAndApplyNCVExpirationRule(bucketData, versions[idx], rules, fakeLogger);
                    const latestEntry = lct2.getLatestEntry();

                    if (!expectedEntries[idx]) {
                        assert.strictEqual(latestEntry, undefined);
                        return;
                    }

                    const expectedTarget = Object.assign(
                        {},
                        bucketData.target,
                        {
                            key: expectedEntries[idx].Key,
                            version: expectedEntries[idx].VersionId,
                        }
                    );

                    assert.strictEqual(latestEntry.getActionType(), 'deleteObject');
                    assert.deepStrictEqual(
                        latestEntry.getAttribute('target'), expectedTarget);
                });
            });
        });
    });

    describe('_ncvHeapAdd', () => {
        let lct2;

        const rules = {
            Id: 'rule_name',
            NoncurrentVersionExpiration: {
                NoncurrentDays: 1,
                NewerNoncurrentVersions: 3,
            },
        };
        const testDate = Date.now();
        const versions = [
            {
                Key: 'testkey',
                VersionId: '4',
                staleDate: new Date(testDate - (1 * DAY)).toISOString(),
            },
            {
                Key: 'testkey',
                VersionId: '3',
                staleDate: new Date(testDate - (2 * DAY)).toISOString(),
            },
            {
                Key: 'testkey',
                VersionId: '1',
                staleDate: new Date(testDate - (4 * DAY)).toISOString(),
            },
            {
                Key: 'testkey',
                VersionId: '2',
                staleDate: new Date(testDate - (3 * DAY)).toISOString(),
            },
            {
                Key: 'testkey',
                VersionId: '0',
                staleDate: new Date(testDate - (5 * DAY)).toISOString(),
            },
        ];

        before(() => {
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
                    this.ncvHeap = new Map();
                }
            }
            lct2 = new LifecycleTaskMock(lp);
        });

        afterEach(() => {
            lct2.reset();
        });

        it('should popuplate and return null if heap has space', () => {
            const ret = lct2._ncvHeapAdd('testbucket', rules, versions[0]);
            assert.strictEqual(ret, null);
        });

        it('should popuplate and return oldest items from if heap is at capacity', () => {
            let ret = lct2._ncvHeapAdd('testbucket', rules, versions[0]); // 4
            assert.strictEqual(ret, null);
            ret = lct2._ncvHeapAdd('testbucket', rules, versions[2]); // 1
            assert.strictEqual(ret, null);
            ret = lct2._ncvHeapAdd('testbucket', rules, versions[3]); // 2
            assert.strictEqual(ret, null);
            ret = lct2._ncvHeapAdd('testbucket', rules, versions[4]); // 0
            assert.strictEqual(ret, versions[4]); // 0
            ret = lct2._ncvHeapAdd('testbucket', rules, versions[1]); // 3
            assert.strictEqual(ret, versions[2]); // 1
        });
    });

    describe('_ncvHeapObjectsClear', () => {
        let lct2;

        before(() => {
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
                    this.ncvHeap = new Map();
                }
            }
            lct2 = new LifecycleTaskMock(lp);
        });

        afterEach(() => {
            lct2.reset();
        });

        it('should clear the ncvHeap object of the listed bucket/keys', () => {
            const rules = {
                Id: 'rule_name',
                NoncurrentVersionExpiration: {
                    NoncurrentDays: 1,
                    NewerNoncurrentVersions: 10,
                },
            };
            const testDate = Date.now();
            const version1 = {
                Key: 'testkey1',
                VersionId: '1',
                staleDate: new Date(testDate - (1 * DAY)).toISOString(),
            };

            const version2 = {
                Key: 'testkey2',
                VersionId: '2',
                staleDate: new Date(testDate - (1 * DAY)).toISOString(),
            };

            const version3 = {
                Key: 'testkey3',
                VersionId: '3',
                staleDate: new Date(testDate - (1 * DAY)).toISOString(),
            };

            const uniqueKeySet = new Set(['testkey1', 'testkey2']);

            const b1 = 'testbucket1';
            const b2 = 'testbucket2';

            lct2._ncvHeapAdd(b1, rules, version1);
            lct2._ncvHeapAdd(b1, rules, version1);
            lct2._ncvHeapAdd(b1, rules, version2);
            lct2._ncvHeapAdd(b1, rules, version3);
            lct2._ncvHeapAdd(b2, rules, version1);
            lct2._ncvHeapAdd(b2, rules, version2);

            lct2._ncvHeapObjectsClear(b1, uniqueKeySet);

            assert(lct2.ncvHeap.has(b1));
            assert(!lct2.ncvHeap.get(b1).has(version1.Key));
            assert(!lct2.ncvHeap.get(b1).has(version2.Key));
            assert(lct2.ncvHeap.get(b1).get(version3.Key).has(rules.Id));
            assert.strictEqual(lct2.ncvHeap.get(b1).get(version3.Key).get(rules.Id).size, 1);

            assert(lct2.ncvHeap.has(b2));
            assert(lct2.ncvHeap.get(b2).has(version1.Key));
            assert(lct2.ncvHeap.get(b2).get(version1.Key).has(rules.Id));
            assert.strictEqual(lct2.ncvHeap.get(b2).get(version1.Key).get(rules.Id).size, 1);
            assert(lct2.ncvHeap.get(b2).has(version2.Key));
            assert(lct2.ncvHeap.get(b2).get(version2.Key).has(rules.Id));
            assert.strictEqual(lct2.ncvHeap.get(b2).get(version2.Key).get(rules.Id).size, 1);
        });
    });

    describe('_ncvHeapBucketClear', () => {
        let lct2;

        before(() => {
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
                    this.ncvHeap = new Map();
                }
            }
            lct2 = new LifecycleTaskMock(lp);
        });

        afterEach(() => {
            lct2.reset();
        });

        it('should clear the ncvHeap object of the listed bucket/keys', () => {
            const rules = {
                Id: 'rule_name',
                NoncurrentVersionExpiration: {
                    NoncurrentDays: 1,
                    NewerNoncurrentVersions: 10,
                },
            };
            const testDate = Date.now();
            const version1 = {
                Key: 'testkey1',
                VersionId: '1',
                staleDate: new Date(testDate - (1 * DAY)).toISOString(),
            };

            const version2 = {
                Key: 'testkey2',
                VersionId: '2',
                staleDate: new Date(testDate - (1 * DAY)).toISOString(),
            };

            const version3 = {
                Key: 'testkey3',
                VersionId: '3',
                staleDate: new Date(testDate - (1 * DAY)).toISOString(),
            };

            const b1 = 'testbucket1';
            const b2 = 'testbucket2';

            lct2._ncvHeapAdd(b1, rules, version1);
            lct2._ncvHeapAdd(b1, rules, version1);
            lct2._ncvHeapAdd(b1, rules, version2);
            lct2._ncvHeapAdd(b1, rules, version3);
            lct2._ncvHeapAdd(b2, rules, version1);
            lct2._ncvHeapAdd(b2, rules, version2);

            lct2._ncvHeapBucketClear(b1);

            assert(!lct2.ncvHeap.has(b1));
            assert(lct2.ncvHeap.has(b2));
            assert(lct2.ncvHeap.get(b2).has(version1.Key));
            assert(lct2.ncvHeap.get(b2).get(version1.Key).has(rules.Id));
            assert.strictEqual(lct2.ncvHeap.get(b2).get(version1.Key).get(rules.Id).size, 1);
            assert(lct2.ncvHeap.get(b2).has(version2.Key));
            assert(lct2.ncvHeap.get(b2).get(version2.Key).has(rules.Id));
            assert.strictEqual(lct2.ncvHeap.get(b2).get(version2.Key).get(rules.Id).size, 1);
        });
    });

    describe('_getObjectVersions', () => {
        let lct2;

        const bucketData = {
            target: {
                owner: 'test-user',
                bucket: 'test-bucket',
            },
            details: {},
        };

        before(() => {
            class LifecycleTaskMock extends LifecycleTask {
                constructor(lp) {
                    super(lp);
                    this.listResponse = {};
                    this.objectsClearCalledWith = null;
                    this.bucketClearCalledWith = null;
                }

                _sendObjectAction(entry, cb) {
                    this.latestEntry = entry;
                    return cb();
                }

                _sendBucketEntry(entry, cb) {
                    this.latestBucketEntry = entry;
                    return cb();
                }

                getLatestEntry() {
                    return this.latestEntry;
                }

                reset() {
                    this.latestEntry = undefined;
                    this.ncvHeap = new Map();
                    this.listResponse = {};
                    this.objectsClearCalledWith = null;
                    this.bucketClearCalledWith = null;
                }

                _ncvHeapObjectsClear(bucketName, uniqueObjectKeys) {
                    this.objectsClearCalledWith = {
                        bucketName,
                        uniqueObjectKeys,
                    };
                }

                _ncvHeapBucketClear(bucketName) {
                    this.bucketClearCalledWith = { bucketName };
                }

                _compareRulesToList(
                    bucketData,
                    bucketLCRules,
                    allVersionsWithStateDate,
                    log,
                    versioningStatus,
                    cb
                ) {
                    return cb();
                }

                _listVersions(bucketData, paramDetails, log, cb) {
                    return cb(null, this.listResponse);
                }
            }
            lct2 = new LifecycleTaskMock(lp);
        });

        afterEach(() => {
            lct2.reset();
        });

        it('should clear heap bucket-level entry when IsTruncated is false', done => {
            lct2.listResponse = {
                IsTruncated: false,
                Versions: [
                    {
                        Key: 'obj1',
                        VersionId: '1',
                        LastModified: '2021-10-04T21:46:49.157Z',
                        ETag: '1:3749f52bb326ae96782b42dc0a97b4c1',
                        Size: 1,
                        StorageClass: 'site1',
                    },
                    {
                        Key: 'obj2',
                        VersionId: '1',
                        LastModified: '2021-10-04T21:46:49.157Z',
                        ETag: '1:3749f52bb326ae96782b42dc0a97b4c1',
                        Size: 1,
                        StorageClass: 'site1',
                    },
                    {
                        Key: 'obj3',
                        VersionId: '1',
                        LastModified: '2021-10-04T21:46:49.157Z',
                        ETag: '1:3749f52bb326ae96782b42dc0a97b4c1',
                        Size: 1,
                        StorageClass: 'site1',
                    },
                ],
                DeleteMarkers: [],
            };
            lct2._getObjectVersions(bucketData, {}, 'Enabled', 0, fakeLogger,
                err => {
                    assert.ifError(err);
                    assert.strictEqual(lct2.objectsClearCalledWith, null);
                    assert.deepStrictEqual(lct2.bucketClearCalledWith, { bucketName: 'test-bucket' });
                    done();
                });
        });

        it('should clear heap object-level entries when IsTruncated is true', done => {
            lct2.listResponse = {
                IsTruncated: true,
                NextKeyMarker: 'obj3',
                Versions: [
                    {
                        Key: 'obj1',
                        VersionId: '1',
                        LastModified: '2021-10-04T21:46:49.157Z',
                        ETag: '1:3749f52bb326ae96782b42dc0a97b4c1',
                        Size: 1,
                        StorageClass: 'site1',
                    },
                    {
                        Key: 'obj2',
                        VersionId: '1',
                        LastModified: '2021-10-04T21:46:49.157Z',
                        ETag: '1:3749f52bb326ae96782b42dc0a97b4c1',
                        Size: 1,
                        StorageClass: 'site1',
                    },
                    {
                        Key: 'obj3',
                        VersionId: '1',
                        LastModified: '2021-10-04T21:46:49.157Z',
                        ETag: '1:3749f52bb326ae96782b42dc0a97b4c1',
                        Size: 1,
                        StorageClass: 'site1',
                    },
                ],
                DeleteMarkers: [],
            };
            lct2._getObjectVersions(bucketData, {}, 'Enabled', 0, fakeLogger,
                err => {
                    assert.ifError(err);
                    assert.strictEqual(lct2.bucketClearCalledWith, null);
                    assert.strictEqual(lct2.objectsClearCalledWith.bucketName, 'test-bucket');
                    assert(lct2.objectsClearCalledWith.uniqueObjectKeys.has('obj1'));
                    assert(lct2.objectsClearCalledWith.uniqueObjectKeys.has('obj2'));
                    assert(!lct2.objectsClearCalledWith.uniqueObjectKeys.has('obj3'));
                    done();
                });
        });
    });
});

