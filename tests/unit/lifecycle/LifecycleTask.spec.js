'use strict';

const assert = require('assert');
const async = require('async');
const sinon = require('sinon');
const { errors } = require('arsenal');
const { ValidLifecycleRules } = require('arsenal').models;

const LifecycleTask = require(
    '../../../extensions/lifecycle/tasks/LifecycleTask');
const LifecycleTaskV2 = require(
    '../../../extensions/lifecycle/tasks/LifecycleTaskV2');
const ActionQueueEntry = require('../../../lib/models/ActionQueueEntry');
const { LifecycleMetrics } = require('../../../extensions/lifecycle/LifecycleMetrics');
const fakeLogger = require('../../utils/fakeLogger');
const { withActiveSpan } = require('../../utils/withActiveSpan');
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
            ncvHeap: new Map(),
            lcOptions: timeOptions,
            log: fakeLogger,
            supportedRules: ValidLifecycleRules,
        }
    ),
};

describe('lifecycle task helper methods', () => {
    let lct;

    before(() => {
        lct = new LifecycleTask(lp);
    });

    afterEach(() => {
        sinon.restore();
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
                    '39383331323130353631303036363939393939395247303031202038363935392e3131',
                    IsLatest: true,
                    LastModified: '2018-04-04T23:16:46.000Z',
                },
                // LastModified matches with a delete marker
                {
                    Key: 'obj-1',
                    VersionId:
                    '39383331323130353632373431383939393939395247303031202038363935392e39',
                    IsLatest: false,
                    LastModified: '2018-04-04T23:16:44.000Z',
                },
                {
                    Key: 'obj-1',
                    VersionId:
                    '39383331323130353632393030393939393939395247303031202038363935392e38',
                    IsLatest: false,
                    LastModified: '2018-04-04T23:16:41.000Z',
                },
                {
                    Key: 'obj-1',
                    VersionId:
                    '39383331323130353634393037323939393939395247303031202038363935392e36',
                    IsLatest: false,
                    LastModified: '2018-04-04T23:16:32.000Z',
                },
            ];
            const dms = [
                {
                    Key: 'obj-1',
                    IsLatest: false,
                    VersionId:
                    '39383331323130353631353130353939393939395247303031202038363935392e3130',
                    LastModified: '2018-04-04T23:16:44.000Z',
                },
                {
                    Key: 'obj-1',
                    IsLatest: false,
                    VersionId:
                    '39383331323130353633303731373939393939395247303031202038363935392e37',
                    LastModified: '2018-04-04T23:16:34.000Z',
                },
            ];

            // Can only do this since I set VersionId in the expected order
            // Normally, when dealing with multiple objects, we wouldn't be able
            // to sort by just VersionId, since they could be intertwined.
            const expected = [...versions, ...dms].sort((a, b) => {
                if (a.VersionId < b.VersionId) {
                    return -1;
                }
                if (a.VersionId > b.VersionId) {
                    return 1;
                }
                return 0;
            });
            const { error, sortedList } = lct._mergeSortedVersionsAndDeleteMarkers(versions, dms, fakeLogger);
            assert.ifError(error);
            assert.deepStrictEqual(sortedList, expected);
        });

        it('should merge and sort the arrays if the modified dates match and the dm has the newest version ID', () => {
            const versions = [
                {
                    Key: 'obj-1',
                    VersionId:
                    '39383331323130353632373431383939393939395247303031202038363935392e39',
                    IsLatest: false,
                    LastModified: '2018-04-04T23:16:44.000Z',
                },
            ];
            const dms = [
                {
                    Key: 'obj-1',
                    IsLatest: false,
                    VersionId:
                    '39383331323130353631353130353939393939395247303031202038363935392e3130',
                    LastModified: '2018-04-04T23:16:44.000Z',
                },
            ];

            const expected = [dms[0], versions[0]];
            const { error, sortedList } = lct._mergeSortedVersionsAndDeleteMarkers(versions, dms, fakeLogger);
            assert.ifError(error);
            assert.deepStrictEqual(sortedList, expected);
        });

        it('should merge and sort the arrays ' +
        'if the modified dates match and the version has the newest versionId', () => {
            const versions = [
                {
                    Key: 'obj-1',
                    VersionId:
                    '39383331323130353631353130353939393939395247303031202038363935392e3130',
                    IsLatest: false,
                    LastModified: '2018-04-04T23:16:44.000Z',
                },
            ];
            const dms = [
                {
                    Key: 'obj-1',
                    IsLatest: false,
                    VersionId:
                    '39383331323130353632373431383939393939395247303031202038363935392e39',
                    LastModified: '2018-04-04T23:16:44.000Z',
                },
            ];

            const expected = [versions[0], dms[0]];
            const { error, sortedList } = lct._mergeSortedVersionsAndDeleteMarkers(versions, dms, fakeLogger);
            assert.ifError(error);
            assert.deepStrictEqual(sortedList, expected);
        });

        it('should merge and sort arrays that include a null version id version', () => {
            const versions = [
                {
                    Key: 'obj-1',
                    VersionId:
                    '39383331323130353632373431383939393939395247303031202038363935392e39',
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
                    '39383331323130353632393030393939393939395247303031202038363935392e38',
                    IsLatest: false,
                    LastModified: '2018-04-04T23:16:47.000Z',
                },
                {
                    Key: 'obj-1',
                    VersionId:
                    '39383331323130353633303731373939393939395247303031202038363935392e37',
                    IsLatest: false,
                    LastModified: '2018-04-04T23:16:46.000Z',
                },
                {
                    Key: 'obj-1',
                    VersionId:
                    '39383331323130353634393037323939393939395247303031202038363935392e36',
                    IsLatest: false,
                    LastModified: '2018-04-04T23:16:40.000Z',
                },
            ];

            const expected = [versions[0], dms[0], versions[1], dms[1], dms[2]];
            const { error, sortedList } = lct._mergeSortedVersionsAndDeleteMarkers(versions, dms, fakeLogger);

            assert.ifError(error);
            assert.deepStrictEqual(sortedList, expected);
        });

        it('should merge and sort arrays that include a short version id version', () => {
            const versions = [
                {
                    Key: 'beluga-projets-d/db/202303081330/belugaprojets.log',
                    VersionId: 'aJoN7z7tnjtR00000000001I4kqeIS4g',
                    IsLatest: false,
                    LastModified: '2023-03-08T12:30:23.000Z',
                },
                {
                    Key: 'beluga-projets-d/db/202304131430/mysql.sql.gz',
                    VersionId: 'aJnucN9uvwQv00000000001I4kqeIS4g',
                    IsLatest: false,
                    LastModified: '2023-04-13T12:30:21.000Z',
                },
            ];
            const dms = [
                {
                    Key: 'beluga-projets-d/db/202303081330/belugaprojets.log',
                    VersionId: 'aJoflc8UewLd00000000001I4kqeIS4g',
                    IsLatest: true,
                    LastModified: '2023-03-23T13:12:11.000Z',
                },
                {
                    Key: 'beluga-projets-d/db/202304131430/mysql.sql.gz',
                    VersionId: 'aJl0my3xBGJF00000000001I4kqeIS4g',
                    IsLatest: true,
                    LastModified: '2023-06-20T23:12:16.000Z',
                },
            ];

            const expected = [dms[0], versions[0], dms[1], versions[1]];
            const { error, sortedList } = lct._mergeSortedVersionsAndDeleteMarkers(versions, dms, fakeLogger);

            assert.ifError(error);
            assert.deepStrictEqual(sortedList, expected);
        });

        it('should return an error if invalid version id', () => {
            const versions = [
                {
                    Key: 'obj-1',
                    VersionId: 'invalid',
                    IsLatest: true,
                    LastModified: '2018-04-04T23:16:47.000Z',
                },
            ];
            const dms = [
                {
                    Key: 'obj-1',
                    VersionId:
                    '39383331323130353632393030393939393939395247303031202038363935392e38',
                    IsLatest: false,
                    LastModified: '2018-04-04T23:16:47.000Z',
                },
            ];

            const { error, sortedList } = lct._mergeSortedVersionsAndDeleteMarkers(versions, dms, fakeLogger);

            assert.deepStrictEqual(error, errors.InternalError);
            assert.deepStrictEqual(sortedList, null);
        });

        it('should return an error if invalid delete marker version id', () => {
            const versions = [
                {
                    Key: 'obj-1',
                    VersionId: '39383331323130353632393030393939393939395247303031202038363935392e38',
                    IsLatest: true,
                    LastModified: '2018-04-04T23:16:47.000Z',
                },
            ];
            const dms = [
                {
                    Key: 'obj-1',
                    VersionId: 'invalid',
                    IsLatest: false,
                    LastModified: '2018-04-04T23:16:47.000Z',
                },
            ];

            const { error, sortedList } = lct._mergeSortedVersionsAndDeleteMarkers(versions, dms, fakeLogger);

            assert.deepStrictEqual(error, errors.InternalError);
            assert.deepStrictEqual(sortedList, null);
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

        const assertNow = time => {
            assert(time, 'transitionTime is not set');
            const now = Date.now();
            assert.ok(time <= now && time >= now - 1000, 'transitionTime is not the current time');
        };

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
                assertNow(latestEntry.getAttribute('transitionTime'));
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
                assertNow(latestEntry.getAttribute('transitionTime'));
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
                assertNow(latestEntry.getAttribute('transitionTime'));
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
                assertNow(latestEntry.getAttribute('transitionTime'));
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

    describe('LifecycleTaskV2 scan context propagation', () => {
        let lifecycleTaskV2;

        const bucketData = {
            target: {
                owner: 'test-user',
                accountId: 'test-account',
                bucket: 'test-bucket',
            },
            contextInfo: {
                conductorScanId: 'scan-A',
                conductorScanStartTimestamp: 1700000000000,
            },
        };

        beforeEach(() => {
            lifecycleTaskV2 = new LifecycleTaskV2(lp);
        });

        afterEach(() => {
            sinon.restore();
        });

        it('should propagate scan context to current-version transition actions', done => {
            sinon.stub(lifecycleTaskV2, '_checkAndApplyExpirationRule').returns(false);
            sinon.stub(lifecycleTaskV2, '_isDeleteMarker').returns(false);
            sinon.stub(lifecycleTaskV2, '_applyTransitionRule')
                .callsFake((params, log, cb) => {
                    assert.strictEqual(params.bucketData.contextInfo.conductorScanId, 'scan-A');
                    assert.strictEqual(
                        params.bucketData.contextInfo.conductorScanStartTimestamp, 1700000000000);
                    cb();
                });

            lifecycleTaskV2._compareCurrent(bucketData, {
                Key: 'test-key',
                VersionId: 'test-version',
                ETag: '"test-etag"',
                LastModified: '2023-01-01T00:00:00.000Z',
            }, {
                Transition: {
                    Days: 1,
                    StorageClass: 'test-site',
                },
            }, fakeLogger, done);
        });

        it('should propagate scan context to expired delete-marker actions', () => {
            class LifecycleTaskV2Mock extends LifecycleTaskV2 {
                _sendObjectAction(entry, cb) {
                    this.latestEntry = entry;
                    return cb();
                }
            }
            lifecycleTaskV2 = new LifecycleTaskV2Mock(lp);

            lifecycleTaskV2._checkAndApplyEODMRule(bucketData, {
                Key: 'test-key',
                VersionId: 'test-version',
                LastModified: new Date(Date.now() - (2 * DAY)).toISOString(),
            }, {
                Expiration: { Days: 1 },
            }, fakeLogger);

            const context = lifecycleTaskV2.latestEntry.getContext();
            assert.strictEqual(context.conductorScanId, 'scan-A');
            assert.strictEqual(context.conductorScanStartTimestamp, 1700000000000);
        });

        it('should preserve V2 continuation requestId context', () => {
            const log = {
                ...fakeLogger,
                getSerializedUids: () => 'test-request-id',
            };
            const entry = lifecycleTaskV2._makeContinuationEntry(bucketData, log, {
                beforeDate: '2023-01-01T00:00:00.000Z',
                prefix: '',
                listType: 'current',
            });

            assert.strictEqual(entry.contextInfo.requestId, 'test-request-id');
            assert.strictEqual(entry.contextInfo.reqId, undefined);
            assert.strictEqual(entry.contextInfo.conductorScanId, 'scan-A');
            assert.strictEqual(
                entry.contextInfo.conductorScanStartTimestamp, 1700000000000);
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
                LastModified: new Date(testDate - (1 * DAY)).toISOString(),
                StorageClass: 'eu-west-1',
            },
            {
                Key: 'testkey',
                VersionId: '3',
                staleDate: new Date(testDate - (2 * DAY)).toISOString(),
                LastModified: new Date(testDate - (2 * DAY)).toISOString(),
                StorageClass: 'eu-west-2',
            },
            {
                Key: 'testkey',
                VersionId: '1',
                staleDate: new Date(testDate - (4 * DAY)).toISOString(),
                LastModified: new Date(testDate - (4 * DAY)).toISOString(),
                // no storage class, to "simulate" a delete marker
            },
            {
                Key: 'testkey',
                VersionId: '2',
                staleDate: new Date(testDate - (3 * DAY)).toISOString(),
                LastModified: new Date(testDate - (3 * DAY)).toISOString(),
                StorageClass: 'eu-west-4',
            },
            {
                Key: 'testkey',
                VersionId: '0',
                staleDate: new Date(testDate - (5 * DAY)).toISOString(),
                LastModified: new Date().toISOString(),
                StorageClass: 'eu-west-5',
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

                    const expectedStorageClass = expectedEntries[idx].StorageClass || '-delete-marker-';

                    assert.strictEqual(latestEntry.getActionType(), 'deleteObject');
                    assert.deepStrictEqual(
                        latestEntry.getAttribute('target'), expectedTarget);
                    assert.deepStrictEqual(
                        latestEntry.getAttribute('details.dataStoreName'), expectedStorageClass);

                    // `details.lastModified` must not be set for NCV expiration, as it is used to
                    // check that the master version has not changed. This check is not relevant for
                    // NCV (if anything, we may check that the current version is actually newer
                    // than this one), and in particular may fail if current revision is a delete
                    // marker
                    assert.deepStrictEqual(
                        latestEntry.getAttribute('details.lastModified'), undefined);
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

        it('should populate and return null if heap has space', () => {
            const ret = lct2._ncvHeapAdd('testbucket', rules, versions[0]);
            assert.strictEqual(ret, null);
        });

        it('should populate and return oldest items from if heap is at capacity', () => {
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
            const ruleId = 'rule_name';
            const rules = {
                NoncurrentVersionExpiration: {
                    ID: ruleId,
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
            assert(lct2.ncvHeap.get(b1).get(version3.Key).has(ruleId));
            assert.strictEqual(lct2.ncvHeap.get(b1).get(version3.Key).get(ruleId).size, 1);

            assert(lct2.ncvHeap.has(b2));
            assert(lct2.ncvHeap.get(b2).has(version1.Key));
            assert(lct2.ncvHeap.get(b2).get(version1.Key).has(ruleId));
            assert.strictEqual(lct2.ncvHeap.get(b2).get(version1.Key).get(ruleId).size, 1);
            assert(lct2.ncvHeap.get(b2).has(version2.Key));
            assert(lct2.ncvHeap.get(b2).get(version2.Key).has(ruleId));
            assert.strictEqual(lct2.ncvHeap.get(b2).get(version2.Key).get(ruleId).size, 1);
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
            const ruleId = 'rule_name';
            const rules = {
                NoncurrentVersionExpiration: {
                    ID: ruleId,
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
            assert(lct2.ncvHeap.get(b2).get(version1.Key).has(ruleId));
            assert.strictEqual(lct2.ncvHeap.get(b2).get(version1.Key).get(ruleId).size, 1);
            assert(lct2.ncvHeap.get(b2).has(version2.Key));
            assert(lct2.ncvHeap.get(b2).get(version2.Key).has(ruleId));
            assert.strictEqual(lct2.ncvHeap.get(b2).get(version2.Key).get(ruleId).size, 1);
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

        it('should return an error if version id is invalid', done => {
            lct2.listResponse = {
                IsTruncated: false,
                Versions: [
                    {
                        Key: 'obj-1',
                        VersionId: '39383331323130353632393030393939393939395247303031202038363935392e38',
                        IsLatest: true,
                        LastModified: '2018-04-04T23:16:47.000Z',
                    },
                ],
                DeleteMarkers: [
                    {
                        Key: 'obj-1',
                        VersionId: 'invalid',
                        IsLatest: false,
                        LastModified: '2018-04-04T23:16:47.000Z',
                    },
                ],
            };
            lct2._getObjectVersions(bucketData, {}, 'Enabled', 0, fakeLogger,
                err => {
                    assert.deepStrictEqual(err, errors.InternalError);
                    done();
                });
        });
    });

    describe('_retryEntry', () => {
        it('should not retry on success', done => {
            const lct = new LifecycleTask(lp);
            const action = sinon.stub();
            action.callsArg(0);

            lct._retryEntry({
                logFields: {},
                log: fakeLogger,
                actionFunc: action,
            }, err => {
                assert.ifError(err);
                assert(action.calledOnce);
                done();
            });
        });

        it('should not retry if error is not retryable', done => {
            const lct = new LifecycleTask(lp);
            const action = sinon.stub();
            action.callsArgWith(0, { code: 'NotRetryable', message: 'test' });

            lct._retryEntry({
                logFields: {},
                log: fakeLogger,
                actionFunc: action,
            }, err => {
                assert.ifError(err);
                assert(action.calledOnce);
                done();
            });
        });

        it('should retry retryable errors up to 5 times', done => {
            const lct = new LifecycleTask(lp);
            lct.retryParams.backoff.min = 10;

            const action = sinon.stub();
            action.callsArgWith(0, { code: 'NotRetryable', message: 'test', retryable: true });

            lct._retryEntry({
                logFields: {},
                log: fakeLogger,
                actionFunc: action,
            }, err => {
                assert.ifError(err);
                assert.equal(action.callCount, 5);
                done();
            });
        });

        it('should stop retrying after reaching 400 total retries', done => {
            const lct = new LifecycleTask(lp);
            lct.retryParams.backoff.min = 1;

            const action = sinon.stub();
            action.callsArgWith(0, { code: 'NotRetryable', message: 'test', retryable: true });

            let count = 0;
            async.whilst(
                () => count++ < 102,
                cb => lct._retryEntry({
                    logFields: { count, totalRetries: lct._totalRetries },
                    log: fakeLogger,
                    actionFunc: action,
                }, cb),
                err => {
                    assert.ifError(err);
                    // 5 attempts x 100, last 2 calls will not be retried (reached max)
                    assert.equal(action.callCount, 502);
                    done();
                }
            );
        });
    });

    describe('processBucketEntry', () => {
        it('should snapshot datamover topic offsets when transition is supported', done => {
            const s3target = {
                // deliberately failing to avoid going through all the logic
                send: sinon.stub().rejects(errors.NoSuchBucket),
            };
            const backbeatMetadataProxy = sinon.stub();
            const bucketData = {
                target: {
                    owner: 'test-user',
                    bucket: 'test-bucket',
                },
                details: {},
            };
            const bucketLCRules = {
                Rules: [],
            };
            const snapshot = sinon.stub(lct, '_snapshotDataMoverTopicOffsets').returns();
            lct.setSupportedRules(ValidLifecycleRules);
            lct.processBucketEntry(bucketLCRules, bucketData, s3target, backbeatMetadataProxy, 0, err => {
                assert.deepEqual(err, errors.NoSuchBucket);
                assert(snapshot.calledOnce);
                done();
            });
        });

        it('should not snapshot datamover topic offsets when transition is not supported', done => {
            const s3target = {
                // deliberately failing to avoid going through all the logic
                send: sinon.stub().rejects(errors.NoSuchBucket),
            };
            const backbeatMetadataProxy = sinon.stub();
            const bucketData = {
                target: {
                    owner: 'test-user',
                    bucket: 'test-bucket',
                },
                details: {},
            };
            const bucketLCRules = {
                Rules: [],
            };
            const snapshot = sinon.stub(lct, '_snapshotDataMoverTopicOffsets').returns();
            lct.setSupportedRules([
                'Expiration',
                'NoncurrentVersionExpiration',
                'AbortIncompleteMultipartUpload',
            ]);
            lct.processBucketEntry(bucketLCRules, bucketData, s3target, backbeatMetadataProxy, 0, err => {
                assert.deepEqual(err, errors.NoSuchBucket);
                assert(snapshot.notCalled);
                done();
            });
        });
    });

    describe('_getTransitionActionEntry', () => {
        let lifecycleTask;

        const testParams = {
            bucket: 'test-bucket',
            owner: 'test-owner',
            objectKey: 'test-key',
            versionId: 'test-version-id',
            eTag: '"test-etag"',
            lastModified: '2023-01-01T00:00:00.000Z',
            site: 'test-site',
            accountId: 'test-account-id',
            transitionTime: Date.now(),
            bucketData: {
                target: {
                    bucket: 'test-bucket',
                    owner: 'test-owner',
                    accountId: 'test-account-id',
                },
                contextInfo: {
                    conductorScanId: 'scan-A',
                    conductorScanStartTimestamp: 1700000000000,
                },
            },
        };

        before(() => {
            class LifecycleTaskMock extends LifecycleTask {
                constructor(lp) {
                    super(lp);
                    this.transitionTasksTopic = 'test-transition-topic';
                    this.headLocationResponse = null;
                    this.headLocationError = null;
                }

                _headLocation(params, locations, log, cb) {
                    if (this.headLocationError) {
                        return cb(this.headLocationError);
                    }
                    return cb(null, this.headLocationResponse);
                }

                setHeadLocationResponse(response) {
                    this.headLocationResponse = response;
                    this.headLocationError = null;
                }

                setHeadLocationError(error) {
                    this.headLocationError = error;
                    this.headLocationResponse = null;
                }
            }
            lifecycleTask = new LifecycleTaskMock(lp);
        });

        afterEach(() => {
            sinon.restore();
        });

        it('should create transition entry for object that can be unconditionally garbage collected', done => {
            const mockObjectMD = {
                getDataStoreName: () => 'local-site',
                getDataStoreVersionId: () => 'version-123',
                getContentLength: () => 1024,
                getUserMetadata: () => null,
            };

            sinon.stub(lifecycleTask, '_canUnconditionallyGarbageCollect').returns(true);

            lifecycleTask._getTransitionActionEntry(testParams, mockObjectMD, fakeLogger, (err, entry) => {
                assert.ifError(err);
                
                assert.strictEqual(entry.getActionType(), 'copyLocation');
                
                const target = entry.getAttribute('target');
                assert.strictEqual(target.bucket, testParams.bucket);
                assert.strictEqual(target.key, testParams.objectKey);
                assert.strictEqual(target.version, testParams.versionId);
                assert.strictEqual(target.eTag, testParams.eTag);
                assert.strictEqual(target.lastModified, testParams.lastModified);
                assert.strictEqual(target.owner, testParams.owner);
                assert.strictEqual(target.accountId, testParams.accountId);
                assert.strictEqual(target.attempt, undefined);
                
                assert.strictEqual(entry.getAttribute('toLocation'), testParams.site);
                
                const source = entry.getAttribute('source');
                assert.strictEqual(source.bucket, testParams.bucket);
                assert.strictEqual(source.objectKey, testParams.objectKey);
                assert.strictEqual(source.storageClass, 'local-site');
                assert.strictEqual(source.lastModified, undefined);
                
                const context = entry.getContext();
                assert.strictEqual(context.origin, 'lifecycle');
                assert.strictEqual(context.ruleType, 'transition');
                
                done();
            });
        });

        it('should create transition entry with attempt number from user metadata', done => {
            const mockObjectMD = {
                getDataStoreName: () => 'local-site',
                getDataStoreVersionId: () => 'version-123',
                getContentLength: () => 1024,
                getUserMetadata: () => JSON.stringify({
                    'x-amz-meta-scal-s3-transition-attempt': '3'
                }),
            };

            sinon.stub(lifecycleTask, '_canUnconditionallyGarbageCollect').returns(true);

            lifecycleTask._getTransitionActionEntry(testParams, mockObjectMD, fakeLogger, (err, entry) => {
                assert.ifError(err);
                assert.strictEqual(entry.getAttribute('target.attempt'), 3);
                done();
            });
        });

        it('should create transition entry for object requiring head location check', done => {
            const mockObjectMD = {
                getDataStoreName: () => 'aws-location',
                getDataStoreVersionId: () => null,
                getContentLength: () => 2048,
                getUserMetadata: () => null,
                getLocation: () => [{ name: 'aws-location', dataStoreVersionId: null }],
            };

            const expectedLastModified = '2023-01-01T12:00:00.000Z';
            lifecycleTask.setHeadLocationResponse(expectedLastModified);

            sinon.stub(lifecycleTask, '_canUnconditionallyGarbageCollect').returns(false);

            lifecycleTask._getTransitionActionEntry(testParams, mockObjectMD, fakeLogger, (err, entry) => {
                assert.ifError(err);
                assert.strictEqual(entry.getActionType(), 'copyLocation');
                assert.strictEqual(entry.getAttribute('source.lastModified'), expectedLastModified);
                done();
            });
        });

        it('should return error when head location fails', done => {
            const mockObjectMD = {
                getDataStoreName: () => 'aws-location',
                getDataStoreVersionId: () => null,
                getContentLength: () => 2048,
                getUserMetadata: () => null,
                getLocation: () => [{ name: 'aws-location', dataStoreVersionId: null }],
            };

            const expectedError = new Error('Head location failed');
            lifecycleTask.setHeadLocationError(expectedError);

            sinon.stub(lifecycleTask, '_canUnconditionallyGarbageCollect').returns(false);

            lifecycleTask._getTransitionActionEntry(testParams, mockObjectMD, fakeLogger, (err, entry) => {
                assert.deepStrictEqual(err, expectedError);
                assert.strictEqual(entry, undefined);
                done();
            });
        });
    });

    describe('_sendObjectAction', () => {
        it('should emit trigger metrics with the entry location', done => {
            const lifecycleTask = new LifecycleTask(lp);
            const sentEntries = [];
            lifecycleTask.objectTasksTopic = 'object-topic';
            lifecycleTask.circuitBreakers = {
                tripped: sinon.stub().returns(false),
            };
            lifecycleTask.producer = {
                sendToTopic: (topic, entries, cb) => {
                    sentEntries.push({ topic, entries });
                    cb();
                },
            };
            const triggeredMetric = sinon.stub(LifecycleMetrics, 'onLifecycleTriggered');

            const entry = ActionQueueEntry.create('deleteObject')
                .setAttribute('target.owner', 'test-owner')
                .setAttribute('target.bucket', 'test-bucket')
                .setAttribute('target.accountId', 'test-account')
                .setAttribute('target.key', 'test-key')
                .setAttribute('details.dataStoreName', 'us-east-1')
                .setAttribute('transitionTime', Date.now() - HOUR);
            lifecycleTask._sendObjectAction(entry, err => {
                assert.ifError(err);
                assert.strictEqual(entry.getAttribute('details.dataStoreName'), 'us-east-1');
                assert.strictEqual(lifecycleTask.circuitBreakers.tripped.firstCall.args[1], 'us-east-1');
                assert.strictEqual(triggeredMetric.firstCall.args[3], 'us-east-1');
                assert.strictEqual(sentEntries.length, 1);
                done();
            });
        });
    });

    describe('_compareObject location metrics', () => {
        // With the x-amz-scal-archive-info request header, CloudServer's
        // HeadObject always returns the real storage class: the preserved
        // cold class for cold and restored objects, the data-store name
        // otherwise (never the STANDARD placeholder the listing may carry).
        [
            {
                desc: 'hot object on the default location',
                listedStorageClass: 'STANDARD',
                headStorageClass: 'us-east-1',
            },
            {
                desc: 'hot object on a non-default location',
                listedStorageClass: 'site-azure',
                headStorageClass: 'site-azure',
            },
            {
                desc: 'cold object',
                listedStorageClass: 'location-dmf-v1',
                headStorageClass: 'location-dmf-v1',
            },
            {
                // restored objects keep the cold class in
                // x-amz-storage-class even though the restored copy lives
                // on a warm location
                desc: 'restored cold object',
                listedStorageClass: 'location-dmf-v1',
                headStorageClass: 'location-dmf-v1',
            },
        ].forEach(({ desc, listedStorageClass, headStorageClass }) => {
            it(`should queue the expiration of a ${desc} with the ` +
            'archive-info storage class', done => {
                class LifecycleTaskMock extends LifecycleTask {
                    _sendObjectAction(entry, cb) {
                        this.latestEntry = entry;
                        return cb();
                    }
                }

                const lifecycleTask = new LifecycleTaskMock(lp);
                lifecycleTask.s3target = {
                    send: sinon.stub().resolves({
                        LastModified: new Date().toISOString(),
                        StorageClass: headStorageClass,
                    }),
                };

                const bucketData = {
                    target: {
                        owner: 'test-owner',
                        bucket: 'test-bucket',
                        accountId: 'test-account',
                    },
                    details: {},
                };
                const rules = {
                    Expiration: {
                        Date: new Date(Date.now() - DAY),
                    },
                };
                const listedObject = Object.assign({}, OBJECT, {
                    StorageClass: listedStorageClass,
                });

                lifecycleTask._compareObject(bucketData, listedObject, rules, fakeLogger, err => {
                    assert.ifError(err);
                    assert.strictEqual(lifecycleTask.s3target.send.calledOnce, true);
                    const command = lifecycleTask.s3target.send.firstCall.args[0];
                    assert(command.middlewareStack.identify()
                        .includes('attachArchiveInfoHeader - build'));
                    assert.strictEqual(
                        lifecycleTask.latestEntry.getAttribute('details.dataStoreName'),
                        headStorageClass
                    );
                    done();
                });
            });
        });
    });

});

describe('LifecycleTask trace-context propagation', () => {
    it('_sendBucketEntry stamps traceparent headers when a span is active', done => {
        let captured;
        const self = {
            bucketTasksTopic: 'bucket-tasks',
            producer: {
                sendToTopic: (topic, entries, cb) => {
                    captured = entries[0];
                    cb(null);
                },
            },
        };
        withActiveSpan(() => {
            LifecycleTask.prototype._sendBucketEntry.call(self, { foo: 1 }, err => {
                assert.ifError(err);
                assert(captured.headers.some(h => h.traceparent));
                done();
            });
        });
    });

    it('_sendObjectAction stamps traceparent headers when a span is active', done => {
        let captured;
        const self = {
            objectTasksTopic: 'object-tasks',
            log: fakeLogger,
            circuitBreakers: { tripped: () => false },
            producer: {
                sendToTopic: (topic, entries, cb) => {
                    captured = entries[0];
                    cb(null);
                },
            },
        };
        const entry = {
            getAttribute: key => (key === 'transitionTime' ? Date.now() : undefined),
            getActionType: () => 'expiration',
            toKafkaMessage: () => JSON.stringify({ foo: 1 }),
        };
        withActiveSpan(() => {
            LifecycleTask.prototype._sendObjectAction.call(self, entry, err => {
                assert.ifError(err);
                assert(captured.headers.some(h => h.traceparent));
                done();
            });
        });
    });
});
