const assert = require('assert');
const uuid = require('uuid/v4');
const util = require('util');

const { errors } = require('arsenal');
const Logger = require('werelogs').Logger;

const LifecycleTask = require('../../../extensions/lifecycle' +
    '/tasks/LifecycleTask');
const Rule = require('../../utils/Rule');

/*
    NOTE:
    When using CURRENT, running the lifecycle tasks will internally use
    `Date.now` to compare dates. This means that CURRENT date usage will most
    likely be in the past.
    To avoid flakiness, I will be setting the day of CURRENT back 2 day to
    avoid any flakiness.
    When comparing rules, if I set a rule that says "Days: 1", then any objects
    using CURRENT as LastModified should pass. To avoid flakiness, set a rule
    that says "Days: 3" to have any objects using CURRENT as LastModified to
    not pass.
*/
// current date
const CURRENT = new Date();
CURRENT.setDate(CURRENT.getDate() - 2);
// 5 days prior to currentDate
const PAST = new Date(CURRENT);
PAST.setDate(PAST.getDate() - 5);
// 5 days after currentDate
const FUTURE = new Date(CURRENT);
FUTURE.setDate(FUTURE.getDate() + 5);

const OWNER = 'testOwner';

// NOTE: Since all S3 calls are only 'GET's, I decided to just go for mock
//  S3 that defines all buckets and their conditions
class S3Mock {
    constructor() {
        this.overrideMax = undefined;

        this.s3Storage = {
            'test-expire': {
                Objects: [
                    {
                        Key: 'expire-1',
                        LastModified: PAST,
                        StorageClass: 'STANDARD',
                        Marker: uuid().replace(/-/g, ''),
                        //
                        TagSet: [],
                    },
                    {
                        Key: 'expire-2',
                        LastModified: CURRENT,
                        StorageClass: 'STANDARD',
                        Marker: uuid().replace(/-/g, ''),
                        //
                        TagSet: [
                            { Key: 'key1', Value: 'value1' },
                        ],
                    },
                    {
                        Key: 'expire-3',
                        LastModified: PAST,
                        StorageClass: 'STANDARD',
                        Marker: uuid().replace(/-/g, ''),
                        //
                        TagSet: [
                            { Key: 'key1', Value: 'value1' },
                            { Key: 'key2', Value: 'value2' },
                        ],
                    },
                    {
                        Key: 'expire-4',
                        LastModified: FUTURE,
                        StorageClass: 'STANDARD',
                        Marker: uuid().replace(/-/g, ''),
                        //
                        TagSet: [
                            { Key: 'key2', Value: 'value2' },
                            { Key: 'key1', Value: 'value1' },
                        ],
                    },
                    {
                        Key: 'expire-5',
                        LastModified: FUTURE,
                        StorageClass: 'STANDARD',
                        Marker: uuid().replace(/-/g, ''),
                        //
                        TagSet: [
                            { Key: 'key2', Value: 'value2' },
                        ],
                    },
                ],
            },
            // 'test-versioned-bucket': {
            //     Versioning: { Status: 'Enabled' },
            //     Versions: [
            //         {
            //             Key: 'a-key',
            //             LastModified: PAST,
            //             IsLatest: true,
            //             StorageClass: 'STANDARD',
            //             VersionId: uuid().replace(/-/g, ''),
            //             //
            //             KeyMarker: uuid().replace(/-/g, ''),
            //             TagSet: [],
            //         },
            //     ],
            // },
            // 'test-mpu': {
            //     Uploads: [
            //         {
            //             Initiated: PAST,
            //             Key: 'first-upload',
            //             StorageClass: 'STANDARD',
            //             UploadId: uuid().replace(/-/g, ''),
            //             //
            //             KeyMarker: uuid().replace(/-/g, ''),
            //         },
            //     ],
            // },
        };
    }

    _noop() {}

    getObjectCount(bucketName) {
        return this.s3Storage[bucketName].Objects.length;
    }

    /**
     * head object operation on either a version or object
     * @param {Object} params - parameters
     * @param {string} params.Bucket - bucket name
     * @param {string} params.Key - key
     * @param {string} [params.VersionId] - optional version id
     * @return {Object} wrapper to mock `send`
     */
    headObject(params) {
        assert(params.Bucket);
        assert(params.Key);

        return {
            send: this._headObjectHandler.bind(this, params),
            on: this._noop,
            httpRequest: { headers: {} },
        };
    }

    // Currently only care about returning `data.LastModified`
    _headObjectHandler(params, cb) {
        const bucket = this.s3Storage[params.Bucket];
        if (!bucket) {
            return cb(errors.NoSuchBucket);
        }
        if (params.VersionId) {
            const version = bucket.Versions.find(v =>
                v.VersionId === params.VersionId && v.Key === params.Key);
            if (!version) {
                return cb(errors.ObjNotFound);
            }

            const res = Object.assign({}, version);
            delete res.TagSet;
            return cb(null, res);
        }
        const obj = bucket.Objects.find(o => o.Key === params.Key);
        if (!obj) {
            return cb(errors.ObjNotFound);
        }

        const res = Object.assign({}, obj);
        delete res.TagSet;
        return cb(null, res);
    }

    /**
     * get bucket versioning
     * @param {Object} params - parameters
     * @param {string} params.Bucket - bucket name
     * @return {Object} wrapper to mock `send`
     */
    getBucketVersioning(params) {
        assert(params.Bucket);

        return {
            send: this._getBucketVersioningHandler.bind(this, params),
            on: this._noop,
            httpRequest: { headers: {} },
        };
    }

    _getBucketVersioningHandler(params, cb) {
        const bucketVersioning = this.s3Storage[params.Bucket].Versioning;
        return cb(null, bucketVersioning || {});
    }

    /**
     * get an object or a versions tags
     * @param {Object} params - parameters
     * @param {string} params.Bucket - bucket name
     * @param {string} params.Key - key
     * @param {string} [params.VersionId] - optional version id
     * @return {Object} wrapper to mock `send`
     */
    getObjectTagging(params) {
        assert(params.Bucket);
        assert(params.Key);

        return {
            send: this._getObjectTaggingHandler.bind(this, params),
            on: this._noop,
            httpRequest: { headers: {} },
        };
    }

    _getObjectTaggingHandler(params, cb) {
        const bucket = this.s3Storage[params.Bucket];
        if (!bucket) {
            return cb(errors.NoSuchBucket);
        }
        if (params.VersionId) {
            if (!bucket.Versions) {
                // TODO: verify this logic. Return empty array? or error?
                return cb(null, { TagSet: [] });
            }
            const version = bucket.Versions.find(v =>
                v.VersionId === params.VersionId && v.Key === params.Key);
            if (!version) {
                return cb(errors.ObjNotFound);
            }

            const res = { TagSet: version.TagSet };
            return cb(null, res);
        }
        const obj = bucket.Objects.find(o => o.Key === params.Key);
        if (!obj) {
            return cb(errors.ObjNotFound);
        }

        const res = { TagSet: obj.TagSet };
        return cb(null, res);
    }

    /**
     * get all objects from bucket for given key
     * @param {Object} params - parameters
     * @param {string} params.Bucket - bucket name
     * @param {string} params.Key - key
     * @return {Object} wrapper to mock `send`
     */
    listObjects(params) {
        assert(params.Bucket);

        // Override default MaxKeys from LifecycleTask
        const newParams = params;
        if (this.overrideMax) {
            newParams.MaxKeys = this.overrideMax;
        }

        return {
            send: this._listObjectsHandler.bind(this, newParams),
            on: this._noop,
            httpRequest: { headers: {} },
        };
    }

    _listObjectsHandler(params, cb) {
        const bucket = this.s3Storage[params.Bucket];
        if (!bucket) {
            return cb(errors.NoSuchBucket);
        }

        const bucketObjs = bucket.Objects;
        if (!bucketObjs || bucketObjs.length === 0) {
            const res = { Contents: [] };
            return cb(null, res);
        }

        let markerIdx = 0;
        let maxIdx = params.MaxKeys;
        if (params.Marker) {
            markerIdx = bucketObjs.findIndex(o => o.Marker === params.Marker);
            maxIdx = markerIdx + params.MaxKeys;
        }

        const IsTruncated = bucketObjs.length > maxIdx;
        const Contents = bucketObjs.slice(markerIdx, maxIdx).map(o => {
            const newObj = Object.assign({}, o);
            delete newObj.TagSet;
            return newObj;
        });
        const res = {
            Contents,
            IsTruncated,
            NextMarker: IsTruncated ? this._findNextMarker(bucketObjs,
                Contents[Contents.length - 1].Marker, 'Marker') : null,
        };
        return cb(null, res);
    }

    /**
     * get all versions from bucket
     * @param {Object} params - parameters
     * @param {string} params.Bucket - bucket name
     * @param {string} [params.KeyMarker] - optional key marker
     * @param {string} [params.VersionIdMarker] - required if key marker exists
     * @return {Object} wrapper to mock `send`
     */
    listObjectVersions(params) {
        assert(params.Bucket);
        if (params.VersionIdMarker || params.KeyMarker) {
            assert(params.VersionIdMarker && params.KeyMarker);
        }

        // Override default MaxKeys from LifecycleTask
        const newParams = params;
        if (this.overrideMax) {
            newParams.MaxKeys = this.overrideMax;
        }

        return {
            send: this._listObjectVersionsHandler.bind(this, newParams),
            on: this._noop,
            httpRequest: { headers: {} },
        };
    }

    _listObjectVersionsHandler(params, cb) {
        const bucket = this.s3Storage[params.Bucket];
        if (!bucket) {
            return cb(errors.NoSuchBucket);
        }

        const bucketVersions = bucket.Versions;
        if (!bucketVersions || bucketVersions.length === 0) {
            const res = { Versions: [] };
            return cb(null, res);
        }

        let markerIdx = 0;
        let maxIdx = params.MaxKeys;
        if (params.KeyMarker) {
            markerIdx = bucketVersions.findIndex(v =>
                v.KeyMarker === params.KeyMarker &&
                v.VersionId === params.VersionIdMarker);
            if (markerIdx === -1) {
                // wrong keymarker / versionidmarker combo
                process.stdout.write('ALERT: Error in list object versions');
                assert(false);
            }
            maxIdx = markerIdx + params.MaxKeys;
        }

        const IsTruncated = bucketVersions.length > maxIdx;
        const Versions = bucketVersions.slice(markerIdx, maxIdx).map(v => {
            const newVer = Object.assign({}, v);
            delete newVer.TagSet;
            delete newVer.KeyMarker;
            return newVer;
        });
        const res = {
            Versions,
            IsTruncated,
            NextKeyMarker: IsTruncated ? this._findNextMarker(bucketVersions,
                Versions[Versions.length - 1].KeyMarker, 'KeyMarker') : null,
            NextVersionIdMarker: IsTruncated ? this._findNextMarker(
                bucketVersions, Versions[Versions.length - 1].VersionId,
                'VersionId') : null,
        };
        return cb(null, res);
    }

    /**
     * get all mpus from bucket
     * @param {Object} params - parameters
     * @param {string} params.Bucket - bucket name
     * @param {string} [params.KeyMarker] - optional key marker
     * @param {string} [params.UploadIdMarker] - required if key marker exists
     * @return {Object} wrapper to mock `send`
     */
    listMultipartUploads(params) {
        assert(params.Bucket);
        if (params.KeyMarker || params.UploadIdMarker) {
            assert(params.KeyMarker && params.UploadIdMarker);
        }

        // Override default MaxKeys from LifecycleTask
        const newParams = params;
        if (this.overrideMax) {
            newParams.MaxKeys = this.overrideMax;
        }

        return {
            send: this._listMultipartUploadsHandler.bind(this, newParams),
            on: this._noop,
            httpRequest: { headers: {} },
        };
    }

    _listMultipartUploadsHandler(params, cb) {
        process.stdout.write(util.inspect(params));
        const bucket = this.s3Storage[params.Bucket];
        if (!bucket) {
            return cb(errors.NoSuchBucket);
        }

        const bucketUploads = bucket.Uploads;
        if (!bucketUploads || bucketUploads.length === 0) {
            const res = { Uploads: [] };
            return cb(null, res);
        }

        let markerIdx = 0;
        let maxIdx = params.MaxUploads;
        if (params.KeyMarker) {
            markerIdx = bucketUploads.findIndex(u =>
                u.KeyMarker === params.KeyMarker &&
                u.UploadId === params.UploadIdMarker);
            if (markerIdx === -1) {
                // wrong keymarker / uploadidmarker combo
                process.stdout.write('ALERT: Error in list multipart uploads');
                assert(false);
            }
            maxIdx = markerIdx + params.MaxUploads;
        }

        const IsTruncated = bucketUploads.length > maxIdx;
        const Uploads = bucketUploads.slice(markerIdx, maxIdx).map(u => {
            const upload = Object.assign({}, u);
            delete upload.KeyMarker;
            return upload;
        });
        const res = {
            Uploads,
            IsTruncated,
            NextKeyMarker: IsTruncated ? this._findNextMarker(bucketUploads,
                Uploads[Uploads.length - 1].KeyMarker, 'KeyMarker') : null,
            NextUploadIdMarker: IsTruncated ? this._findNextMarker(
                bucketUploads, Uploads[Uploads.length - 1].UploadId,
                'UploadId') : null,
        };
        return cb(null, res);
    }

    // helper methods //

    _findNextMarker(objs, marker, id) {
        const curObjIdx = objs.findIndex(o => o[id] === marker);

        if (curObjIdx === -1 || curObjIdx === objs.length - 1) {
            // error in test
            // Either object should not be `IsTruncated` or
            // `Marker` cannot be found in list of objects
            process.stdout.write('ALERT: Error in test functionality\n');
            assert(false);
        }
        return objs[curObjIdx + 1][id];
    }

    /**
     * set max keys or parts
     * @param {number} max - max keys or parts in a listing
     * @return {undefined}
     */
    setMax(max) {
        this.overrideMax = max;
    }

    resetMax() {
        this.overrideMax = undefined;
    }
}

class LifecycleProducerMock {
    constructor() {
        this._log = new Logger('LifecycleProducer:test:LifecycleProducerMock');

        // TODO: only added current working features
        this._lcConfig = {
            rules: {
                expiration: { enabled: true },
                noncurrentVersionExpiration: { enabled: true },
                abortIncompleteMultipartUpload: { enabled: true },
                // below are features not implemented yet
                transition: { enabled: false },
                noncurrentVersionTransition: { enabled: false },
            },
        };

        this.sendBucketEntry = null;
        this.sendObjectEntry = null;

        this.sendCount = {
            bucket: 0,
            object: 0,
        };
        this.entries = {
            bucket: [],
            object: [],
        };
    }

    setBucketFxn(fxn, cb) {
        this.sendBucketEntry = fxn;
        cb();
    }

    setObjectFxn(fxn, cb) {
        this.sendObjectEntry = fxn;
        cb();
    }

    incrementCount(type) {
        this.sendCount[type]++;
    }

    addEntry(type, entry) {
        this.entries[type].push(entry);
    }

    getCount() {
        return this.sendCount;
    }

    getEntries() {
        return this.entries;
    }

    disableRule(rule) {
        if (this._lcConfig.rules[rule]) {
            this._lcConfig.rules[rule].enabled = false;
        }
    }

    enableRule(rule) {
        if (this._lcConfig.rules[rule]) {
            this._lcConfig.rules[rule].enabled = true;
        }
    }

    getStateVars() {
        return {
            sendBucketEntry: this.sendBucketEntry,
            sendObjectEntry: this.sendObjectEntry,
            removeBucketFromQueued: () => {},
            enabledRules: this._lcConfig.rules,
            log: this._log,
        };
    }

    reset() {
        this.sendCount = {
            bucket: 0,
            object: 0,
        };
        this.entries = {
            bucket: [],
            object: [],
        };
        this.sendBucketEntry = null;
        this.sendObjectEntry = null;
    }
}


/*
    To override MaxKeys or MaxUploads, set `testMaxKeys` or `testMaxUploads`
    respectively in params.
*/

describe('lifecycle producer functional tests with mocking', () => {
    let lcp;
    let lcTask;
    let s3mock;

    before(() => {
        lcp = new LifecycleProducerMock();
        s3mock = new S3Mock();
    });

    // Example lifecycle configs
    // {
    //     "Filter": {
    //         "And": {
    //             "Prefix": "myprefix",
    //             "Tags": [
    //                 {
    //                     "Value": "value1",
    //                     "Key": "tag1"
    //                 },
    //                 {
    //                     "Value": "value2",
    //                     "Key": "tag2"
    //                 }
    //             ]
    //         }
    //     },
    //     "Status": "Enabled",
    //     "Expiration": {
    //         "Days": 5
    //     },
    //     "ID": "rule1"
    // },

    describe('non-versioned bucket tests', () => {
        before(() => {
            lcp.setBucketFxn((entry, cb) => {
                lcp.incrementCount('bucket');
                lcp.addEntry('bucket', entry);

                return cb(null, true);
            }, () => {});
            lcp.setObjectFxn((entry, cb) => {
                lcp.incrementCount('object');
                lcp.addEntry('object', entry.target.key);

                // can assert entry stuff
                return cb(null, true);
            }, () => {});
            lcTask = new LifecycleTask(lcp);
        });

        afterEach(() => {
            lcp.reset();
            s3mock.resetMax();
        });

        [
            // basic expire with Date rule, no pagination, no tags
            {
                message: 'should verify that EXPIRED objects are sent to ' +
                    'object kafka topic with no pagination, no tags',
                bucketLCRules: [
                    new Rule().addID('task-1').addExpiration('Date', PAST)
                        .build(),
                ],
                bucketEntry: {
                    action: 'testing',
                    target: {
                        bucket: 'test-expire',
                        owner: OWNER,
                    },
                    details: {},
                },
                expected: {
                    objects: ['expire-1', 'expire-2', 'expire-3',
                        'expire-4', 'expire-5'],
                    bucketCount: 0,
                    objectCount: 5,
                },
            },
            // basic expire with 1 Day rule, pagination, tagging part 1
            {
                message: 'should verify that EXPIRED objects are sent to ' +
                    'object kafka topic with pagination, with tags: rule 1',
                bucketLCRules: [
                    new Rule().addID('task-1').addExpiration('Days', 1)
                        .addTag('key1', 'value1').build(),
                ],
                bucketEntry: {
                    action: 'testing',
                    target: {
                        bucket: 'test-expire',
                        owner: OWNER,
                    },
                    details: {},
                },
                setMax: 2,
                expected: {
                    objects: ['expire-2', 'expire-3'],
                    bucketCount: 2,
                    objectCount: 2,
                },
            },
            // basic expire with 1 Day rule, pagination, tagging part 2
            {
                message: 'should verify that EXPIRED objects are sent to ' +
                    'object kafka topic with pagination, with tags: rule 2',
                bucketLCRules: [
                    new Rule().addID('task-1').addExpiration('Days', 1)
                        .addTag('key2', 'value2').build(),
                ],
                bucketEntry: {
                    action: 'testing',
                    target: {
                        bucket: 'test-expire',
                        owner: OWNER,
                    },
                    details: {},
                },
                setMax: 3,
                expected: {
                    objects: ['expire-3'],
                    bucketCount: 1,
                    objectCount: 1,
                },
            },
            // Multiple bucket expiration rules, no pagination, tagging
            {
                message: 'should verify that EXPIRED rules are properly ' +
                    'being handled',
                bucketLCRules: [
                    new Rule().addID('task-1').addExpiration('Days', 3)
                        .addTag('key2', 'value2').build(),
                    new Rule().addID('task-2').addExpiration('Days', 1)
                        .addTag('key1', 'value1').addTag('key2', 'value2')
                        .build(),
                    new Rule().addID('task-3').addExpiration('Date', FUTURE)
                        .build(),
                ],
                bucketEntry: {
                    action: 'testing',
                    target: {
                        bucket: 'test-expire',
                        owner: OWNER,
                    },
                    details: {},
                },
                expected: {
                    objects: ['expire-3'],
                    bucketCount: 0,
                    objectCount: 1,
                },
            },
        ].forEach(item => {
            it(item.message, done => {
                if (item.setMax) {
                    s3mock.setMax(item.setMax);
                }
                let counter = 0;
                function wrapProcessBucketEntry(bucketLCRules, bucketEntry,
                s3mock, cb) {
                    lcTask.processBucketEntry(bucketLCRules, bucketEntry,
                    s3mock, err => {
                        assert.ifError(err);
                        counter++;

                        const entries = lcp.getEntries();
                        // console.log(entries)

                        if (counter === entries.bucket.length) {
                            return wrapProcessBucketEntry(bucketLCRules,
                                entries.bucket[entries.bucket.length - 1],
                                s3mock, cb);
                        }
                        return cb(null);
                    });
                }
                wrapProcessBucketEntry(item.bucketLCRules, item.bucketEntry,
                s3mock, err => {
                    assert.ifError(err);

                    const count = lcp.getCount();
                    const entries = lcp.getEntries();

                    const expectedObjects = item.expected.objects;

                    assert.equal(count.bucket, item.expected.bucketCount);
                    assert.equal(count.object, item.expected.objectCount);

                    assert.deepStrictEqual(entries.object, expectedObjects);
                    done();
                });
            });
        });
    });
});
