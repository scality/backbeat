'use strict'; // eslint-disable-line

const assert = require('assert');
const async = require('async');
const { ObjectMD, BucketInfo } = require('arsenal').models;
const { decode } = require('arsenal').versioning.VersionID;
const errors = require('arsenal').errors;
const VID_SEP = require('arsenal').versioning.VersioningConstants
          .VersionId.Separator;

const config = require('../../config.json');
const MongoQueueProcessor =
    require('../../../extensions/mongoProcessor/MongoQueueProcessor');
const authdata = require('../../../conf/authdata.json');
const ObjectQueueEntry = require('../../../lib/models/ObjectQueueEntry');
const fakeLogger = require('../../utils/fakeLogger');

const kafkaConfig = config.kafka;
const mongoProcessorConfig = config.extensions.mongoProcessor;
const mongoClientConfig = config.queuePopulator.mongo;
const mConfig = {};

const bootstrapList = config.extensions.replication.destination.bootstrapList;

const BUCKET = 'mqp-test-bucket';
const KEY = 'testkey1';
const LOCATION = 'us-east-1';
const VERSION_ID = '98445230573829999999RG001  15.144.0';
// new version id > existing version id
const NEW_VERSION_ID = '98445235075994999999RG001  14.90.2';

const mockReplicationInfo = {
    role: 'arn:aws:iam::root:role/s3-replication-role',
    destination: `arn:aws:s3:::${BUCKET}`,
    rules: [
        {
            prefix: '',
            enabled: true,
            id:
            'ZDA1YzQ4N2EtMmU1Zi00OTc0LTkxOGEtYzI0YjI0ZjI3NmY4',
            storageClass: bootstrapList[1].site,
        },
    ],
    preferredReadLocation: null,
};

class MongoClientMock {
    constructor() {
        this._added = [];
        this._deleted = [];
    }

    reset() {
        this._added = [];
        this._deleted = [];
    }

    getAdded() {
        return this._added;
    }

    getDeleted() {
        return this._deleted;
    }

    getBucketAttributes(bucket, log, cb) {
        const store = {
            [BUCKET]: {
                acl: {
                    Canned: 'private',
                    FULL_CONTROL: [],
                    WRITE: [],
                    WRITE_ACP: [],
                    READ: [],
                    READ_ACP: []
                },
                name: BUCKET,
                owner: authdata.accounts[0].canonicalID,
                ownerDisplayName: authdata.accounts[0].name,
                creationDate: '2019-04-08T16:47:13.154Z',
                mdBucketModelVersion: 10,
                transient: false,
                deleted: false,
                serverSideEncryption: null,
                versioningConfiguration: {
                    Status: 'Enabled',
                },
                locationConstraint: LOCATION,
                readLocationConstraint: null,
                cors: null,
                replicationConfiguration: mockReplicationInfo,
                lifecycleConfiguration: null,
                uid: 'ecf97531-3627-4fac-9492-e53e9dfc9470',
                isNFS: null,
                ingestion: {
                    status: 'enabled',
                },
            },
        };
        if (!store[bucket]) {
            return cb(errors.NoSuchBucket);
        }
        const bucketMDStr = JSON.stringify(store[bucket]);
        const bucketMD = BucketInfo.deSerialize(bucketMDStr);
        return cb(null, bucketMD);
    }

    getObject(bucket, key, params, log, cb) {
        const existingKeys = [KEY];
        if (bucket !== BUCKET) {
            return cb(errors.InternalError);
        }
        if (!existingKeys.includes(key)) {
            return cb(errors.NoSuchKey);
        }
        if (params && params.versionId && params.versionId !== VERSION_ID) {
            return cb(errors.NoSuchKey);
        }
        // we get object from mongo to determine replicationInfo.Content types.
        // use "tags" and "versionId" for determining this.
        const obj = new ObjectMD()
                            .setVersionId(VERSION_ID)
                            .setTags({ mytag: 'mytags-value' });
        return cb(null, obj._data);
    }

    deleteObject(bucket, key, params, log, cb) {
        assert.strictEqual(bucket, BUCKET);
        assert([KEY, `${KEY}${VID_SEP}${VERSION_ID}`].includes(key));
        this._deleted.push(key);
        return cb();
    }

    putObject(bucket, key, objVal, params, log, cb) {
        assert.strictEqual(bucket, BUCKET);
        let adjustedKey = key;
        // versionId will not be specified for single null versions
        if (params && params.versionId) {
            adjustedKey = `${key}${VID_SEP}${params.versionId}`;
        }
        this._added.push({ key: adjustedKey, objVal });
        return cb();
    }
}

class MongoQueueProcessorMock extends MongoQueueProcessor {
    start() {
        // mocks
        this._mongoClient = new MongoClientMock();
        this._mProducer = {
            close: () => {},
            publishMetrics: (metric, type, ext) => {
                this.addToMetricsStore({ metric, type, ext });
            },
        };
        this._bootstrapList = bootstrapList;
        this._metricsStore = [];
    }

    sendMockEntry(entry, cb) {
        return this._consumer.sendMockEntry(entry, cb);
    }

    addToMetricsStore(obj) {
        this._metricsStore.push(obj);
    }

    reset() {
        this._accruedMetrics = {};
        this._mongoClient.reset();
    }

    resetMetricsStore() {
        this._metricsStore = [];
    }

    getAdded() {
        return this._mongoClient.getAdded();
    }

    getDeleted() {
        return this._mongoClient.getDeleted();
    }

    getMetricsStore() {
        return this._metricsStore;
    }
}

describe('MongoQueueProcessor', function mqp() {
    this.timeout(5000);

    let mqp;
    let mongoClient;

    before(() => {
        mqp = new MongoQueueProcessorMock(kafkaConfig, mongoProcessorConfig,
            mongoClientConfig, mConfig);
        mqp.start();

        mongoClient = mqp._mongoClient;
    });

    afterEach(() => {
        mqp.reset();
    });

    describe('::_getZenkoObjectMetadata', () => {
        function testGetZenkoObjectMetadata(entry, cb) {
            mongoClient.getBucketAttributes(BUCKET, fakeLogger,
            (error, bucketInfo) => {
                assert.ifError(error);

                mqp._getZenkoObjectMetadata(fakeLogger, entry, bucketInfo, cb);
            });
        }

        it('should return empty if key does not exist in mongo', done => {
            const key = 'nonexistant';
            const objmd = new ObjectMD().setKey(key);
            const entry = new ObjectQueueEntry(BUCKET, key, objmd);
            testGetZenkoObjectMetadata(entry, (err, res) => {
                assert.ifError(err);

                assert.strictEqual(res, undefined);
                return done();
            });
        });

        it('should return empty if version id of object does not exist in ' +
        'mongo', done => {
            const versionKey = `${KEY}${VID_SEP}${NEW_VERSION_ID}`;
            const objmd = new ObjectMD()
                                .setKey(KEY)
                                .setVersionId(NEW_VERSION_ID);
            const entry = new ObjectQueueEntry(BUCKET, versionKey, objmd);
            testGetZenkoObjectMetadata(entry, (err, res) => {
                assert.ifError(err);

                assert.strictEqual(res, undefined);
                return done();
            });
        });

        it('should return empty if bucket replication info is disabled',
        done => {
            const disabledRepInfo = Object.assign({}, mockReplicationInfo, {
                rules: [{ enabled: false }],
            });
            const disabledMockBucketInfo = {
                getReplicationConfiguration: () => disabledRepInfo,
            };
            const versionKey = `${KEY}${VID_SEP}${NEW_VERSION_ID}`;
            const objmd = new ObjectMD()
                                .setKey(KEY)
                                .setVersionId(NEW_VERSION_ID);
            const entry = new ObjectQueueEntry(BUCKET, versionKey, objmd);
            mqp._getZenkoObjectMetadata(fakeLogger, entry,
                disabledMockBucketInfo, (err, res) => {
                    assert.ifError(err);

                    assert.strictEqual(res, undefined);
                    return done();
                });
        });

        it('should return object metadata for existing version', done => {
            const versionKey = `${KEY}${VID_SEP}${VERSION_ID}`;
            const objmd = new ObjectMD()
                                .setKey(KEY)
                                .setVersionId(VERSION_ID);
            const entry = new ObjectQueueEntry(BUCKET, versionKey, objmd);
            testGetZenkoObjectMetadata(entry, (err, res) => {
                assert.ifError(err);

                assert(res);
                assert.strictEqual(res.versionId, VERSION_ID);
                return done();
            });
        });
    });

    describe('::_processObjectQueueEntry', () => {
        function validateMetricReport(type, done) {
            // only 2 types of metric type reports
            assert(type === 'completed' || type === 'pendingOnly');

            const expectedMetricStore = [{
                ext: 'ingestion',
                metric: {
                    [LOCATION]: { ops: 1 },
                },
                type,
            }];

            const checker = setInterval(() => {
                const ms = mqp.getMetricsStore();
                if (ms.length !== 0) {
                    clearInterval(checker);
                    assert.deepStrictEqual(expectedMetricStore, ms);
                    done();
                }
            }, 1000);
        }

        afterEach(() => {
            mqp.resetMetricsStore();
        });

        it('should save to mongo a new version entry and update fields',
        done => {
            const versionKey = `${KEY}${VID_SEP}${NEW_VERSION_ID}`;
            const objmd = new ObjectMD()
                                .setAcl()
                                .setKey(KEY)
                                .setVersionId(NEW_VERSION_ID);
            const entry = new ObjectQueueEntry(BUCKET, versionKey, objmd);

            async.waterfall([
                next => mongoClient.getBucketAttributes(BUCKET, fakeLogger,
                    next),
                (bucketInfo, next) => mqp._processObjectQueueEntry(fakeLogger,
                    entry, LOCATION, bucketInfo, next),
            ], err => {
                assert.ifError(err);

                const added = mqp.getAdded();
                assert.strictEqual(added.length, 1);
                const objVal = added[0].objVal;
                assert.strictEqual(added[0].key, versionKey);
                // acl should reset
                assert.deepStrictEqual(objVal.acl, new ObjectMD().getAcl());
                // owner md should update
                assert.strictEqual(objVal['owner-display-name'],
                    authdata.accounts[0].name);
                assert.strictEqual(objVal['owner-id'],
                    authdata.accounts[0].canonicalID);
                // dataStoreName should update
                assert.strictEqual(objVal.dataStoreName, LOCATION);
                // locations should update, no data in object
                assert.strictEqual(objVal.location.length, 1);
                const loc = objVal.location[0];
                assert.strictEqual(loc.key, KEY);
                assert.strictEqual(loc.size, 0);
                assert.strictEqual(loc.start, 0);
                assert.strictEqual(loc.dataStoreName, LOCATION);
                assert.strictEqual(loc.dataStoreType, 'aws_s3');
                assert.strictEqual(decode(loc.dataStoreVersionId),
                    NEW_VERSION_ID);

                const repInfo = objVal.replicationInfo;
                // replication info should update
                assert.strictEqual(repInfo.status, 'PENDING');
                assert.deepStrictEqual(repInfo.backends, [{
                    site: bootstrapList[1].site,
                    status: 'PENDING',
                    dataStoreVersionId: '',
                }]);
                // size of object is 0 and is a new version
                assert.deepStrictEqual(repInfo.content,
                    ['METADATA']);
                assert.strictEqual(repInfo.storageClass,
                    bootstrapList[1].site);
                assert.strictEqual(repInfo.storageType, 'aws_s3');
                assert.strictEqual(repInfo.dataStoreVersionId, '');

                validateMetricReport('completed', done);
            });
        });

        it('should save to mongo a new object key with data', done => {
            const versionKey = `new-${KEY}${VID_SEP}${NEW_VERSION_ID}`;
            const objmd = new ObjectMD()
                                .setKey(`new-${KEY}`)
                                .setVersionId(NEW_VERSION_ID)
                                .setContentLength(110);
            const entry = new ObjectQueueEntry(BUCKET, versionKey, objmd);

            async.waterfall([
                next => mongoClient.getBucketAttributes(BUCKET, fakeLogger,
                    next),
                (bucketInfo, next) => mqp._processObjectQueueEntry(fakeLogger,
                    entry, LOCATION, bucketInfo, next),
            ], err => {
                assert.ifError(err);

                const added = mqp.getAdded();
                assert.strictEqual(added.length, 1);
                // since specifying content-length, should update Content
                const repInfo = added[0].objVal.replicationInfo;
                assert.deepStrictEqual(repInfo.content, ['DATA', 'METADATA']);

                validateMetricReport('completed', done);
            });
        });

        // if specifying same version id, and same object tags, we consider
        // this a duplicate entry
        it('should not save to mongo if considered a duplicate', done => {
            // use existing version id
            const versionKey = `${KEY}${VID_SEP}${VERSION_ID}`;
            // specify existing tags
            const objmd = new ObjectMD()
                                .setKey(KEY)
                                .setVersionId(VERSION_ID)
                                .setTags({ mytag: 'mytags-value' });
            const entry = new ObjectQueueEntry(BUCKET, versionKey, objmd);

            async.waterfall([
                next => mongoClient.getBucketAttributes(BUCKET, fakeLogger,
                    next),
                (bucketInfo, next) => mqp._processObjectQueueEntry(fakeLogger,
                    entry, LOCATION, bucketInfo, next),
            ], err => {
                assert.ifError(err);

                const added = mqp.getAdded();
                assert.strictEqual(added.length, 0);

                validateMetricReport('pendingOnly', done);
            });
        });

        it('should save md-only delete tagging updates to mongo', done => {
            // use existing version id
            const versionKey = `${KEY}${VID_SEP}${VERSION_ID}`;
            // no object tags in new entry w/ same version id
            const objmd = new ObjectMD()
                                .setKey(KEY)
                                .setVersionId(VERSION_ID);
            const entry = new ObjectQueueEntry(BUCKET, versionKey, objmd);

            async.waterfall([
                next => mongoClient.getBucketAttributes(BUCKET, fakeLogger,
                    next),
                (bucketInfo, next) => mqp._processObjectQueueEntry(fakeLogger,
                    entry, LOCATION, bucketInfo, next),
            ], err => {
                assert.ifError(err);

                const added = mqp.getAdded();
                const objVal = added[0].objVal;
                assert.strictEqual(added.length, 1);
                assert.deepStrictEqual(objVal.replicationInfo.content,
                    ['METADATA', 'DELETE_TAGGING']);

                validateMetricReport('completed', done);
            });
        });

        it('should save md-only put tagging updates to mongo', done => {
            // use existing version id
            const versionKey = `${KEY}${VID_SEP}${VERSION_ID}`;
            // change the value of a tag
            const objmd = new ObjectMD()
                                .setKey(KEY)
                                .setVersionId(VERSION_ID)
                                .setTags({ mytag: 'new-tag-value' });
            const entry = new ObjectQueueEntry(BUCKET, versionKey, objmd);

            async.waterfall([
                next => mongoClient.getBucketAttributes(BUCKET, fakeLogger,
                    next),
                (bucketInfo, next) => mqp._processObjectQueueEntry(fakeLogger,
                    entry, LOCATION, bucketInfo, next),
            ], err => {
                assert.ifError(err);

                const added = mqp.getAdded();
                const objVal = added[0].objVal;
                assert.strictEqual(added.length, 1);
                assert.deepStrictEqual(objVal.replicationInfo.content,
                    ['METADATA', 'PUT_TAGGING']);

                validateMetricReport('completed', done);
            });
        });

        it('should save a null version with internal version id', done => {
            const nullVersionId = '99999999999999999999RG001  ';
            const versionKey = `${KEY}${VID_SEP}${nullVersionId}`;
            const objmd = new ObjectMD()
                                .setKey(KEY)
                                .setVersionId(nullVersionId);
            const entry = new ObjectQueueEntry(BUCKET, versionKey, objmd);

            async.waterfall([
                next => mongoClient.getBucketAttributes(BUCKET, fakeLogger,
                    next),
                (bucketInfo, next) => mqp._processObjectQueueEntry(fakeLogger,
                    entry, LOCATION, bucketInfo, next),
            ], err => {
                assert.ifError(err);

                const added = mqp.getAdded();
                assert.strictEqual(added.length, 1);
                const objVal = added[0].objVal;
                assert.strictEqual(objVal.location.length, 1);
                const loc = objVal.location[0];
                assert.strictEqual(decode(loc.dataStoreVersionId),
                    nullVersionId);
                validateMetricReport('completed', done);
            });
        });
    });

    describe('::_processDeleteOpQueueEntry', () => {
        it('should delete an existing object from mongo', done => {
            // use existing version id
            const versionKey = `${KEY}${VID_SEP}${VERSION_ID}`;
            const objmd = new ObjectMD()
                                .setKey(KEY)
                                .setVersionId(VERSION_ID);
            const entry = new ObjectQueueEntry(BUCKET, versionKey, objmd);
            async.waterfall([
                next => mongoClient.getBucketAttributes(BUCKET, fakeLogger,
                    next),
                (bucketInfo, next) => mqp._processDeleteOpQueueEntry(fakeLogger,
                    entry, LOCATION, next),
            ], err => {
                assert.ifError(err);

                const deleted = mqp.getDeleted();
                assert.strictEqual(deleted.length, 1);
                assert.strictEqual(deleted[0], versionKey);
                done();
            });
        });
    });

    describe('::_getBucketInfo', () => {
        it('should memoize bucket info', done => {
            const objmd = new ObjectMD();
            const entry = new ObjectQueueEntry(BUCKET, KEY, objmd);
            const bucketMemState = mqp._bucketMemState;
            // bucket should not be memoized
            assert.strictEqual(bucketMemState.getBucketInfo(BUCKET), undefined);

            mqp._getBucketInfo(entry, fakeLogger, (err, bucketInfo) => {
                assert.ifError(err);
                // has it memoized?
                const bucketInfoInMem = bucketMemState.getBucketInfo(BUCKET);
                assert(bucketInfoInMem);
                const location = bucketInfoInMem.getLocationConstraint();
                const repConfig = bucketInfoInMem.getReplicationConfiguration();
                assert(location);
                assert(repConfig);
                assert.deepStrictEqual(bucketInfoInMem, bucketInfo);
                assert.strictEqual(location, LOCATION);
                assert.deepStrictEqual(repConfig, mockReplicationInfo);
                done();
            });
        });
    });
});
