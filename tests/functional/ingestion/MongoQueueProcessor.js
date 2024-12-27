'use strict';

const assert = require('assert');
const async = require('async');
const sinon = require('sinon');
const { ObjectMD, BucketInfo } = require('arsenal').models;
const { decode, encode } = require('arsenal').versioning.VersionID;
const errors = require('arsenal').errors;
const VID_SEP = require('arsenal').versioning.VersioningConstants
          .VersionId.Separator;

const config = require('../../config.json');
const MongoQueueProcessor =
    require('../../../extensions/mongoProcessor/MongoQueueProcessor');
const authdata = require('../../../conf/authdata.json');
const ObjectQueueEntry = require('../../../lib/models/ObjectQueueEntry');
const DeleteOpQueueEntry = require('../../../lib/models/DeleteOpQueueEntry');
const fakeLogger = require('../../utils/fakeLogger');
const { ObjectMDArchive, LifecycleConfiguration, NotificationConfiguration } = require('arsenal/build/lib/models');

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

const mockArchive = new ObjectMDArchive(
    { archiveId: '123456789' },
    Date.now() - 3600 * 1000, 1,
    Date.now(), Date.now() + 23 * 3600 * 1000,
);

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
            .setTags({ mytag: 'mytags-value' })
            .setDataStoreName(LOCATION)
            .setLocation([{
                key: KEY,
                dataStoreName: LOCATION,
                dataStoreVersionId: encode(VERSION_ID),
            }]);
        return cb(null, obj._data);
    }

    deleteObject(bucket, key, params, log, cb) {
        assert.strictEqual(bucket, BUCKET);
        assert([KEY, `${KEY}${VID_SEP}${VERSION_ID}`].includes(key));
        const versionId = params && params.versionId;
        this._deleted.push({ key, versionId });
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
        sinon.restore();
    });

    describe('::_getZenkoObjectMetadata', () => {
        it('should return an error if key does not exist in mongo', done => {
            const key = 'nonexistant';
            const objmd = new ObjectMD().setKey(key);
            const entry = new ObjectQueueEntry(BUCKET, key, objmd);
            mqp._getZenkoObjectMetadata(fakeLogger, entry, VERSION_ID, (err, res) => {
                assert.ok(err?.is?.NoSuchKey);

                assert.strictEqual(res, undefined);
                return done();
            });
        });

        it('should return an error if version id of object does not exist in mongo', done => {
            const versionKey = `${KEY}${VID_SEP}${NEW_VERSION_ID}`;
            const objmd = new ObjectMD()
                .setKey(KEY)
                .setVersionId(NEW_VERSION_ID);
            const entry = new ObjectQueueEntry(BUCKET, versionKey, objmd);
            mqp._getZenkoObjectMetadata(fakeLogger, entry, NEW_VERSION_ID, (err, res) => {
                assert.ok(err?.is?.NoSuchKey);

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
            mqp._getZenkoObjectMetadata(fakeLogger, entry, VERSION_ID, (err, res) => {
                assert.ifError(err);

                assert(res);
                assert.strictEqual(res.versionId, VERSION_ID);
                return done();
            });
        });

        it('should return object metadata for existing version from DeleteOpQueueEntry', done => {
            const versionKey = `${KEY}${VID_SEP}${VERSION_ID}`;
            const entry = new DeleteOpQueueEntry(BUCKET, versionKey);
            mqp._getZenkoObjectMetadata(fakeLogger, entry, VERSION_ID, (err, res) => {
                assert.ifError(err);

                assert(res);
                assert.strictEqual(res.versionId, VERSION_ID);
                return done();
            });
        });

        it('should return object metadata for null "master" version', done => {
            const versionKey = `${KEY}`;
            const objmd = new ObjectMD()
                .setKey(KEY);
            const entry = new ObjectQueueEntry(BUCKET, versionKey, objmd);
            mqp._getZenkoObjectMetadata(fakeLogger, entry, null, (err, res) => {
                assert.ifError(err);

                assert(res);
                assert.strictEqual(res.versionId, VERSION_ID);
                return done();
            });
        });

        it('should return object metadata for null "suspended" version', done => {
            const versionKey = `${KEY}${VID_SEP}${NEW_VERSION_ID}`;
            const objmd = new ObjectMD()
                .setKey(KEY)
                .setVersionId(NEW_VERSION_ID)
                .setNullVersionId(NEW_VERSION_ID)
                .setIsNull(true);
            const entry = new ObjectQueueEntry(BUCKET, versionKey, objmd);
            mqp._getZenkoObjectMetadata(fakeLogger, entry, NEW_VERSION_ID, (err, res) => {
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
            sinon.restore();
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
                // key shall now be always populated
                assert.deepStrictEqual(objVal.key, KEY);
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
            const objKey = `new-${KEY}`;
            const versionKey = `new-${KEY}${VID_SEP}${NEW_VERSION_ID}`;
            const contentLength = 110;
            const contentMD5 = '7cccfcef3abdfaba48b2d193cb146074';
            const objLocations = [{
                key: objKey,
                start: 0,
                size: 50,
                dataStoreName: LOCATION,
                dataStoreETag: `1:${contentMD5}`,
                dataStoreVersionId: encode(NEW_VERSION_ID),
            }, {
                key: objKey,
                start: 50,
                size: 50,
                dataStoreName: LOCATION,
                dataStoreETag: `2:${contentMD5}`,
                dataStoreVersionId: encode(NEW_VERSION_ID),
            }, {
                key: objKey,
                start: 100,
                size: 50,
                dataStoreName: LOCATION,
                dataStoreETag: `3:${contentMD5}`,
                dataStoreVersionId: encode(NEW_VERSION_ID),
            }];
            const objmd = new ObjectMD()
                            .setKey(objKey)
                            .setVersionId(NEW_VERSION_ID)
                            .setContentLength(contentLength)
                            .setContentMd5(contentMD5)
                            .setLocation(objLocations);
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
                const obj = added[0];
                // since specifying content-length, should update Content
                const repInfo = obj.objVal.replicationInfo;
                assert.deepStrictEqual(repInfo.content, ['DATA', 'METADATA']);

                // assert location data
                assert.strictEqual(obj.objVal.location.length, 1);
                const loc = obj.objVal.location[0];
                assert.strictEqual(loc.key, objKey);
                assert.strictEqual(loc.size, contentLength);
                assert.strictEqual(loc.start, 0);
                assert.strictEqual(loc.dataStoreName, LOCATION);
                assert.strictEqual(loc.dataStoreType, 'aws_s3');
                assert.strictEqual(loc.dataStoreETag, `1:${contentMD5}`);
                assert.strictEqual(decode(loc.dataStoreVersionId),
                    NEW_VERSION_ID);
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

        it('should fail when mongo is not available', done => {
            const versionKey = `${KEY}${VID_SEP}${VERSION_ID}`;
            const objmd = new ObjectMD()
                .setAcl()
                .setKey(KEY)
                .setVersionId(VERSION_ID);
            const entry = new ObjectQueueEntry(BUCKET, versionKey, objmd);
            const getObject = sinon.stub(mongoClient, 'getObject').yields(errors.InternalError);
            async.waterfall([
                next => mongoClient.getBucketAttributes(BUCKET, fakeLogger,
                    next),
                (bucketInfo, next) => mqp._processObjectQueueEntry(fakeLogger,
                    entry, LOCATION, bucketInfo, next),
            ], err => {
                assert.ok(err?.is?.InternalError);

                sinon.assert.calledOnce(getObject);
                assert.strictEqual(getObject.getCall(0).args[0], BUCKET);
                assert.strictEqual(getObject.getCall(0).args[1], KEY);
                assert.strictEqual(getObject.getCall(0).args[2].versionId, VERSION_ID);

                validateMetricReport('pendingOnly', done);
            });
        });

        it('should save to mongo a new version entry when no replication', done => {
            const versionKey = `${KEY}${VID_SEP}${NEW_VERSION_ID}`;
            const objmd = new ObjectMD()
                .setAcl()
                .setKey(KEY)
                .setVersionId(NEW_VERSION_ID);
            const entry = new ObjectQueueEntry(BUCKET, versionKey, objmd);
            const getObject = sinon.stub(mongoClient, 'getObject').yields(errors.InternalError);
            async.waterfall([
                next => mongoClient.getBucketAttributes(BUCKET, fakeLogger,
                    next),
                (bucketInfo, next) => next(null,
                    bucketInfo.setReplicationConfiguration(null)),
                (bucketInfo, next) => mqp._processObjectQueueEntry(fakeLogger,
                    entry, LOCATION, bucketInfo, next),
            ], err => {
                assert.ifError(err);
                sinon.assert.notCalled(getObject);

                const added = mqp.getAdded();
                assert.strictEqual(added.length, 1);
                const objVal = added[0].objVal;
                assert.strictEqual(added[0].key, versionKey);
                // key shall now be always populated
                assert.deepStrictEqual(objVal.key, KEY);
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

                // replication info should be empty
                const repInfo = objVal.replicationInfo;
                assert.strictEqual(repInfo.status, '');
                assert.deepStrictEqual(repInfo.backends, []);
                assert.deepStrictEqual(repInfo.content, []);
                assert.strictEqual(repInfo.storageClass, '');
                assert.strictEqual(repInfo.storageType, '');
                assert.strictEqual(repInfo.dataStoreVersionId, '');

                done();
            });
        });

        it('should save to mongo a new version entry when scal-version-id not found', done => {
            const versionKey = `${KEY}${VID_SEP}${NEW_VERSION_ID}`;
            const objmd = new ObjectMD()
                .setAcl()
                .setKey(KEY)
                .setVersionId(NEW_VERSION_ID);
            const entry = new ObjectQueueEntry(BUCKET, versionKey, objmd)
                .setUserMetadata({ 'x-amz-meta-scal-version-id': encode(VERSION_ID) });
            const getObject = sinon.stub(mongoClient, 'getObject').yields(errors.NoSuchKey);
            async.waterfall([
                next => mongoClient.getBucketAttributes(BUCKET, fakeLogger,
                    next),
                (bucketInfo, next) => next(null,
                    bucketInfo.setReplicationConfiguration(null)),
                (bucketInfo, next) => mqp._processObjectQueueEntry(fakeLogger,
                    entry, LOCATION, bucketInfo, next),
            ], err => {
                assert.ifError(err);

                sinon.assert.calledOnce(getObject);
                assert.strictEqual(getObject.getCall(0).args[0], BUCKET);
                assert.strictEqual(getObject.getCall(0).args[1], KEY);
                assert.strictEqual(getObject.getCall(0).args[2].versionId, VERSION_ID);

                const added = mqp.getAdded();
                assert.strictEqual(added.length, 1);
                const objVal = added[0].objVal;
                assert.strictEqual(added[0].key, versionKey);
                // key shall now be always populated
                assert.deepStrictEqual(objVal.key, KEY);
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

                // replication info should be empty
                const repInfo = objVal.replicationInfo;
                assert.strictEqual(repInfo.status, '');
                assert.deepStrictEqual(repInfo.backends, []);
                assert.deepStrictEqual(repInfo.content, []);
                assert.strictEqual(repInfo.storageClass, '');
                assert.strictEqual(repInfo.storageType, '');
                assert.strictEqual(repInfo.dataStoreVersionId, '');

                done();
            });
        });

        it('should not update restored entry', done => {
            const versionKey = `${KEY}${VID_SEP}${NEW_VERSION_ID}`;
            const objmd = new ObjectMD()
                .setAcl()
                .setKey(KEY)
                .setVersionId(NEW_VERSION_ID)
                .setUserMetadata({ 'x-amz-meta-scal-version-id': encode(VERSION_ID) });
            const entry = new ObjectQueueEntry(BUCKET, versionKey, objmd);
            const getObject = sinon.stub(mongoClient, 'getObject').yields(null,
                new ObjectMD()
                    .setKey(KEY)
                    .setVersionId(VERSION_ID)
                    .setDataStoreName(LOCATION)
                    .setAmzStorageClass('cold')
                    .setArchive(mockArchive)
                    .setLocation([{
                        key: KEY,
                        start: 0,
                        size: 50,
                        dataStoreName: LOCATION,
                        dataStoreVersionId: NEW_VERSION_ID,
                    }])._data
            );
            async.waterfall([
                next => mongoClient.getBucketAttributes(BUCKET, fakeLogger,
                    next),
                (bucketInfo, next) => next(null,
                    bucketInfo.setReplicationConfiguration(null)),
                (bucketInfo, next) => mqp._processObjectQueueEntry(fakeLogger,
                    entry, LOCATION, bucketInfo, next),
            ], err => {
                assert.ifError(err);

                sinon.assert.calledOnce(getObject);
                assert.strictEqual(getObject.getCall(0).args[0], BUCKET);
                assert.strictEqual(getObject.getCall(0).args[1], KEY);
                assert.strictEqual(getObject.getCall(0).args[2].versionId, VERSION_ID);

                const added = mqp.getAdded();
                assert.strictEqual(added.length, 0);

                done();
            });
        });

        it('should update tags on restored entry', done => {
            const versionKey = `${KEY}${VID_SEP}${NEW_VERSION_ID}`;
            const entry = new ObjectQueueEntry(BUCKET, versionKey, new ObjectMD()
                .setAcl()
                .setKey(KEY)
                .setTags({ mytag: 'mytags-value' })
                .setVersionId(NEW_VERSION_ID)
                .setUserMetadata({ 'x-amz-meta-scal-version-id': encode(VERSION_ID) }));
            const objmd = new ObjectMD()
                .setKey(KEY)
                .setVersionId(VERSION_ID)
                .setDataStoreName(LOCATION)
                .setAmzStorageClass('cold')
                .setArchive(mockArchive)
                .setLocation([{
                    key: KEY,
                    start: 0,
                    size: 50,
                    dataStoreName: LOCATION,
                    dataStoreVersionId: NEW_VERSION_ID,
                }]);
            const getObject = sinon.stub(mongoClient, 'getObject').yields(null, objmd.getValue());
            async.waterfall([
                next => mongoClient.getBucketAttributes(BUCKET, fakeLogger,
                    next),
                (bucketInfo, next) => mqp._processObjectQueueEntry(fakeLogger,
                    entry, LOCATION, bucketInfo, next),
            ], err => {
                assert.ifError(err);

                sinon.assert.calledOnce(getObject);
                assert.strictEqual(getObject.getCall(0).args[0], BUCKET);
                assert.strictEqual(getObject.getCall(0).args[1], KEY);
                assert.strictEqual(getObject.getCall(0).args[2].versionId, VERSION_ID);

                const added = mqp.getAdded();
                assert.strictEqual(added.length, 1);

                // Expect tags and replicationInfo to have been updated
                const objVal = added[0].objVal;
                assert.deepStrictEqual(objVal, objmd
                    .setTags({ mytag: 'mytags-value' })
                    .setReplicationInfo({
                        backends: [{
                            dataStoreVersionId: '',
                            site: 'test-site-2',
                            status: 'PENDING'
                        }],
                        content: ['METADATA', 'PUT_TAGGING'],
                        dataStoreVersionId: '',
                        destination: 'arn:aws:s3:::mqp-test-bucket',
                        isNFS: null,
                        role: 'arn:aws:iam::root:role/s3-replication-role',
                        status: 'PENDING',
                        storageClass: 'test-site-2',
                        storageType: 'aws_s3'
                    })
                    .getValue());

                done();
            });
        });
    });

    describe('::_processDeleteOpQueueEntry', () => {
        [
            {
                title: '',
                patchBucketInfo: (bucketInfo, next) => next(null, bucketInfo),
                options: { doesNotNeedOpogUpdate: true, versionId: VERSION_ID },
            },
            {
                title: ' with bucket notification',
                patchBucketInfo: (bucketInfo, next) => next(null, {
                    ...bucketInfo,
                    notificationConfiguration: new NotificationConfiguration(),
                }),
                options: { versionId: VERSION_ID },
            },
            {
                title: ' with lifecycle configuration',
                patchBucketInfo: (bucketInfo, next) => next(null, {
                    ...bucketInfo,
                    lifecycleConfiguration: new LifecycleConfiguration(null, { replicationEndpoints: [] }),
                }),
                options: { versionId: VERSION_ID },
            },
        ].forEach(({
            title, patchBucketInfo, options,
        }) => it(`should delete an existing versioned object from mongo${title}`, done => {
            // use existing version id
            const versionKey = `${KEY}${VID_SEP}${VERSION_ID}`;
            const entry = new DeleteOpQueueEntry(BUCKET, versionKey);
            const deleteObject = sinon.stub(mongoClient, 'deleteObject').callThrough();
            async.waterfall([
                next => mongoClient.getBucketAttributes(BUCKET, fakeLogger,
                    next),
                patchBucketInfo,
                (bucketInfo, next) => mqp._processDeleteOpQueueEntry(fakeLogger,
                    entry, LOCATION, bucketInfo, next),
            ], err => {
                assert.ifError(err);

                const deleted = mqp.getDeleted();
                assert.strictEqual(deleted.length, 1);
                assert.strictEqual(deleted[0].key, KEY);
                assert.strictEqual(deleted[0].versionId, VERSION_ID);

                sinon.assert.calledOnce(deleteObject);
                assert.deepStrictEqual(deleteObject.getCall(0).args[2], options);

                done();
            });
        }));

        it('should delete an existing non versioned object from mongo', done => {
            const objmd = new ObjectMD()
                .setKey(KEY)
                .setDataStoreName(LOCATION);
            const entry = new DeleteOpQueueEntry(BUCKET, KEY);
            sinon.stub(mqp._mongoClient, 'getObject').yields(null, objmd
                .setLocation([{
                    key: KEY,
                    dataStoreVersionId: '',
                    dataStoreName: LOCATION,
                }])
                .getValue());
            async.waterfall([
                next => mongoClient.getBucketAttributes(BUCKET, fakeLogger,
                    next),
                (bucketInfo, next) => mqp._processDeleteOpQueueEntry(fakeLogger,
                    entry, LOCATION, bucketInfo, next),
            ], err => {
                assert.ifError(err);

                const deleted = mqp.getDeleted();
                assert.strictEqual(deleted.length, 1);
                assert.strictEqual(deleted[0].key, KEY);
                assert.strictEqual(deleted[0].versionId, undefined);
                done();
            });
        });

        it('should use scal-version-id overhead field', done => {
            // use existing version id
            const versionKey = `${KEY}${VID_SEP}${NEW_VERSION_ID}`;
            const entry = new DeleteOpQueueEntry(BUCKET, versionKey, {
                'x-amz-meta-scal-version-id': encode(VERSION_ID),
            });
            const getObject = sinon.stub(mongoClient, 'getObject').yields(null, new ObjectMD()
                .setKey(KEY)
                .setVersionId(VERSION_ID)
                .setDataStoreName(LOCATION)
                .setLocation([{
                    key: KEY,
                    dataStoreVersionId: encode(NEW_VERSION_ID),
                    dataStoreName: LOCATION,
                }])
                .getValue());
            const deleteObject = sinon.stub(mongoClient, 'deleteObject').callThrough();
            async.waterfall([
                next => mongoClient.getBucketAttributes(BUCKET, fakeLogger,
                    next),
                (bucketInfo, next) => mqp._processDeleteOpQueueEntry(fakeLogger,
                    entry, LOCATION, bucketInfo, next),
            ], err => {
                assert.ifError(err);

                sinon.assert.calledOnce(getObject);
                assert.deepStrictEqual(deleteObject.getCall(0).args[0], BUCKET);
                assert.deepStrictEqual(deleteObject.getCall(0).args[1], KEY);
                assert.deepStrictEqual(deleteObject.getCall(0).args[2], {
                    versionId: VERSION_ID
                });

                sinon.assert.calledOnce(deleteObject);
                assert.deepStrictEqual(deleteObject.getCall(0).args[0], BUCKET);
                assert.deepStrictEqual(deleteObject.getCall(0).args[1], KEY);
                assert.deepStrictEqual(deleteObject.getCall(0).args[2], {
                    doesNotNeedOpogUpdate: true,
                    versionId: VERSION_ID
                });

                done();
            });
        });

        it('should not delete object from mongo when object is in another location', done => {
            // use existing version id
            const versionKey = `${KEY}${VID_SEP}${VERSION_ID}`;
            const entry = new DeleteOpQueueEntry(BUCKET, versionKey);
            sinon.stub(mqp._mongoClient, 'getObject').yields(null, new ObjectMD()
                .setKey(KEY)
                .setVersionId(VERSION_ID)
                .setDataStoreName('cold')
                .setLocation([{
                    key: KEY,
                    dataStoreVersionId: encode(VERSION_ID),
                    dataStoreName: LOCATION,
                }])
                .getValue());
            async.waterfall([
                next => mongoClient.getBucketAttributes(BUCKET, fakeLogger,
                    next),
                (bucketInfo, next) => mqp._processDeleteOpQueueEntry(fakeLogger,
                    entry, LOCATION, bucketInfo, next),
            ], err => {
                assert.ifError(err);

                const deleted = mqp.getDeleted();
                assert.strictEqual(deleted.length, 0);
                done();
            });
        });

        it('should not fail if object to delete does not exist anymore in mongo', done => {
            // use existing version id
            const versionKey = `${KEY}${VID_SEP}${VERSION_ID}`;
            const entry = new DeleteOpQueueEntry(BUCKET, versionKey);
            const deleteObject = sinon.stub(mongoClient, 'deleteObject').yields(errors.NoSuchKey);
            async.waterfall([
                next => mongoClient.getBucketAttributes(BUCKET, fakeLogger, next),
                (bucketInfo, next) => mqp._processDeleteOpQueueEntry(fakeLogger,
                    entry, LOCATION, bucketInfo, next),
            ], err => {
                assert.ifError(err);

                assert.ok(deleteObject.calledOnce);
                assert.strictEqual(deleteObject.getCall(0).args[0], BUCKET);
                assert.strictEqual(deleteObject.getCall(0).args[1], KEY);
                assert.deepStrictEqual(deleteObject.getCall(0).args[2], {
                    doesNotNeedOpogUpdate: true,
                    versionId: VERSION_ID
                });

                done();
            });
        });

        it('should fail if deleteObject fails', done => {
            // use existing version id
            const versionKey = `${KEY}${VID_SEP}${VERSION_ID}`;
            const entry = new DeleteOpQueueEntry(BUCKET, versionKey);
            const deleteObject = sinon.stub(mongoClient, 'deleteObject').yields(errors.InternalError);
            async.waterfall([
                next => mongoClient.getBucketAttributes(BUCKET, fakeLogger, next),
                (bucketInfo, next) => mqp._processDeleteOpQueueEntry(fakeLogger,
                    entry, LOCATION, bucketInfo, next),
            ], err => {
                assert.ok(err?.is?.InternalError);

                assert.ok(deleteObject.calledOnce);
                assert.strictEqual(deleteObject.getCall(0).args[0], BUCKET);
                assert.strictEqual(deleteObject.getCall(0).args[1], KEY);
                assert.deepStrictEqual(deleteObject.getCall(0).args[2], {
                    doesNotNeedOpogUpdate: true,
                    versionId: VERSION_ID,
                });

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
