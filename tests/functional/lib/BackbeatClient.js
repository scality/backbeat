const http = require('http');
const assert = require('assert');
const BucketInfo = require('arsenal').models.BucketInfo;
const BackbeatClient = require('../../../lib/clients/BackbeatClient');
const { getAccountCredentials } =
    require('../../../lib/credentials/AccountCredentials');
const { MetadataMock, mockLogs, objectList, dummyBucketMD } =
    require('../../utils/MockMetadataServer');
const backbeatClientTestPort = 9004;
const bucketName = 'bucket1';
const bucketName2 = 'bucket2';
const objectName = 'testobject1';

const accountCreds = getAccountCredentials({
    type: 'account',
    account: 'bart',
});

const backbeatClient = new BackbeatClient({
    endpoint: `http://localhost:${backbeatClientTestPort}`,
    sslEnabled: false,
    credentials: accountCreds,
});

const serverMock = new MetadataMock();

describe('BackbeatClient unit tests with mock server', () => {
    let httpServer;
    before(done => {
        httpServer = http.createServer(
            (req, res) => serverMock.onRequest(req, res))
                .listen(backbeatClientTestPort, done);
    });

    after(() => httpServer.close());

    it('should get list of buckets managed by raft session', done => {
        const destReq = backbeatClient.getRaftBuckets({
            LogId: '1',
        });
        return destReq.send((err, data) => {
            assert.ifError(err);
            const expectedRes = {
                0: bucketName,
                1: bucketName2,
            };
            assert.deepStrictEqual(data, expectedRes);
            return done();
        });
    });
    it('should get raftId', done => {
        const destReq = backbeatClient.getRaftId({
            Bucket: bucketName,
        });
        return destReq.send((err, data) => {
            assert.ifError(err);
            assert.strictEqual(data[0], '5');
            return done();
        });
    });

    it('should get raftLogs', done => {
        const destReq = backbeatClient.getRaftLog({
            LogId: '1',
        });
        return destReq.send((err, data) => {
            assert.ifError(err);
            assert.deepStrictEqual(data, mockLogs);
            return done();
        });
    });

    it('should get bucket metadata', done => {
        const destReq = backbeatClient.getBucketMetadata({
            Bucket: bucketName,
        });
        return destReq.send((err, data) => {
            assert.ifError(err);
            const bucketMd = dummyBucketMD[bucketName];
            const expectedBucketMD = new BucketInfo(bucketMd._name,
                bucketMd._owner, bucketMd._ownerDisplayName,
                bucketMd._creationDate, bucketMd._mdBucketModelVersion,
                bucketMd._acl, bucketMd._transient, bucketMd._deleted,
                bucketMd._serverSideEncryption,
                bucketMd.versioningConfiguration, bucketMd._locationContraint,
                bucketMd._websiteConfiguration, bucketMd._cors,
                bucketMd._lifeCycle);
            const recBucketMD = new BucketInfo(data.name, data.owner,
                data.ownerDisplayName, data.creationDate,
                data.mdBucketModelVersion, data.acl, data.transient,
                data.deleted, null, data.versioningConfiguration,
                data.locationConstraint);
            delete expectedBucketMD._uid;
            delete recBucketMD._uid;
            assert.deepStrictEqual(recBucketMD, expectedBucketMD);
            return done();
        });
    });

    it('should get object list', done => {
        const destReq = backbeatClient.getObjectList({
            Bucket: bucketName,
        });
        return destReq.send((err, data) => {
            assert.ifError(err);
            assert.deepStrictEqual(data, objectList);
            return done();
        });
    });

    it('should get object metadata', done => {
        const destReq = backbeatClient.getObjectMetadata({
            Bucket: bucketName,
            Key: objectName,
        });
        return destReq.send((err, data) => {
            assert.ifError(err);
            assert.strictEqual(typeof data, 'object');
            assert(data.key);
            assert.strictEqual(data.key, 'dogsAreGood');
            return done();
        });
    });
});
