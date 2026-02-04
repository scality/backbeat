const http = require('http');
const assert = require('assert');
const BucketInfo = require('arsenal').models.BucketInfo;
const { getAccountCredentials } =
    require('../../../lib/credentials/AccountCredentials');
const { MetadataMock, mockLogs, objectList, dummyBucketMD, objectMD } =
    require('../utils/MetadataMock');
const { 
    BackbeatRoutesClient,
    GetRaftIdCommand,
    GetRaftBucketsCommand,
    GetRaftLogCommand,
    GetBucketMetadataCommand,
    GetObjectListCommand,
    GetMetadataCommand,
    GetBucketCseqCommand,
    PutDataCommand
} = require('@scality/cloudserverclient');
const backbeatClientTestPort = 9004;
const bucketName = 'bucket1';
const bucketName2 = 'bucket2';
const objectName = 'object1';

const expectedLogs = JSON.parse(JSON.stringify(mockLogs));
const expectedObjectList = JSON.parse(JSON.stringify(objectList));
const expectedObjectMD = objectMD[objectName];

const accountCreds = getAccountCredentials({
    type: 'account',
    account: 'bart',
});

const backbeatClient = new BackbeatRoutesClient({
    endpoint: `http://localhost:${backbeatClientTestPort}`,
    credentials: accountCreds.getCredentialsProvider(),
    region: 'us-east-1',
});

const serverMock = new MetadataMock();

describe('BackbeatClient unit tests with mock server', () => {
    let httpServer;
    before(done => {
        expectedLogs.log.forEach((log, i) => {
            log.entries.forEach((entry, j) => {
                expectedLogs.log[i].entries[j].value.attributes =
                    JSON.stringify(entry.value.attributes);
                expectedLogs.log[i].entries[j].value =
                    JSON.stringify(entry.value);
            });
        });
        expectedObjectList.Contents.forEach((obj, i) => {
            expectedObjectList.Contents[i].value =
                JSON.stringify(obj.value);
        });
        httpServer = http.createServer(
            (req, res) => serverMock.onRequest(req, res))
                .listen(backbeatClientTestPort, done);
    });

    after(() => httpServer.close());

    // this test may be skipped because ingestion does not need list bucket per raft
    it('should get list of buckets managed by raft session', async () => {
        const data = await backbeatClient.send(new GetRaftBucketsCommand({
            LogId: '1',
        }));
        assert.deepStrictEqual(data.Buckets, [bucketName, bucketName2]);
    });
    
    it('should get raftId', async () => {
        const data = await backbeatClient.send(new GetRaftIdCommand({
            Bucket: bucketName,
        }));
        assert.strictEqual(data.RaftId, '1');
    });

    it('should get raftLogs', async () => {
        const data = await backbeatClient.send(new GetRaftLogCommand({
            LogId: '1',
        }));
        const dataStr = await data.Body.transformToString();
        const jsonData = JSON.parse(dataStr);
        assert.deepStrictEqual(jsonData, expectedLogs);
    });

    it('should get bucket metadata', async () => {
        const data = await backbeatClient.send(new GetBucketMetadataCommand({
            Bucket: bucketName,
        }));
        const bucketMd = dummyBucketMD[bucketName];
        const expectedBucketMD = new BucketInfo(bucketMd.name,
            bucketMd.owner, bucketMd.ownerDisplayName,
            bucketMd.creationDate, bucketMd.mdBucketModelVersion,
            bucketMd.acl, bucketMd.transient, bucketMd.deleted,
            bucketMd.serverSideEncryption,
            bucketMd.versioningConfiguration, bucketMd.locationConstraint,
            bucketMd.websiteConfiguration, bucketMd.cors,
            bucketMd.lifeCycle);
        const recBucketMD = new BucketInfo(data.name, data.owner,
            data.ownerDisplayName, data.creationDate,
            data.mdBucketModelVersion, data.acl, data.transient,
            data.deleted, data.serverSideEncryption,
            data.versioningConfiguration, data.locationConstraint,
            data.websiteConfiguration, data.cors, data.lifeCycle);
        delete expectedBucketMD._uid;
        delete recBucketMD._uid;
        assert.deepStrictEqual(recBucketMD, expectedBucketMD);
    });

    it('should get object list', async () => {
        const data = await backbeatClient.send(new GetObjectListCommand({
            Bucket: bucketName,
        }));
        assert.deepStrictEqual(data.Contents, expectedObjectList.Contents);
        assert.deepStrictEqual(data.IsTruncated, expectedObjectList.IsTruncated);
        assert.deepStrictEqual(data.Delimiter, expectedObjectList.Delimiter);
        assert.deepStrictEqual(data.CommonPrefixes, expectedObjectList.CommonPrefixes);
    });

    it('should get object metadata', async () => {
        const data = await backbeatClient.send(new GetMetadataCommand({
            Bucket: bucketName,
            Key: objectName,
        }));
        const dataValue = JSON.parse(data.Body);
        assert.deepStrictEqual(dataValue, expectedObjectMD);
    });

    it('should get bucket specified cseq', async () => {
        const data = await backbeatClient.send(new GetBucketCseqCommand({
            Bucket: bucketName,
        }));
        assert.strictEqual(data.CseqInfo[0].cseq, 7);
    });

    it('should handle openresty HTML error response for putData', async () => {
        try {
            await backbeatClient.send(new PutDataCommand({
                Bucket: 'bkterr',
                Key: 'objerr',
                CanonicalID: 'test-canonical-id',
                ContentLength: 0,
                Body: '',
                VersioningRequired: true,
            }));
            throw new Error('Expected an error but got success');
        } catch (err) {
            assert.strictEqual(err.name, 'HTML Bad Request');
            assert.strictEqual(err.message, '400 Request Header Or Cookie Too Large');
            assert.strictEqual(err.$metadata.httpStatusCode, 400);
            assert(err.rawBody);
            assert(err.rawBody.includes('<title>400 Request Header Or Cookie Too Large</title>'));
            assert(err.rawBody.includes('<center>Request Header Or Cookie Too Large</center>'));
            assert(err.rawBody.includes('<center>openresty</center>'));
        }
    });
});
