const async = require('async');
const assert = require('assert');
const fs = require('fs');
const http = require('http');
const querystring = require('querystring');
const URL = require('url');
const werelogs = require('werelogs');
const Logger = werelogs.Logger;
const BucketInfo = require('arsenal').models.BucketInfo;

const IngestionProducer =
    require('../../../lib/queuePopulator/IngestionProducer');

class MetadataMock {
    constructor() {
        // this.log = new Logger('IngestionProducer:test:metadataMock');
        // const bucketList = ['bucket1', 'bucket2', 'bucket3'];
        // const routes = {
        //     getBuckets: () => ({
        //         method: 'GET',
        //         path: '/_/raft_sessions/1/bucket',
        //         handler: () => this._getBuckets,
        //     }),
        //     getBucketMd: () => ({
        //         method: 'GET',
        //         path: '/default/attributes/bucket1',
        //         handler: () => this._getBucketMd,
        //     }),
        //     listObject: () => ({
        //         method: 'GET',
        //         path: '/default/bucket/bucket1?listingType=Delimiter',
        //         handler: () => this._getBucketMd,
        //     }),
        //     getObjectMD: () => ({
        //         method: 'GET',
        //         
        //     })
        // };
        // const path = {
        //     '/_/raft_sessions/'
        // }
        // const params = {};
    }

    onRequest(req, res) {
        console.log('RECEIVED REQUEST');
        console.log('req url', req.url);
        console.log('req query', req.query);
        console.log('req headers', req.headers);
        const url = URL.parse(req.url);
        const query = querystring.parse(req.query);
        const host = req.headers.host.split(':')[0];
        // this.requestsPerHost[host] += 1;
        if (req.method !== 'GET') {
            res.writeHead(501);
            return res.end(JSON.stringify({
                error: 'mock server only supports GET requests',
            }));
        }
        if (req.url === '/_/raft_sessions/1/bucket') {
            console.log('getting buckets for raft session');
            const value = ['bucket1'];
            res.writeHead(200, { 'content-type': 'application/json' });
            res.write(JSON.stringify(value));
            // return res.end(JSON.stringify(value));
            return res.end();
        } else if (req.url === '/default/attributes/bucket1') {
            console.log('trying to grab md for bucket1');
            const bucketMd = {
                _acl: {
                    Canned: 'private',
                    FULL_CONTROL: [],
                    WRITE: [],
                    WRITE_ACP: [],
                    READ: [],
                    READ_ACP: [] },
                _name: 'xxxfriday10',
                _owner: '94224c921648ada653f584f3caf42654ccf3f1cbd2e569a24e88eb460f2f84d8',
                _ownerDisplayName: 'test_1518720219',
                _creationDate: '2018-02-16T21:55:16.415Z',
                _mdBucketModelVersion: 5,
                _transient: false,
                _deleted: false,
                _serverSideEncryption: null,
                _versioningConfiguration: null,
                _locationConstraint: 'us-east-1',
                _websiteConfiguration: null,
                _replicationConfiguration: null,
                _cors: null,
                _lifecycleConfiguration: null,
                _uid: undefined,
            };
            const dummyBucketMdObj = new BucketInfo(bucketMd._name, bucketMd._owner,
                bucketMd._ownerDisplayName, bucketMd._creationDate,
                bucketMd._mdBucketModelVersion, bucketMd._acl, bucketMd._transient,
                bucketMd._deleted, bucketMd._serverSideEncryption,
                bucketMd.versioningConfiguration, bucketMd._locationContraint,
                bucketMd._websiteConfiguration, bucketMd._cors, bucketMd._lifeCycle);
            console.log('getting bucket metadata');
            console.log('stringify bucketMd', JSON.stringify(bucketMd));
            console.log('stringify dummyObj', dummyBucketMdObj.serialize());
            return res.end(dummyBucketMdObj.serialize());
        } else if (req.url === '/default/bucket/bucket1?listingType=Delimiter') {
            console.log('listing objects in bucket');
            console.log(req.url);
            const objectList = {
                Contents: [
                    { key: 'testobject1',
                    value: JSON.stringify({
                        'owner-display-name': 'test_1518720219',
                        'owner-id':
                            '94224c921648ada653f584f3caf42654ccf3f1cbd2e569a24e88eb460f2f84d8',
                        'content-length': 0,
                        'content-md5': 'd41d8cd98f00b204e9800998ecf8427e',
                        'x-amz-version-id': 'null',
                        'x-amz-server-version-id': '',
                        'x-amz-storage-class': 'STANDARD',
                        'x-amz-server-side-encryption': '',
                        'x-amz-server-side-encryption-aws-kms-key-id': '',
                        'x-amz-server-side-encryption-customer-algorithm': '',
                        'x-amz-website-redirect-location': '',
                        'acl': {
                            Canned: 'private',
                            FULL_CONTROL: [],
                            WRITE_ACP: [],
                            READ: [],
                            READ_ACP: [],
                        },
                        'key': '',
                        'location': null,
                        'isDeleteMarker': false,
                        'tags': {},
                        'replicationInfo': {
                            status: '',
                            backends: [],
                            content: [],
                            destination: '',
                            storageClass: '',
                            role: '',
                            storageType: '',
                            dataStoreVersionId: '',
                        },
                        'dataStoreName': 'us-east-1',
                        'last-modified': '2018-02-16T22:43:37.174Z',
                        'md-model-version': 3,
                    }) },
                ],
            };
            return res.end(JSON.stringify(objectList));
        } else if (req.url === '/default/bucket/bucket1/testobject1?') {
            console.log('getting object metadata');
            return res.end(JSON.stringify({
                metadata: 'dogsAreGood',
            }));
        }
        return res.end(JSON.stringify({
            error: 'invalid path',
        }));
    }
}

describe.only('ingestion producer functional tests with mock', () => {
    let httpServer;
    let metadataMock;

    before(done => {
        console.log('before the ingesion producer test');
        metadataMock = new MetadataMock();
        httpServer = http.createServer(
            (req, res) => metadataMock.onRequest(req, res));
        httpServer.listen(7778);
        done();
        this.iProducer = new IngestionProducer({
            bucketdBootstrap: ['localhost:7778'],
            bucketdLog: undefined,
        });
    });

    after(done => {
        httpServer.close();
        done();
    });

    it('can generate a valid snapshot', done => {
        return this.iProducer.snapshot(1, (err, res) => {
            // we expect 3 logs - 2 logs for the bucket, and 1 log for the obj.
            console.log(res);
            assert.strictEqual(res.length, 3);
            res.forEach(entry => {
                assert(entry.type);
                assert(entry.bucket);
                assert(entry.key);
                assert(entry.value || entry.value === null);
            });
            return done();
        });
    });

    it('can send the snapshot to a generic producer', done => {
        done();
    });

    it('can be consumed by MongoQueueProcessor', done => {
        done();
    });
});
