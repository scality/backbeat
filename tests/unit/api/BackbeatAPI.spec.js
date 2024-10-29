const assert = require('assert');
const sinon = require('sinon');
const MongoClient = require('mongodb').MongoClient;

const BackbeatAPI = require('../../../lib/api/BackbeatAPI');
const BackbeatRequest = require('../../../lib/api/BackbeatRequest');
const ObjectQueueEntry = require('../../../lib/models/ObjectQueueEntry');
const config = require('../../../lib/Config');
const fakeLogger = require('../../utils/fakeLogger');
const setupIngestionSiteMock = require('../../utils/mockIngestionSite');
const locationConfig = require('../../../conf/locationConfig.json');
const { errors } = require('arsenal');

describe('BackbeatAPI', () => {
    let bbapi;

    const destconfig = config.extensions.replication.destination;
    const { site } = destconfig.bootstrapList[0];
    const ingestSite = `${destconfig.bootstrapList[1].site}-ingestion`;
    const coldSite = Object.keys(locationConfig)
        .filter(site => locationConfig[site].isCold)[0];

    before(() => {
        setupIngestionSiteMock();
        bbapi = new BackbeatAPI(config, fakeLogger, { timer: true });
    });

    afterEach(() => {
        sinon.restore();
    });

    // valid routes
    [
        { url: '/_/metrics/crr/all', method: 'GET' },
        { url: '/_/metrics/ingestion/all', method: 'GET' },
        { url: '/_/healthcheck', method: 'GET' },
        { url: '/_/metrics/crr/all/backlog', method: 'GET' },
        { url: '/_/metrics/crr/all/completions', method: 'GET' },
        { url: '/_/metrics/crr/all/failures', method: 'GET' },
        { url: '/_/metrics/crr/all/throughput', method: 'GET' },
        { url: '/_/metrics/ingestion/all/completions', method: 'GET' },
        { url: `/_/metrics/ingestion/${ingestSite}/completions`,
            method: 'GET' },
        { url: '/_/metrics/ingestion/all/throughput', method: 'GET' },
        { url: `/_/metrics/ingestion/${ingestSite}/throughput`, method: 'GET' },
        { url: '/_/metrics/ingestion/all/pending', method: 'GET' },
        { url: `/_/metrics/ingestion/${ingestSite}/pending`, method: 'GET' },
        { url: '/_/monitoring/metrics', method: 'GET' },
        { url: '/_/crr/failed/mybucket/mykey?versionId=test-myvId',
            method: 'GET' },
        { url: '/_/crr/failed/mybucket/mykey?role=testrole', method: 'GET' },
        { url: '/_/crr/failed?mymarker', method: 'GET' },
        // invalid params but will default to getting all buckets
        { url: '/_/crr/failed/mybucket', method: 'GET' },
        { url: '/_/crr/failed', method: 'POST' },
        { url: '/_/crr/pause', method: 'POST' },
        { url: '/_/crr/resume', method: 'POST' },
        { url: '/_/crr/resume/all/schedule', method: 'POST' },
        { url: '/_/crr/resume', method: 'GET' },
        { url: '/_/crr/status', method: 'GET' },
        { url: '/_/ingestion/pause', method: 'POST' },
        { url: '/_/ingestion/resume', method: 'POST' },
        { url: '/_/ingestion/resume/all/schedule', method: 'POST' },
        { url: '/_/ingestion/resume', method: 'GET' },
        { url: '/_/ingestion/status', method: 'GET' },
        { url: '/_/lifecycle/pause', method: 'POST' },
        { url: '/_/lifecycle/resume', method: 'POST' },
        { url: '/_/lifecycle/resume/all/schedule', method: 'POST' },
        { url: '/_/lifecycle/resume', method: 'GET' },
        { url: '/_/lifecycle/status', method: 'GET' },
        { url: `/_/metrics/crr/${site}/throughput/mybucket/mykey` +
            '?versionId=test-myvId', method: 'GET' },
        { url: `/_/metrics/crr/${site}/progress/mybucket/mykey` +
            '?versionId=test-myvId', method: 'GET' },
        // valid site for given service
        { url: `/_/crr/pause/${site}`, method: 'POST' },
        { url: `/_/crr/resume/${site}`, method: 'POST' },
        { url: `/_/crr/status/${site}`, method: 'GET' },
        { url: `/_/ingestion/pause/${ingestSite}`, method: 'POST' },
        { url: `/_/ingestion/resume/${ingestSite}`, method: 'POST' },
        { url: `/_/ingestion/status/${ingestSite}`, method: 'GET' },
        { url: `/_/lifecycle/pause/${coldSite}`, method: 'POST' },
        { url: `/_/lifecycle/resume/${coldSite}`, method: 'POST' },
        { url: `/_/lifecycle/status/${coldSite}`, method: 'GET' },
        { url: '/_/configuration/workflows', method: 'POST' },
    ].forEach(request => {
        it(`should validate route: ${request.method} ${request.url}`, () => {
            const req = new BackbeatRequest(request);
            const routeError = bbapi.findValidRoute(req);

            assert.equal(routeError, null);
        });
    });

    // invalid routes
    [
        { url: '/_/invalid/crr/all', method: 'GET' },
        { url: '/_/metrics/ext/all', method: 'GET' },
        { url: '/_/metrics/crr/test', method: 'GET' },
        { url: '/_/metrics/crr/all/backlo', method: 'GET' },
        { url: '/_/metrics/crr/all/completionss', method: 'GET' },
        { url: '/_/metrics/crr/all/failures', method: 'POST' },
        { url: '/_/metrics/crr/all/fail', method: 'GET' },
        { url: '/_/invalid/crr/all', method: 'GET' },
        { url: '/_/metrics/pause/all', method: 'GET' },
        // unavailable routes for given service
        { url: '/_/metrics/ingestion/all/backlog', method: 'GET' },
        { url: '/_/metrics/ingestion/all/failures', method: 'GET' },
        // invalid http verb
        { url: '/_/healthcheck', method: 'POST' },
        { url: '/_/monitoring/metrics', method: 'POST' },
        { url: '/_/crr/pause', method: 'GET' },
        { url: '/_/crr/status', method: 'POST' },
        { url: '/_/ingestion/pause', method: 'GET' },
        { url: '/_/ingestion/status', method: 'POST' },
        { url: '/_/metrics/crr/unknown-site/throughput/mybucket/mykey' +
            '?versionId=test-myvId', method: 'GET' },
        { url: '/_/metrics/crr/unknown-site/progress/mybucket/mykey' +
            '?versionId=test-myvId', method: 'GET' },
        { url: '/_/metrics/ingestion/all/throughput', method: 'POST' },
        { url: `/_/metrics/ingestion/${site}/completions`, method: 'POST' },
        { url: `/_/metrics/ingestion/${site}/pending`, method: 'POST' },
        // invalid site for given service
        { url: `/_/ingestion/pause/${site}`, method: 'POST' },
        { url: `/_/ingestion/resume/${site}`, method: 'POST' },
        { url: `/_/ingestion/status/${site}`, method: 'GET' },
        // invalid site for given service
        { url: `/_/crr/pause/${ingestSite}`, method: 'POST' },
        { url: `/_/crr/resume/${ingestSite}`, method: 'POST' },
        { url: `/_/crr/status/${ingestSite}`, method: 'GET' },
        { url: `/_/ingestion/pause/${site}`, method: 'POST' },
        { url: `/_/ingestion/resume/${site}`, method: 'POST' },
        { url: `/_/ingestion/status/${site}`, method: 'GET' },
        { url: '/_/lifecycle/pause/non-existent', method: 'POST' },
        { url: '/_/lifecycle/resume/non-existent', method: 'POST' },
        { url: '/_/lifecycle/status/non-existent', method: 'GET' },
    ].forEach(request => {
        it(`should invalidate route: ${request.method} ${request.url}`, () => {
            const req = new BackbeatRequest(request);
            const routeError = bbapi.findValidRoute(req);

            assert(routeError);
        });
    });

    it('should calculate the average throughput through redis intervals',
    () => {
        bbapi._getData = function overwriteGetData(details, data, cb) {
            return cb(null, [
                // ops
                {
                    requests: [25, 0, 0, 25],
                    errors: [0, 0, 0, 0],
                },
                // bytes
                {
                    requests: [1024, 0, 0, 1024],
                    errors: [0, 0, 0, 0],
                },
            ]);
        };

        bbapi.getThroughput({ service: 'crr' }, (err, data) => {
            assert.ifError(err);

            assert(data.throughput);
            assert(data.throughput.description);
            assert(data.throughput.results);

            const results = data.throughput.results;
            const count = Number.parseFloat(results.count);
            const size = Number.parseFloat(results.size);
            // count must be at least 0.03 given the first interval and never be
            // over double its size. This can only happen when interval time has
            // just reset and the just-expired interval data fully applies
            assert(count >= 0.03 && count <= 0.06);
            assert(size >= 1.14 && size <= 2.28);
        });
    });

    [
        {
            details: {
                service: 'lifecycle',
                site: 'all',
                extensions: {
                    lifecycle: [
                        'us-east-1',
                        'us-east-2',
                        'location-dmf-v1',
                        'all',
                    ],
                },
            },
            expected: [
                'us-east-1',
                'us-east-2',
                'location-dmf-v1',
            ]
        },
        {
            details: {
                service: 'lifecycle',
                site: 'us-east-1',
                extensions: {
                    lifecycle: [
                        'us-east-1',
                        'us-east-2',
                        'location-dmf-v1',
                        'all',
                    ],
                },
            },
            expected: ['us-east-1']
        },
    ].forEach(params => {
        it('getRequestedSites:: should return correct list of requested sites', () => {
            const sites = bbapi.getRequestedSites(params.details);
            assert.deepStrictEqual(sites, params.expected);
        });
    });

    describe('_setupMongoClient', () => {
        let mongoClientStub;
        let infoSpy;
        let errorSpy;
        let debugSpy;

        beforeEach(() => {
            mongoClientStub = sinon.stub(MongoClient, 'connect');
            infoSpy = sinon.spy(fakeLogger, 'info');
            errorSpy = sinon.spy(fakeLogger, 'error');
            debugSpy = sinon.spy(fakeLogger, 'debug');
        });

        afterEach(() => {
            mongoClientStub.restore();
            infoSpy.restore();
            errorSpy.restore();
            debugSpy.restore();
        });

        it('should connect to MongoDB when configuration is present', done => {
            const mockDb = { db: sinon.stub().returns({}) };
            mongoClientStub.yields(null, mockDb);

            bbapi._setupMongoClient(err => {
                assert.ifError(err);
                assert(mongoClientStub.calledOnce);
                assert(infoSpy.calledWith('Connected to MongoDB', {
                    method: 'BackbeatAPI._setupMongoClient',
                }));
                done();
            });
        });

        it('should log an error when MongoDB connection fails', done => {
            const mockError = new Error('Connection failed');
            mongoClientStub.yields(mockError);

            bbapi._setupMongoClient(err => {
                assert.strictEqual(err, mockError);
                assert(mongoClientStub.calledOnce);
                assert(errorSpy.calledWith('Could not connect to MongoDB', {
                    method: 'BackbeatAPI._setupMongoClient',
                    error: mockError.message,
                }));
                done();
            });
        });

        it('should skip MongoDB client setup when configuration is not present', done => {
            delete bbapi._config.queuePopulator.mongo;

            bbapi._setupMongoClient(err => {
                assert.ifError(err);
                assert(mongoClientStub.notCalled);
                assert(debugSpy.calledWith('MongoDB configuration not found, skipping MongoDB client setup', {
                    method: 'BackbeatAPI._setupMongoClient',
                }));
                done();
            });
        });
    });

    describe('setupInternals', () => {
        let setZookeeperStub;
        let setupMongoClientStub;
        let setProducerStub;
        let setupLocationStatusManagerStub;
        let errorSpy;
        let debugSpy;

        beforeEach(() => {
            setZookeeperStub = sinon.stub(bbapi, '_setZookeeper').yields(null);
            setupMongoClientStub = sinon.stub(bbapi, '_setupMongoClient').yields(null);
            setProducerStub = sinon.stub(bbapi, '_setProducer').yields(null, {});
            setupLocationStatusManagerStub = sinon.stub(bbapi, '_setupLocationStatusManager').yields(null);
            errorSpy = sinon.spy(fakeLogger, 'error');
            debugSpy = sinon.spy(fakeLogger, 'debug');
        });

        afterEach(() => {
            setZookeeperStub.restore();
            setupMongoClientStub.restore();
            setProducerStub.restore();
            setupLocationStatusManagerStub.restore();
            errorSpy.restore();
            debugSpy.restore();
        });

        it('should setup internals when MongoDB configuration exists', done => {
            bbapi._config.queuePopulator.mongo = {
                host: 'localhost',
                port: 27017,
                db: 'backbeat',
            };
            bbapi.setupInternals(err => {
                assert.ifError(err);
                assert(setZookeeperStub.calledOnce);
                assert(setupMongoClientStub.calledOnce);
                assert(setProducerStub.calledThrice);
                assert(setupLocationStatusManagerStub.calledOnce);
                done();
            });
        });

        it('should setup internals when MongoDB configuration does not exist', done => {
            delete bbapi._config.queuePopulator.mongo;

            bbapi.setupInternals(err => {
                assert.ifError(err);
                assert(setZookeeperStub.calledOnce);
                assert(setupMongoClientStub.calledOnce);
                assert(setProducerStub.calledThrice);
                assert(setupLocationStatusManagerStub.notCalled);
                done();
            });
        });
    });

    describe('validateQuery', () => {
        it('should return an error if marker is invalid', () => {
            const req = new BackbeatRequest({
                url: '/_/crr/failed?marker=invalid',
                method: 'GET',
            });
            const routeError = bbapi.validateQuery(req);
            assert.deepEqual(routeError, errors.InvalidQueryParameter);
        });

        it('should return an error if sitename is invalid', () => {
            const req = new BackbeatRequest({
                url: '/_/crr/failed?sitename=',
                method: 'GET',
            });
            const routeError = bbapi.validateQuery(req);
            assert.deepEqual(routeError, errors.InvalidQueryParameter);
        });

        it('should return an error if role is invalid', () => {
            const req = new BackbeatRequest({
                url: '/_/crr/failed?role=',
                method: 'GET',
            });
            const routeError = bbapi.validateQuery(req);
            assert.deepEqual(routeError, errors.InvalidQueryParameter);
        });

        it('should return null if query is valid', () => {
            const req = new BackbeatRequest({
                url: '/_/crr/failed?marker=1&sitename=site1&role=replication',
                method: 'GET',
            });
            const routeError = bbapi.validateQuery(req);
            assert.strictEqual(routeError, null);
        });
    });

    describe('getFailedCRR', () => {
        it('should use the role in redis key', done => {
            const stub = sinon.stub(bbapi, '_getEntriesAcrossSites').yields();
            sinon.stub(bbapi, '_getFailedCRRResponse').yields();

            const details = {
                bucket: 'mybucket',
                key: 'mykey',
                role: 'replication',
            };
            bbapi.getFailedCRR(details, err => {
                assert.ifError(err);
                assert.strictEqual(stub.getCall(0).args.at(0), 'mybucket:mykey::replication');
                done();
            });
        });

        it('should use proper redis key if role not specified', done => {
            const stub = sinon.stub(bbapi, '_getEntriesAcrossSites').yields();
            sinon.stub(bbapi, '_getFailedCRRResponse').yields();

            const details = {
                bucket: 'mybucket',
                key: 'mykey',
            };
            bbapi.getFailedCRR(details, err => {
                assert.ifError(err);
                assert.strictEqual(stub.getCall(0).args.at(0), 'mybucket:mykey:');
                done();
            });
        });
    });

    describe('_getObjectQueueEntry', () => {
        it('should setup role on backbeatMetadataProxy if auth type is "role"', done => {
            const setupSourceRole = sinon.stub().returns();
            sinon.stub(bbapi, '_repConfig').value({
                source: {
                    auth: {
                        type: 'role',
                    },
                },
            });
            sinon.stub(bbapi, '_backbeatMetadataProxy').value({
                setupSourceRole,
                setSourceClient: sinon.stub().returns({
                    getMetadata: sinon.stub().yields(null, {
                        Body: JSON.stringify({}),
                    }),
                }),
            });
            const entry = {
                getBucket: () => 'mybucket',
                getObjectKey: () => 'mykey',
                getEncodedVersionId: () => 'myvId',
                getSite: () => 'site1',
                getReplicationRoles: () => 'role1',
                getLogInfo: () => 'entry',
            };
            bbapi._getObjectQueueEntry(entry, err => {
                assert.ifError(err);
                assert(setupSourceRole.calledOnce);
                done();
            });
        });

        it('should not setup role on backbeatMetadataProxy if auth type is not "role"', done => {
            const setupSourceRole = sinon.stub().returns();
            sinon.stub(bbapi, '_repConfig').value({
                source: {
                    auth: {
                        type: 'service',
                    },
                },
            });
            sinon.stub(bbapi, '_backbeatMetadataProxy').value({
                setupSourceRole,
                setSourceClient: sinon.stub().returns({
                    getMetadata: sinon.stub().yields(null, {
                        Body: JSON.stringify({}),
                    }),
                }),
            });
            const entry = {
                getBucket: () => 'mybucket',
                getObjectKey: () => 'mykey',
                getEncodedVersionId: () => 'myvId',
                getSite: () => 'site1',
                getReplicationRoles: () => 'role1',
                getLogInfo: () => 'entry',
            };
            bbapi._getObjectQueueEntry(entry, err => {
                assert.ifError(err);
                assert(setupSourceRole.notCalled);
                done();
            });
        });
    });

    describe('_getFailedCRRResponse', () => {
        it('should include role in the resposnse when auth type is "role"', done => {
            const objectMD = {
                'md-model-version': 2,
                'owner-display-name': 'Bart',
                'owner-id': '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be',
                'x-amz-storage-class': 'STANDARD',
                'content-length': 542,
                'content-type': 'text/plain',
                'last-modified': '2017-07-13T02:44:25.515Z',
                'content-md5': '01064f35c238bd2b785e34508c3d27f4',
                'key': 'object',
                'isDeleteMarker': false,
                'isNull': false,
                'dataStoreName': 'STANDARD',
                'originOp': 's3:ObjectRestore:Post',
                'versionId': 'test-myvId',
                'replicationInfo': {
                    role: 'replication',
                    status: 'SUCCESS',
                    backends: [{
                            site: 'azure',
                            status: 'SUCCESS',
                            dataStoreVersionId: '123456',
                    }],
                },
            };
            const entry = new ObjectQueueEntry('mybucket', 'mykey', objectMD);
            sinon.stub(bbapi, '_getObjectQueueEntry').yields(null, entry);
            sinon.stub(bbapi, '_repConfig').value({
                source: {
                    auth: {
                        type: 'role',
                    },
                },
            });
            const expectedResponse = {
                IsTruncated: false,
                Versions: [{
                    Bucket: 'mybucket',
                    Key: 'mykey',
                    VersionId: '746573742d6d79764964',
                    StorageClass: null,
                    Size: 542,
                    LastModified: '2017-07-13T02:44:25.515Z',
                    Role: 'replication'
                }],
            };
            bbapi._getFailedCRRResponse(undefined, [entry], (err, resp) => {
                assert.ifError(err);
                assert.deepEqual(resp, expectedResponse);
                done();
            });
        });
    });

    describe('_parseRetryFailedCRR', () => {
        it('should fail if the "Role" property is not included when auth type is "role"', () => {
            const body = JSON.stringify([{
                Bucket: 'mybucket',
                Key: 'mykey',
                StorageClass: 'replication',
            }]);
            sinon.stub(bbapi, '_repConfig').value({
                source: {
                    auth: {
                        type: 'role',
                    },
                },
            });
            const res = bbapi._parseRetryFailedCRR(body);
            assert(res.error);
        });

        it('should succeed if the "Role" property is included when auth type is "role"', () => {
            const body = JSON.stringify([{
                Bucket: 'mybucket',
                Key: 'mykey',
                StorageClass: 'replication',
                Role: 'replication',
            }]);
            sinon.stub(bbapi, '_repConfig').value({
                source: {
                    auth: {
                        type: 'role',
                    },
                },
            });
            const res = bbapi._parseRetryFailedCRR(body);
            assert.strictEqual(res.error, undefined);
        });

        it('should succeed if the "Role" property is not included when auth type is not "role"', () => {
            const body = JSON.stringify([{
                Bucket: 'mybucket',
                Key: 'mykey',
                StorageClass: 'replication',
            }]);
            sinon.stub(bbapi, '_repConfig').value({
                source: {
                    auth: {
                        type: 'service',
                    },
                },
            });
            const res = bbapi._parseRetryFailedCRR(body);
            assert.strictEqual(res.error, undefined);
        });
    });

    describe('retryFailedCRR', () => {
        it('should include the role in the key when searching redis', done => {
            const body = JSON.stringify([{
                Bucket: 'mybucket',
                Key: 'mykey',
                StorageClass: 'replication',
                Role: 'replication',
            }]);
            sinon.stub(bbapi, '_repConfig').value({
                source: {
                    auth: {
                        type: 'role',
                    },
                },
            });
            const zscore = sinon.stub(bbapi._redisClient, 'zscore').yields(null, null);
            bbapi.retryFailedCRR(undefined, body, err => {
                assert.ifError(err);
                assert.strictEqual(zscore.getCall(0).args.at(1), 'mybucket:mykey::replication');
                done();
            });
        });
    });
});
