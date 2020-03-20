const http = require('http');
const assert = require('assert');

const werelogs = require('werelogs');
const Logger = werelogs.Logger;

const { ObjectMD } = require('arsenal').models;

const config = require('../../../lib/Config');

const ActionQueueEntry = require('../../../lib/models/ActionQueueEntry');

const QueueProcessor = require('../../../extensions/replication' +
                               '/queueProcessor/QueueProcessor');
const ReplicateObject =
      require('../../../extensions/replication/tasks/ReplicateObject');
const MultipleBackendTask =
      require('../../../extensions/replication/tasks/MultipleBackendTask');
const CopyLocationTask =
      require('../../../extensions/replication/tasks/CopyLocationTask');

const constants = {
    bucket: 'test-bucket',
    objectKey: 'test-object',
    versionId: 'test-object-versionId',
    sourceLocation: 'zenko-source-location',
    targetLocation: 'zenko-target-location',
    // data size should be sufficient to have data held in the socket
    // buffers, 10MB seems to work
    contents: Buffer.alloc(10000000).fill('Z'),
};

const qpParams = [
    { connectionString: '127.0.0.1:2181/backbeat',
      autoCreateNamespace: false },
    null,
    { hosts: 'localhost:9092' },
    { auth: { type: 'account', account: 'bart' },
      s3: { host: '127.0.0.1',
            port: 7777 },
      transport: 'http',
    },
    { auth: { type: 'account', account: 'bart' },
      bootstrapList: [{
          site: 'zenko-target-location', servers: ['127.0.0.2:7777'],
      }],
      transport: 'http' },
    { topic: 'backbeat-func-test-dummy-topic',
      replicationStatusTopic: 'backbeat-func-test-repstatus',
      queueProcessor: {
          retry: {
              scality: { timeoutS: 5 },
          },
          groupId: 'backbeat-func-test-group-id',
      },
    },
    undefined,
    { topic: 'metrics-test-topic' },
    {},
    {},
    'zenko-target-location',
];

const mockSourceEntry = {
    getLogInfo: () => ({}),
    getContentLength: () => constants.contents.length,
    getBucket: () => constants.bucket,
    getObjectKey: () => constants.objectKey,
    getEncodedVersionId: () => constants.versionId,
    getDataStoreName: () => constants.sourceLocation,
    getReplicationIsNFS: () => false,
    getOwnerId: () => 'bart',
    getContentMd5: () => 'bac4bea7bac4bea7bac4bea7bac4bea7',
    getReplicationStorageType: () => 'aws_s3',
    getUserMetadata: () => {},
    getContentType: () => 'application/octet-stream',
    getCacheControl: () => undefined,
    getContentDisposition: () => undefined,
    getContentEncoding: () => undefined,
    getTags: () => {},
    setReplicationSiteDataStoreVersionId: () => {},
};

const mockEmptySourceEntry = {
    getLogInfo: () => ({}),
    getContentLength: () => 0,
    getBucket: () => constants.bucket,
    getObjectKey: () => constants.objectKey,
    getEncodedVersionId: () => constants.versionId,
    getDataStoreName: () => constants.targetLocation,
    getReplicationIsNFS: () => false,
    getOwnerId: () => 'bart',
    getContentMd5: () => 'bac4bea7bac4bea7bac4bea7bac4bea7',
    getReplicationStorageType: () => 'aws_s3',
    getUserMetadata: () => {},
    getContentType: () => 'application/octet-stream',
    getCacheControl: () => undefined,
    getContentDisposition: () => undefined,
    getContentEncoding: () => undefined,
    getTags: () => {},
    setReplicationSiteDataStoreVersionId: () => {},
};

const mockPartInfo = {
    key: 'bac4bea7bac4bea7bac4bea7bac4bea7bac4bea7',
    start: 0,
    size: constants.contents.length,
    dataStoreName: 'us-east-1',
    dataStoreETag: '1:',
};

const mockActionEntry = ActionQueueEntry.create('copyLocation')
      .setAttribute('target.bucket', constants.bucket)
      .setAttribute('target.key', constants.objectKey)
      .setAttribute('target.version', constants.versionId)
      .setAttribute('toLocation', 'us-east-2');

const mockObjectMD = new ObjectMD()
      .setContentLength(constants.contents.length);

const mockEmptyObjectMD = new ObjectMD()
      .setContentLength(0);

const log = new Logger('test:streamedCopy');

const locationConstraints = {
    'zenko-source-location': {
        locationType: 'location-scality-ring-s3-v1',
        objectId: '44bf13a4-4c4e-11ea-bb25-f2291cdcb467',
        details: {
            credentials: {
                accessKey: '4OU7W5SI61NM37797059',
                secretKey: 'qHDO1NUBC9OH1a0cbRk7ghue3Gb9aQsAwEc7PHBi'
            },
            bucketName: 'zenko-source-bucket',
            serverSideEncryption: false,
            awsEndpoint: '127.0.0.3:7777',
            supportsVersioning: true,
            pathStyle: true,
            https: false
        },
        type: 'aws_s3',
        sizeLimitGB: null,
        isTransient: false,
        legacyAwsBehavior: false
    },
    'zenko-target-location': {
        locationType: 'location-scality-ring-s3-v1',
        objectId: '927057d9-4c4b-11ea-b5a1-6e95057283c8',
        details: {
            credentials: {
                accessKey: '4OU7W5SI61NM37797059',
                secretKey: 'qHDO1NUBC9OH1a0cbRk7ghue3Gb9aQsAwEc7PHBi'
            },
            bucketName: 'zenko-target-bucket',
            serverSideEncryption: false,
            awsEndpoint: '127.0.0.4:7777',
            supportsVersioning: true,
            pathStyle: true,
            https: false
        },
        type: 'aws_s3',
        sizeLimitGB: null,
        isTransient: false,
        legacyAwsBehavior: false
    },
};

class S3Mock {
    constructor() {
        this.log = new Logger('test:streamedCopy:S3Mock');

        this.testScenario = null;
    }

    setTestScenario(testScenario) {
        this.testScenario = testScenario;
    }

    onRequestToCloudserver(req, res) {
        if (req.method === 'GET') {
            const expectedUrls = ['', 'partNumber=1&'].map(
                partArg => `/${constants.bucket}/${constants.objectKey}` +
                    `?${partArg}versionId=${constants.versionId}`);
            assert(expectedUrls.includes(req.url));
            if (this.testScenario === 'abortGet') {
                log.info('aborting GET request');
                res.socket.end();
                res.socket.destroy();
            } else {
                res.write(constants.contents);
                res.end();
            }
        } else if (req.method === 'PUT') {
            const expectedUrls = ['putobject', 'putpart'].map(
                op => `/_/backbeat/multiplebackenddata/${constants.bucket}` +
                    `/${constants.objectKey}?operation=${op}`)
                  .concat([`/_/backbeat/data/${constants.bucket}` +
                           `/${constants.objectKey}`]);
            assert(expectedUrls.includes(req.url));
            req.on('data', () => {
                if (this.testScenario === 'abortPut') {
                    log.info('aborting PUT request');
                    res.socket.end();
                    res.socket.destroy();
                }
            });
            req.on('end', () => {
                if (req.url.startsWith('/_/backbeat/data/')) {
                    res.write('[{"key":"target-key","dataStoreName":"us-east-2"}]');
                }
                res.end();
            });
        } else {
            assert(false, `did not expect HTTP method ${req.method}`);
        }
    }

    onRequestToSourceLocation(req, res) {
        if (req.method === 'GET') {
            if (req.url === '/zenko-source-bucket?location') {
                res.end('<?xml version="1.0" encoding="UTF-8"?>'
                        + '<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
                        + `${constants.sourceLocation}`
                        + '</LocationConstraint>');
            } else if (req.url === '/zenko-source-bucket/test-bucket/test-object'
                       + '?versionId=test-object-versionId') {
                if (this.testScenario === 'abortGet') {
                    log.info('aborting GET request');
                    res.socket.end();
                    res.socket.destroy();
                } else {
                    res.write(constants.contents);
                    res.end();
                }
            } else {
                assert(false, `did not expect HTTP GET on source location URL ${req.url}`);
            }
        } else {
            assert(false, `did not expect HTTP method ${req.method} on source location`);
        }
    }

    onRequestToTargetLocation(req, res) {
        if (req.method === 'GET') {
            if (req.url === '/zenko-target-bucket?location') {
                res.end('<?xml version="1.0" encoding="UTF-8"?>'
                        + '<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
                        + `${constants.targetLocation}`
                        + '</LocationConstraint>');
            } else {
                assert(false, `did not expect HTTP GET on target location URL ${req.url}`);
            }
        } else if (req.method === 'POST') {
            req.on('data', () => {
                if (this.testScenario === 'abortPut') {
                    log.info('aborting PUT request');
                    res.socket.end();
                    res.socket.destroy();
                }
            });
            req.on('end', () => {
                if (req.url === '/zenko-target-bucket/test-bucket/test-object?uploads') {
                    res.end('<?xml version="1.0" encoding="UTF-8"?>'
                            + '<InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
                            + `<Bucket>${locationConstraints[constants.targetLocation].details.bucketName}</Bucket>`
                            + `<Key>${constants.bucket}/${constants.objectKey}</Key>`
                            + '<UploadId>test-upload-id</UploadId>'
                            + '</InitiateMultipartUploadResult>');
                } else if (req.url === '/zenko-target-bucket/test-bucket/test-object?uploadId=test-upload-id') {
                    res.setHeader('x-amz-version-id', 'target-version');
                    res.end('<?xml version="1.0" encoding="UTF-8"?>'
                            + '<CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
                            + `<Location>http://127.0.0.4:7777/${constants.bucket}/${constants.objectKey}</Location>`
                            + `<Bucket>${constants.bucket}</Bucket>`
                            + `<Key>${constants.objectKey}</Key>`
                            + '<ETag>&quot;22545f0b26f292a02ae12dcde07a6094-2&quot;</ETag>'
                            + '</CompleteMultipartUploadResult>');
                } else {
                    assert(false, `did not expect HTTP method ${req.method} on target location URL ${req.url}`);
                }
            });
        } else if (req.method === 'PUT') {
            req.on('data', () => {});
            req.on('end', () => {
                if (req.url === '/zenko-target-bucket/test-bucket/test-object') {
                    res.setHeader('x-amz-version-id', 'target-version');
                    res.setHeader('etag', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa');
                    res.end();
                } else if (req.url.startsWith('/zenko-target-bucket/test-bucket/test-object?partNumber=')) {
                    res.setHeader('etag', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa');
                    res.end();
                } else {
                    assert(false, `did not expect HTTP method ${req.method} on target location URL ${req.url}`);
                }
            });
        } else if (req.method === 'DELETE') {
            if (req.url === '/zenko-target-bucket/test-bucket/test-object?uploadId=test-upload-id') {
                res.writeHead(204);
                res.end();
            } else {
                assert(false, `did not expect HTTP DELETE on target location URL ${req.url}`);
            }
        } else {
            assert(false, `did not expect HTTP method ${req.method} on target location`);
        }
    }

    onRequest(req, res) {
        const host = req.headers.host.split(':')[0];
        if (host === '127.0.0.1' || host === '127.0.0.2') {
            return this.onRequestToCloudserver(req, res);
        }
        if (host === '127.0.0.3') {
            return this.onRequestToSourceLocation(req, res);
        }
        if (host === '127.0.0.4') {
            return this.onRequestToTargetLocation(req, res);
        }
        assert(false, `did not expect HTTP request to ${host} (${req.method} ${req.url})`);
        return undefined;
    }
}

describe('streamed copy functional tests', () => {
    let httpServer;
    let s3mock;
    let qp;

    beforeEach(done => {
        s3mock = new S3Mock();
        httpServer = http.createServer(
            (req, res) => s3mock.onRequest(req, res));
        httpServer.listen(7777, () => {
            config.setLocationConstraints(locationConstraints);
            done();
        });
        qp = new QueueProcessor(...qpParams);
        qp._mProducer = {
            getProducer: () => ({
                send: () => {},
            }),
            publishMetrics: () => {},
        };
    });

    afterEach(done => {
        httpServer.close();
        process.nextTick(done);
    });

    [{ name: 'ReplicateObject::_getAndPutPartOnce',
       call: cb => {
           const repTask = new ReplicateObject(qp);
           repTask._setupSourceClients('dummyrole', log);
           repTask._setupDestClients('dummyrole', log);
           repTask._getAndPutPartOnce(
               mockSourceEntry, mockSourceEntry, mockPartInfo,
               log.newRequestLogger(), cb);
       },
     },
     { name: 'MultipleBackendTask::_getAndPutObjectOnce',
       call: cb => {
           const mbTask = new MultipleBackendTask(qp);
           mbTask._setupSourceClients('dummyrole', log);
           mbTask._getAndPutObjectOnce(
               mockSourceEntry, log.newRequestLogger(), cb);
       },
     },
     { name: 'MultipleBackendTask::_getRangeAndPutMPUPartOnce',
       call: cb => {
           const mbTask = new MultipleBackendTask(qp);
           mbTask._setupSourceClients('dummyrole', log);
           mbTask._getRangeAndPutMPUPartOnce(
               mockSourceEntry,
               { start: 0, end: constants.contents.length - 1 },
               1, 'uploadId', log.newRequestLogger(), cb);
       },
     },
     { name: 'CopyLocationTask::_getAndPutObjectOnce',
       call: cb => {
           const clTask = new CopyLocationTask(qp);
           clTask._setupClients(mockActionEntry, log);
           clTask._getAndPutObjectOnce(
               mockActionEntry, mockObjectMD, log.newRequestLogger(), cb);
       },
     },
     { name: 'CopyLocationTask::_getRangeAndPutMPUPartOnce',
       call: cb => {
           const clTask = new CopyLocationTask(qp);
           clTask._setupClients(mockActionEntry, log);
           clTask._getRangeAndPutMPUPartOnce(
               mockActionEntry, mockObjectMD,
               { start: 0, end: constants.contents.length - 1 },
               1, 'uploadId', log.newRequestLogger(), cb);
       },
     }].forEach(testedFunc => {
         ['noError', 'abortGet', 'abortPut'].forEach(testScenario => {
             it(`${testedFunc.name} with test scenario ${testScenario}`,
               done => {
                   s3mock.setTestScenario(testScenario);
                   testedFunc.call(err => {
                       if (testScenario === 'noError') {
                           assert.ifError(err);
                       } else {
                           assert(err);
                       }
                       // socket processing in the agent is asynchronous, so
                       // we have to make the check asynchronous too
                       setTimeout(() => {
                           if (testScenario === 'abortGet' ||
                               testScenario === 'abortPut') {
                               // check that sockets have been properly
                               // closed after an error occurred
                               const srcAgentSockets = qp.sourceHTTPAgent.sockets;
                               assert.strictEqual(
                                   Object.keys(srcAgentSockets).length, 0,
                                   'HTTP source agent still has open sockets');
                               const dstAgentSockets = qp.destHTTPAgent.sockets;
                               assert.strictEqual(
                                   Object.keys(dstAgentSockets).length, 0,
                                   'HTTP dest agent still has open sockets');
                           }
                           s3mock.setTestScenario(null);
                           done();
                       }, 0);
                   });
               });
        });
    });

    [{ name: 'MultipleBackendTask::_getAndPutObjectOnce (empty object)',
       call: cb => {
           const mbTask = new MultipleBackendTask(qp);
           mbTask._setupSourceClients('dummyrole', log);
           mbTask._getAndPutObjectOnce(
               mockEmptySourceEntry, log.newRequestLogger(), cb);
       },
     },
     { name: 'CopyLocationTask::_getAndPutObjectOnce (empty object)',
       call: cb => {
           const clTask = new CopyLocationTask(qp);
           clTask._setupClients(mockActionEntry, log);
           clTask._getAndPutObjectOnce(
               mockActionEntry, mockEmptyObjectMD, log.newRequestLogger(), cb);
       },
     }].forEach(testedFunc => {
         ['noError'].forEach(testScenario => {
             it(`${testedFunc.name} with test scenario ${testScenario}`,
                done => {
                    s3mock.setTestScenario(testScenario);
                    testedFunc.call(err => {
                        assert.ifError(err);
                        s3mock.setTestScenario(null);
                        done();
                    });
                });
         });
     });
});
