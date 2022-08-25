const http = require('http');
const assert = require('assert');

const werelogs = require('werelogs');
const Logger = werelogs.Logger;

const { ObjectMD } = require('arsenal').models;
const { decode } = require('arsenal').versioning.VersionID;

const ActionQueueEntry = require('../../../lib/models/ActionQueueEntry');
const ObjectQueueEntry = require('../../../lib/models/ObjectQueueEntry');

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
    versionId: '7465737456657273696f6e494453757065724c6f6e67',
    dataStoreName: 'us-east-1',
    // data size should be sufficient to have data held in the socket
    // buffers, 10MB seems to work
    contents: Buffer.alloc(10000000).fill('Z'),
};

const objMetadata = {
    'cache-control': undefined,
    'content-disposition': undefined,
    'content-encoding': undefined,
    'content-length': 10000000,
    'content-md5': 'bac4bea7bac4bea7bac4bea7bac4bea7',
    'content-type': 'application/octet-stream',
    'owner-id': 'bart',
    'versionId': decode(constants.versionId),
    'dataStoreName': 'us-east-1',
    'replicationInfo': {
        isNFS: false,
        storageType: 'aws_s3'
    },
    'tags': {},
    'replayCount': 0
};

const emptyObjMetadata = {
    'cache-control': undefined,
    'content-disposition': undefined,
    'content-encoding': undefined,
    'content-length': 10000000,
    'content-md5': 'bac4bea7bac4bea7bac4bea7bac4bea7',
    'content-type': 'application/octet-stream',
    'owner-id': 'bart',
    'versionId': decode(constants.versionId),
    'dataStoreName': 'us-east-1',
    'replicationInfo': {
        isNFS: false,
        storageType: 'aws_s3'
    },
    'tags': {},
    'replayCount': 0
};

class MockQueueEntry extends ObjectQueueEntry {
    setAmzServerSideEncryption() { return {}; }

    setAmzEncryptionKeyId() { return {}; }

    setAmzEncryptionCustomerAlgorithm() { return {}; }

    setReplicationSiteDataStoreVersionId() { return {}; }
}

const fullObj = new MockQueueEntry(
    constants.bucket,
    `${constants.objectKey}\0${objMetadata.versionId}`,
    objMetadata
);

const emptyObj = new MockQueueEntry(
    constants.bucket,
    `${constants.objectKey}\0${emptyObjMetadata.versionId}`,
    emptyObjMetadata
);

const qpParams = [
    'backbeat-func-test-dummy-topic',
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
          site: 'sf', servers: ['127.0.0.2:7777'],
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
    'sf',
];

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

class S3Mock {
    constructor() {
        this.log = new Logger('test:streamedCopy:S3Mock');

        this.testScenario = null;
    }

    setTestScenario(testScenario) {
        this.testScenario = testScenario;
    }

    onRequest(req, res) {
        if (req.method === 'PUT') {
            const expectedUrls = ['putobject', 'putpart'].map(
                op => `/_/backbeat/multiplebackenddata/${constants.bucket}` +
                    `/${constants.objectKey}?operation=${op}`)
                  .concat([`/_/backbeat/data/${constants.bucket}/${constants.objectKey}?v2`]);
            assert(expectedUrls.includes(req.url));
            const chunks = [];
            req.on('data', chunk => {
                if (this.testScenario === 'abortPut') {
                    log.info('aborting PUT request');
                    res.socket.end();
                    res.socket.destroy();
                } else {
                    chunks.push(chunk);
                }
            });
            req.on('end', () => {
                if (req.url.startsWith('/_/backbeat/data/')) {
                    res.write('[{"key":"target-key","dataStoreName":"us-east-2"}]');
                }
                res.end();
            });
        } else if (req.method === 'GET') {
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
        } else {
            assert(false, `did not expect HTTP method ${req.method}`);
        }
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
        httpServer.listen(7777);
        qp = new QueueProcessor(...qpParams);
        qp._mProducer = {
            getProducer: () => ({
                send: () => {},
            }),
            publishMetrics: () => {},
        };
        process.nextTick(done);
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
               fullObj, fullObj, mockPartInfo,
               log.newRequestLogger(), cb);
       },
     },
     { name: 'MultipleBackendTask::_getAndPutObjectOnce',
       call: cb => {
           const mbTask = new MultipleBackendTask(qp);
           mbTask._setupSourceClients('dummyrole', log);
           mbTask._getAndPutObjectOnce(
               fullObj, log.newRequestLogger(), cb);
       },
     },
     { name: 'MultipleBackendTask::_getRangeAndPutMPUPartOnce',
       call: cb => {
           const mbTask = new MultipleBackendTask(qp);
           mbTask._setupSourceClients('dummyrole', log);
           mbTask._getRangeAndPutMPUPartOnce(
               fullObj,
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
               emptyObj, log.newRequestLogger(), cb);
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
