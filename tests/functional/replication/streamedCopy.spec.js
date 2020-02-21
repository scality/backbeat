const http = require('http');
const assert = require('assert');

const werelogs = require('werelogs');
const Logger = werelogs.Logger;

const { ObjectMD } = require('arsenal').models;

const ActionQueueEntry = require('../../../lib/models/ActionQueueEntry');

const QueueProcessor = require('../../../extensions/replication' +
                               '/queueProcessor/QueueProcessor');
const MultipleBackendTask =
      require('../../../extensions/replication/tasks/MultipleBackendTask');
const CopyLocationTask =
      require('../../../extensions/replication/tasks/CopyLocationTask');

const constants = {
    bucket: 'test-bucket',
    objectKey: 'test-object',
    versionId: 'test-object-versionId',
    dataStoreName: 'us-east-1',
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

const mockSourceEntry = {
    getLogInfo: () => ({}),
    getContentLength: () => constants.contents.length,
    getBucket: () => constants.bucket,
    getObjectKey: () => constants.objectKey,
    getEncodedVersionId: () => constants.versionId,
    getDataStoreName: () => constants.dataStoreName,
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

const mockActionEntry = ActionQueueEntry.create('copyLocation')
      .setAttribute('target.bucket', constants.bucket)
      .setAttribute('target.key', constants.objectKey)
      .setAttribute('target.version', constants.versionId)
      .setAttribute('toLocation', 'us-east-2');

const mockObjectMD = new ObjectMD()
      .setContentLength(constants.contents.length);

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
                    `/${constants.objectKey}?operation=${op}`);
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
                res.end();
            });
        } else if (req.method === 'GET') {
            assert.strictEqual(req.url, `/${constants.bucket}/${constants.objectKey}` +
                               `?versionId=${constants.versionId}`);
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

    [{ name: 'MultipleBackendTask::_getAndPutObjectOnce',
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
                               // check that sockets have been properly closed after
                               // an error occurred
                               const agentSockets = qp.sourceHTTPAgent.sockets;
                               assert.strictEqual(Object.keys(agentSockets).length, 0,
                                                  'HTTP source agent still has open sockets');
                           }
                           s3mock.setTestScenario(null);
                           done();
                       }, 0);
                   });
               });
        });
    });
});
