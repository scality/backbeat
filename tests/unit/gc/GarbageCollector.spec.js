'use strict'; // eslint-disable-line

const assert = require('assert');
const http = require('http');

const GarbageCollector = require('../../../extensions/gc/GarbageCollector');
const GarbageCollectorTask =
      require('../../../extensions/gc/tasks/GarbageCollectorTask');
const ActionQueueEntry = require('../../../lib/models/ActionQueueEntry');

describe('garbage collector', () => {
    let gc;
    let gcTask;
    let httpServer;
    let expectBatchDeleteLocations;

    before(() => {
        gc = new GarbageCollector({
            kafkaConfig: {},
            s3Config: {
                host: 'localhost',
                port: 7777,
            },
            gcConfig: {
                topic: 'backbeat-gc',
                auth: {
                    type: 'account',
                    account: 'bart',
                },
                consumer: {
                    groupId: 'backbeat-gc-consumer-group',
                },
            },
        });
        gcTask = new GarbageCollectorTask(gc);

        httpServer = http.createServer(
            (req, res) => {
                if (expectBatchDeleteLocations === null) {
                    assert.fail('did not expect a batch delete request');
                }
                assert.strictEqual(req.url, '/_/backbeat/batchdelete');
                const buffers = [];
                req.on('data', data => {
                    buffers.push(data);
                });
                req.on('end', () => {
                    const reqObj = JSON.parse(
                        Buffer.concat(buffers).toString());
                    assert.deepStrictEqual(expectBatchDeleteLocations,
                                           reqObj.Locations);
                    res.end();
                });
            });
        httpServer.listen(7777);
    });
    after(() => {
        httpServer.close();
    });
    it('should skip unsupported action type', done => {
        expectBatchDeleteLocations = null;
        const action = ActionQueueEntry.create('foo');
        gcTask.processActionEntry(action, done);
    });
    it('should send batch delete request with locations array with ' +
    'no dataStoreVersionId', done => {
        expectBatchDeleteLocations = [{
            key: 'foo',
            dataStoreName: 'ds',
            size: 10,
        }];
        const action = ActionQueueEntry.create('deleteData')
              .setAttribute('target.locations', expectBatchDeleteLocations);
        gcTask.processActionEntry(action, done);
    });
    it('should send batch delete request with locations array with ' +
    'some dataStoreVersionId', done => {
        expectBatchDeleteLocations = [{
            key: 'foo',
            dataStoreName: 'ds',
            size: 10,
            dataStoreVersionId: 'someversion',
        }];
        const action = ActionQueueEntry.create('deleteData')
              .setAttribute('target.locations', expectBatchDeleteLocations);
        gcTask.processActionEntry(action, done);
    });
});
