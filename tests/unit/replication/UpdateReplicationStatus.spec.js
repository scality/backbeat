const assert = require('assert');
const sinon = require('sinon');

const UpdateReplicationStatus =
      require('../../../extensions/replication/tasks/UpdateReplicationStatus');
const ObjectQueueEntry =
      require('../../../extensions/replication/utils/ObjectQueueEntry');

const Logger = require('werelogs').Logger;
const logger = new Logger('Backbeat:Replication:UpdateReplicationStatus:tests');

describe('UpdateReplicationStatus', () => {
    it('should update replication status and reset the "originOp" field', done => {
        const mockRSP = {
            getStateVars: sinon.stub().returns({
                repConfig: {
                    replicationStatusProcessor: {},
                },
                sourceConfig: {
                    auth: {},
                },
            }),
        };
        const objMd = {
            replicationInfo: {
                status: 'PENDING',
                backends: [{
                    site: 'sf',
                    status: 'PENDING',
                }],
            },
            originOp: 's3:ObjectCreated:Put',
        };
        const replicationEntry = {
            replicationInfo: {
                status: 'COMPLETED',
                backends: [{
                    site: 'sf',
                    status: 'COMPLETED',
                }],
            },
            originOp: 's3:ObjectCreated:Put',
        };
        const task = new UpdateReplicationStatus(mockRSP, {
            status: sinon.stub(),
        });
        task.backbeatSourceClient = {
            getMetadata: sinon.stub().callsArgWith(2, null, { Body: JSON.stringify(objMd) }),
            putMetadata: (updatedSourceEntry, log, cb) => {
                assert.strictEqual(updatedSourceEntry.getReplicationStatus(), 'COMPLETED');
                // originOp should have been reset before putting metadata
                assert.strictEqual(updatedSourceEntry.getOriginOp(), '');
                cb();
            },
        };
        const sourceEntry = new ObjectQueueEntry('mybucket', 'mykey', replicationEntry);
        sourceEntry.setSite('sf');
        const log = logger.newRequestLogger();
        task._updateReplicationStatus(sourceEntry, log, done);
    });
});
