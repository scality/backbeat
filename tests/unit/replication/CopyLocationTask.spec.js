const assert = require('assert');
const sinon = require('sinon');

const CopyLocationTask = require('../../../extensions/replication/tasks/CopyLocationTask');
const ActionQueueEntry = require('../../../lib/models/ActionQueueEntry');
const { errors } = require('arsenal');
const { ObjectMD } = require('arsenal').models;

const fakeLogger = require('../../utils/fakeLogger');

describe('CopyLocationTask', () => {
    describe('_checkObjectState', () => {
        let task;

        beforeEach(() => {
            task = new CopyLocationTask({
                getStateVars: () => ({
                    site: 'test-site',
                    mProducer: {
                        getProducer: () => {},
                    }
                }),
            });
        });

        it('should return invalidState error object has been changed', () => {
            const objMd = new ObjectMD();
            objMd.setContentMd5('1234-9');

            const entry = new ActionQueueEntry({
                target: {
                    eTag: '"156781-9"',
                },
            });

            const res = task._checkObjectState(entry, objMd);
            assert(res.InvalidObjectState);
        });

        it('should return invalidState error when object already transitioned', () => {
            const objMd = new ObjectMD();
            objMd.setDataStoreName('test-site');

            const entry = new ActionQueueEntry({
                target: {},
                toLocation: 'test-site',
            });

            const res = task._checkObjectState(entry, objMd);
            assert(res.InvalidObjectState);
        });

        it('should not return error if object is valid', () => {
            const objMd = new ObjectMD();
            objMd.setDataStoreName('STANDARD');
            objMd.setContentMd5('1234-9');

            const entry = new ActionQueueEntry({
                target: {
                    eTag: '"1234-9"',
                },
                toLocation: 'test-site',
            });

            const res = task._checkObjectState(entry, objMd);
            assert.equal(res, null);
        });
    });

    describe('_publishCopyLocationStatus', () => {
        let task;

        beforeEach(() => {
            task = new CopyLocationTask({
                getStateVars: () => ({
                    site: 'test-site',
                    mProducer: {
                        getProducer: () => {},
                    }
                }),
            });
        });

        it('should skip object if object state is invalid', () => {
            const entry = new ActionQueueEntry({
                target: {
                    key: 'key',
                    eTag: '"1234-9"',
                },
                toLocation: 'test-site',
            });

            task.replicationStatusProducer = sinon.stub().yields();

            const res = task._publishCopyLocationStatus(errors.InvalidObjectState, entry, null, fakeLogger);
            assert.strictEqual(res.committable, true);
            assert(task.replicationStatusProducer.notCalled);
        });
    });
});
