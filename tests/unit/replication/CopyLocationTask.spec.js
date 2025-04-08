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

    describe('_initiateMPU', () => {
        let task;
        beforeEach(() => {
            task = new CopyLocationTask({
                getStateVars: () => ({
                    site: 'test-site',
                    mProducer: {
                        getProducer: () => {},
                    },
                }),
            });
        });
        it('should init mpu when location type is not azure', done => {
            const entry = new ActionQueueEntry({
                target: {
                    key: 'key',
                    eTag: '"1234-9"',
                },
                toLocation: 'test-site',
            });
            task.backbeatClient = {
                multipleBackendInitiateMPU: sinon.stub().returns({
                    send: cb => cb(null, {}),
                    on: () => {},
                }),
            };
            task.destConfig = {
                replicationEndpoint: {
                    site: 'test-site',
                    type: 'aws_s3',
                },
            };
            task._initiateMPU(entry, new ObjectMD(), fakeLogger, err => {
                assert.ifError(err);
                assert(task.backbeatClient.multipleBackendInitiateMPU.calledOnce);
                done();
            });
        });
        it('should not init mpu when location type is azure', done => {
            const entry = new ActionQueueEntry({
                target: {
                    key: 'key',
                    eTag: '"1234-9"',
                },
                toLocation: 'test-site',
            });
            task.backbeatClient = {
                multipleBackendInitiateMPU: sinon.stub().returns({
                    send: cb => cb(null, {}),
                    on: () => {},
                }),
            };
            task.destConfig = {
                replicationEndpoint: {
                    site: 'test-site',
                    type: 'azure',
                },
            };
            task._initiateMPU(entry, new ObjectMD(), fakeLogger, err => {
                assert.ifError(err);
                assert(task.backbeatClient.multipleBackendInitiateMPU.notCalled);
                done();
            });
        });
    });

    describe('_completeRangedMPU', () => {
        let task;
        beforeEach(() => {
            task = new CopyLocationTask({
                getStateVars: () => ({
                    site: 'test-site',
                    repConfig: {
                        queueProcessor: {
                            mpuPartsConcurrency: 2,
                        },
                    },
                    mProducer: {
                        getProducer: () => {},
                    },
                }),
            });
        });
        it('should abort MPU on part upload error', done => {
            const entry = new ActionQueueEntry({
                target: {
                    key: 'key',
                    eTag: '"1234-9"',
                },
                toLocation: 'test-site',
            });
            const objectMD = new ObjectMD();
            objectMD.setContentLength(200);
            
            sinon.stub(task, '_getRanges').returns([
                { start: 0, end: 100 },
                { start: 101, end: 199 }
            ]);
            
            const putRangeFunc = sinon.stub(task, '_getRangeAndPutMPUPart');
            putRangeFunc.onCall(0).yields(null, {
                partNumber: 0,
                ETag: 'etag1',
            });
            putRangeFunc.onCall(1).yields(new Error('Upload failed'));
            
            const abortMpuFunc = sinon.stub(task, '_multipleBackendAbortMPU').yields();
            const completeMpuFunc = sinon.stub(task, '_completeMPU').yields();
            
            task.destConfig = {
                replicationEndpoint: {
                    site: 'test-site',
                    type: 'aws_s3',
                },
            };
            
            const uploadId = 'test-upload-id';
            task._completeRangedMPU(entry, objectMD, uploadId, fakeLogger, err => {
                assert(err);
                assert.strictEqual(err.message, 'Upload failed');
                assert(abortMpuFunc.calledOnce);
                assert(abortMpuFunc.calledWith(
                    entry, objectMD, uploadId, fakeLogger, sinon.match.func
                ));
                assert(completeMpuFunc.notCalled);
                done();
            });
        });
            
        it('should handle Azure special case for MPU parts', done => {
            const entry = new ActionQueueEntry({
                target: {
                    key: 'key',
                    eTag: '"1234-9"',
                },
                toLocation: 'test-site',
            });
            const objectMD = new ObjectMD();
            objectMD.setContentLength(200);
            
            sinon.stub(task, '_getRanges').returns([
                { start: 0, end: 100 },
                { start: 101, end: 199 }
            ]);
            
            const putRangeFunc = sinon.stub(task, '_getRangeAndPutMPUPart');
            putRangeFunc.onCall(0).yields(null, {
                partNumber: 0,
                ETag: 'etag1',
                numberSubParts: 2
            });
            putRangeFunc.onCall(1).yields(null, {
                partNumber: 1,
                ETag: 'etag2',
                numberSubParts: 1
            });
            
            const completeMpuFunc = sinon.stub(task, '_completeMPU').yields();
            
            task.destConfig = {
                replicationEndpoint: {
                    site: 'test-site',
                    type: 'azure',
                },
            };
            
            task._completeRangedMPU(entry, objectMD, 'test-upload-id', fakeLogger, err => {
                assert.ifError(err);
                assert(completeMpuFunc.calledOnce);
                const completionData = completeMpuFunc.firstCall.args[3];
                assert(completionData[0].NumberSubParts);
                assert(completionData[1].NumberSubParts);
                done();
            });
        });
    });

    describe('constructor', () => {
        it('should use retry config of the relevent type', () => {
            const task = new CopyLocationTask({
                getStateVars: () => ({
                    mProducer: {
                        getProducer: () => {},
                    },
                    repConfig: {
                        queueProcessor: {
                            retry: {
                                scality: {
                                    maxRetries: 13,
                                },
                                azure: {
                                    maxRetries: 5,
                                },
                            },
                        },
                    },
                    destConfig: {
                        replicationEndpoint: {
                            site: 'test-site',
                            type: 'scality',
                        }
                    },
                }),
            });
            assert.strictEqual(task.retryParams.maxRetries, 13);
        });
    });
});
