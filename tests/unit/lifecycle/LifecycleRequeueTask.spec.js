'use strict';

const assert = require('assert');
const werelogs = require('werelogs');

const { ObjectMD } = require('arsenal').models;
const ActionQueueEntry = require('../../../lib/models/ActionQueueEntry');
const { LifecycleResetTransitionInProgressTask } = require(
    '../../../extensions/lifecycle/tasks/LifecycleResetTransitionInProgressTask');

const {
    BackbeatMetadataProxyMock,
    ProcessorMock,
} = require('../mocks');

const makeMd = () => new ObjectMD()
    .setContentMd5('etag1')
    .setTransitionInProgress(true)
    .setUserMetadata({
        'x-amz-meta-scal-s3-transition-attempt': 0,
    });

describe('LifecycleRequeueTask::handleBatch', () => {
    let backbeatMetadataProxyClient;
    let task;

    beforeEach(() => {
        backbeatMetadataProxyClient = new BackbeatMetadataProxyMock();

        const objectProcessor = new ProcessorMock(
            null,
            null,
            null,
            backbeatMetadataProxyClient,
            null,
            null,
            null,
            new werelogs.Logger('test:LifecycleRequeueTask'));

        task = new LifecycleResetTransitionInProgressTask(objectProcessor);
    });

    describe('ObjNotFound handling in batch', () => {
        it('should skip deleted objects and continue processing valid ones', done => {
            const batchEntry = ActionQueueEntry.create('requeueTransition')
                .setAttribute('target', {
                    byAccount: {
                        123: {
                            bucket1: [
                                { objectKey: 'obj1', objectVersion: 'v1', eTag: '"etag1"', try: 1 },
                                { objectKey: 'obj2', objectVersion: 'v2', eTag: '"etag1"', try: 1 },
                                { objectKey: 'obj3', objectVersion: 'v3', eTag: '"etag1"', try: 1 },
                            ],
                        },
                    },
                });

            backbeatMetadataProxyClient.setMdObjForKey('obj1', makeMd());
            backbeatMetadataProxyClient.setErrorForKey('obj2', { name: 'ObjNotFound' });
            backbeatMetadataProxyClient.setMdObjForKey('obj3', makeMd());

            task.processActionEntry(batchEntry, err => {
                assert.ifError(err);
                assert.deepStrictEqual(
                    backbeatMetadataProxyClient._putCalls.sort(),
                    ['obj1', 'obj3'],
                );
                done();
            });
        });

        it('should complete without error when all objects are deleted', done => {
            const batchEntry = ActionQueueEntry.create('requeueTransition')
                .setAttribute('target', {
                    byAccount: {
                        123: {
                            bucket1: [
                                { objectKey: 'obj1', objectVersion: 'v1', eTag: '"etag1"', try: 1 },
                                { objectKey: 'obj2', objectVersion: 'v2', eTag: '"etag1"', try: 1 },
                            ],
                        },
                    },
                });

            backbeatMetadataProxyClient.setErrorForKey('obj1', { name: 'ObjNotFound' });
            backbeatMetadataProxyClient.setErrorForKey('obj2', { name: 'ObjNotFound' });

            task.processActionEntry(batchEntry, err => {
                assert.ifError(err);
                assert.deepStrictEqual(backbeatMetadataProxyClient._putCalls, []);
                done();
            });
        });

        it('should abort batch on non-ObjNotFound errors', done => {
            const batchEntry = ActionQueueEntry.create('requeueTransition')
                .setAttribute('target', {
                    byAccount: {
                        123: {
                            bucket1: [
                                { objectKey: 'obj1', objectVersion: 'v1', eTag: '"etag1"', try: 1 },
                                { objectKey: 'obj2', objectVersion: 'v2', eTag: '"etag1"', try: 1 },
                            ],
                        },
                    },
                });

            backbeatMetadataProxyClient.setErrorForKey('obj1', { name: 'InternalError' });
            backbeatMetadataProxyClient.setMdObjForKey('obj2', makeMd());

            task.processActionEntry(batchEntry, err => {
                assert.ok(err);
                assert.strictEqual(err.name, 'InternalError');
                assert.deepStrictEqual(backbeatMetadataProxyClient._putCalls, []);
                done();
            });
        });
    });

    describe('NoSuchBucket handling in batch', () => {
        it('should skip objects from deleted bucket and continue other buckets', done => {
            const batchEntry = ActionQueueEntry.create('requeueTransition')
                .setAttribute('target', {
                    byAccount: {
                        123: {
                            deletedBucket: [
                                { objectKey: 'obj1', objectVersion: 'v1', eTag: '"etag1"', try: 1 },
                            ],
                            validBucket: [
                                { objectKey: 'obj2', objectVersion: 'v2', eTag: '"etag1"', try: 1 },
                            ],
                        },
                    },
                });

            backbeatMetadataProxyClient.setErrorForKey('obj1', { name: 'NoSuchBucket' });
            backbeatMetadataProxyClient.setMdObjForKey('obj2', makeMd());

            task.processActionEntry(batchEntry, err => {
                assert.ifError(err);
                assert.deepStrictEqual(backbeatMetadataProxyClient._putCalls, ['obj2']);
                done();
            });
        });

        it('should complete without error when all objects are in a deleted bucket', done => {
            const batchEntry = ActionQueueEntry.create('requeueTransition')
                .setAttribute('target', {
                    byAccount: {
                        123: {
                            deletedBucket: [
                                { objectKey: 'obj1', objectVersion: 'v1', eTag: '"etag1"', try: 1 },
                                { objectKey: 'obj2', objectVersion: 'v2', eTag: '"etag1"', try: 1 },
                            ],
                        },
                    },
                });

            backbeatMetadataProxyClient.setErrorForKey('obj1', { name: 'NoSuchBucket' });
            backbeatMetadataProxyClient.setErrorForKey('obj2', { name: 'NoSuchBucket' });

            task.processActionEntry(batchEntry, err => {
                assert.ifError(err);
                assert.deepStrictEqual(backbeatMetadataProxyClient._putCalls, []);
                done();
            });
        });
    });
});
