const assert = require('assert');
const sinon = require('sinon');
const async = require('async');

const TaskScheduler = require('../../../../lib/tasks/TaskScheduler');

describe('TaskScheduler', () => {    
    afterEach(() => {
        sinon.restore();
    });

    it('should ensure serialization of updates with the same queue key',
    done => {
        const taskScheduler = new TaskScheduler(
            (entry, done) => {
                setTimeout(() => {
                    assert.strictEqual(
                        entry.object.value, entry.setValueTo - 1);
                    // eslint-disable-next-line no-param-reassign
                    entry.object.value = entry.setValueTo;
                    done();
                }, Math.random() * 100);
            },
            entry => entry.object.queueKey);
        const objects = [];
        let doneCount = 0;
        function doneFunc() {
            ++doneCount;
            if (doneCount === objects.length * 10) {
                done();
            }
        }
        for (let i = 0; i < 10; ++i) {
            objects.push({ queueKey: `key_${i}`, value: -1 });
        }
        for (let value = 0; value < 10; ++value) {
            for (let i = 0; i < 10; ++i) {
                taskScheduler.push({ object: objects[i], setValueTo: value },
                                   doneFunc);
            }
        }
    });

    it('with queue key, should skip extra updates that have an existing ' +
    'dedupe key', done => {
        const taskScheduler = new TaskScheduler(
            (entry, done) => {
                setTimeout(() => {
                    assert.strictEqual(
                        entry.object.value, entry.setValueTo - 1);
                    // eslint-disable-next-line no-param-reassign
                    entry.object.value = entry.setValueTo;
                    done();
                }, Math.random() * 100);
            },
            entry => entry.object.queueKey,
            entry => entry.dedupeKey);
        const objects = [];
        let doneCount = 0;
        function doneFunc() {
            ++doneCount;
            if (doneCount === objects.length * 100) {
                done();
            }
        }
        for (let i = 0; i < 10; ++i) {
            objects.push({ queueKey: `key_${i}`, value: -1 });
        }
        for (let value = 0; value < 100; ++value) {
            for (let i = 0; i < 10; ++i) {
                taskScheduler.push({
                    object: objects[i],
                    setValueTo: value,
                    // all tasks with value >= 10 will have the same
                    // dedupe key than one of the tasks with
                    // value < 10, so should be skipped
                    dedupeKey: `key_${i}_${value % 10}`,
                }, doneFunc);
            }
        }
    });

    it('without queue key, should skip extra updates that have an existing ' +
    'dedupe key', done => {
        const taskScheduler = new TaskScheduler(
            (entry, done) => {
                setTimeout(() => {
                    // there should be only one update per value
                    assert.strictEqual(entry.object.value, -1);
                    // eslint-disable-next-line no-param-reassign
                    entry.object.value = entry.setValueTo;
                    done();
                }, 100 + Math.random() * 100);
            },
            null,
            entry => entry.dedupeKey);
        const objects = [];
        let doneCount = 0;
        function doneFunc() {
            ++doneCount;
            if (doneCount === objects.length * 10) {
                for (let i = 0; i < 10; ++i) {
                    assert.strictEqual(objects[i].value, 0);
                }
                done();
            }
        }
        for (let i = 0; i < 10; ++i) {
            objects.push({ value: -1 });
        }
        for (let value = 0; value < 10; ++value) {
            for (let i = 0; i < 10; ++i) {
                taskScheduler.push({
                    object: objects[i],
                    setValueTo: value,
                    // all tasks with value >= 10 will have the same
                    // dedupe key than one of the tasks with
                    // value < 10, so should be skipped
                    dedupeKey: `key_${i}`,
                }, doneFunc);
            }
        }
    });

    it('should accurately report the number of queued and running tasks', async () => {
        const taskStates = {
            key1: { complete: false },
            key1q: { complete: false },
            key2: { complete: false },
            key3: { complete: false },
            key4: { complete: false },
            nokey1: { complete: false },
            nokey2: { complete: false },
        };
        
        const startSignals = {};
        const completeSignals = {};
        
        // Create promise-based signals for each task
        Object.keys(taskStates).forEach(key => {
            const signals = {};
            startSignals[key] = new Promise(resolve => { signals.startResolve = resolve; });
            completeSignals[key] = new Promise(resolve => { signals.completeResolve = resolve; });
            taskStates[key].signals = signals;
        });
        
        // Create a task processor that signals when it starts and waits for completion signal
        const processTask = async (ctx, callback) => {
            const key = ctx.key;
            taskStates[key].signals.startResolve();
            
            // Wait until we're told to complete
            await completeSignals[key];
            taskStates[key].complete = true;
            callback();
        };
        
        const taskScheduler = new TaskScheduler(
            (ctx, done) => processTask(ctx, done),
            ctx => ctx.queueKey,
            null,
        );
        
        const drainStub = sinon.stub();
        taskScheduler.setDrain(drainStub);

        // Add tasks with keys - these should run in parallel (one per key)
        taskScheduler.push({ key: 'key1', queueKey: 'key1' }, () => {});
        taskScheduler.push({ key: 'key2', queueKey: 'key2' }, () => {});
        taskScheduler.push({ key: 'key3', queueKey: 'key3' }, () => {});
        
        // Add another task with key1 - this should be queued
        taskScheduler.push({ key: 'key1q', queueKey: 'key1' }, () => {});
        
        // Add tasks without keys - these should run in parallel
        taskScheduler.push({ key: 'nokey1' }, () => {});
        taskScheduler.push({ key: 'nokey2' }, () => {});
        
        await Promise.all([
            startSignals['key1'],
            startSignals['key2'], 
            startSignals['key3'],
            startSignals['nokey1'],
            startSignals['nokey2']
        ]);
        
        assert.strictEqual(taskScheduler.running(), 5);
        assert.strictEqual(taskScheduler.length(), 1);
        
        taskStates['key1'].signals.completeResolve();
        await new Promise(resolve => setTimeout(resolve, 10)); // Small delay for processing
        
        // After key1 completes, its queued task should start
        assert.strictEqual(taskScheduler.running(), 5);
        assert.strictEqual(taskScheduler.length(), 0);

        assert(drainStub.notCalled);
        
        // Complete all remaining tasks
        Object.keys(taskStates).forEach(key => {
            if (!taskStates[key].complete) {
                taskStates[key].signals.completeResolve();
            }
        });
        
        // Wait a moment for all tasks to complete processing
        await new Promise(resolve => setTimeout(resolve, 10));

        assert.strictEqual(taskScheduler.running(), 0);
        assert.strictEqual(taskScheduler.length(), 0);
        assert(drainStub.calledOnce);
    });

    describe('_tryDrain', () => {
        it('should call the drain function if there are no tasks in the queue', () => {
            const taskScheduler = new TaskScheduler(() => {});
            const drainStub = sinon.stub();
            taskScheduler.setDrain(drainStub);
            sinon.stub(taskScheduler, 'idle').returns(true);
            taskScheduler._tryDrain();
            assert(drainStub.calledOnce);
        });
        it('should not call the drain function if there are tasks in the queue', () => {
            const taskScheduler = new TaskScheduler(() => {});
            const drainStub = sinon.stub();
            taskScheduler.setDrain(drainStub);
            sinon.stub(taskScheduler, 'idle').returns(false);
            taskScheduler._tryDrain();
            assert(drainStub.notCalled);
        });
        it('should not fail when drain function not defined', () => {
            const taskScheduler = new TaskScheduler(() => {});
            sinon.stub(taskScheduler, 'idle').returns(false);
            assert.doesNotThrow(() => taskScheduler._tryDrain());
        });
    });

    describe('setDrain', () => {
        it('should set the drain function', () => {
            const taskScheduler = new TaskScheduler(() => {});
            const drainStub = sinon.stub();
            taskScheduler.setDrain(drainStub);
            assert.strictEqual(taskScheduler._drainFunc, drainStub);
        });
    });

    describe('idle', () => {
        it('should return true if there are no tasks running or waiting', () => {
            const taskScheduler = new TaskScheduler(() => {});
            taskScheduler._running = 0;
            taskScheduler._inQueue = 0;
            assert(taskScheduler.idle());
        });
        it('should return false if there are tasks running', () => {
            const taskScheduler = new TaskScheduler(() => {});
            taskScheduler._running = 1;
            taskScheduler._inQueue = 0;
            assert(!taskScheduler.idle());
        });
        it('should return false if there are tasks waiting', () => {
            const taskScheduler = new TaskScheduler(() => {});
            taskScheduler._running = 0;
            taskScheduler._inQueue = 1;
            assert(!taskScheduler.idle());
        });
        it('should return false if there are tasks running and waiting', () => {
            const taskScheduler = new TaskScheduler(() => {});
            taskScheduler._running = 1;
            taskScheduler._inQueue = 1;
            assert(!taskScheduler.idle());
        });
    });

    describe('running', () => {
        it('should return the number of tasks currently being processed', () => {
            const taskScheduler = new TaskScheduler(() => {});
            taskScheduler._running = 3;
            assert.strictEqual(taskScheduler.running(), 3);
        });
    });

    describe('length', () => {
        it('should return the number of tasks waiting to be processed', () => {
            const taskScheduler = new TaskScheduler(() => {});
            taskScheduler._inQueue = 5;
            assert.strictEqual(taskScheduler.length(), 5);
        });
    });

    describe('push', () => {
        it('should process a task immediately if there is no queue key and no dedup key', done => {
            const processingFunc = sinon.stub().yields({ data: 'data' });
            const taskScheduler = new TaskScheduler(processingFunc, null, null);
            taskScheduler.push({}, res => {
                assert.strictEqual(res.data, 'data');
                assert(processingFunc.calledOnce);
                assert.strictEqual(taskScheduler._running, 0);
                assert.strictEqual(taskScheduler._inQueue, 0);
                done();
            });
        });
        it('should process a task immediately if there is a queue key and no dedup key', done => {
            const processingFunc = sinon.stub().yields({ data: 'data' });
            const taskScheduler = new TaskScheduler(processingFunc, ctx => ctx.queueKey, null);
            taskScheduler.push({ queueKey: 'key' }, res => {
                assert.strictEqual(res.data, 'data');
                assert(processingFunc.calledOnce);
                assert.strictEqual(taskScheduler._running, 0);
                assert.strictEqual(taskScheduler._inQueue, 0);
                done();
            });
        });
        it('should skip task with existing dedup key', done => {
            let blockedTaskResolve;
            const blockedTaskPromise = new Promise(resolve => { blockedTaskResolve = resolve; });
            const processingFunc = sinon.stub().callsFake((ctx, cb) => {
                if (ctx.id === 'block') {
                    return blockedTaskPromise.then(() => cb());
                }
                return cb();
            });
            const taskScheduler = new TaskScheduler(processingFunc, null, ctx => ctx.dedupeKey);

            const firstTaskCb = sinon.spy();
            taskScheduler.push({ id: 'block', dedupeKey: 'key' }, firstTaskCb);

            const secondTaskCb = sinon.spy();
            taskScheduler.push({ dedupeKey: 'key' }, secondTaskCb);

            process.nextTick(() => {
                assert(processingFunc.calledOnce);
                assert(firstTaskCb.notCalled);
                assert(secondTaskCb.calledOnce);

                blockedTaskResolve();

                setTimeout(() => {
                    assert(firstTaskCb.calledOnce);
                    assert(processingFunc.calledOnce);
                    assert.strictEqual(taskScheduler._running, 0);
                    assert.strictEqual(taskScheduler._inQueue, 0);
                    done();
                }, 10);
            });
        });
        it('should remove dedup key after task completion', done => {
            const processingFunc = sinon.stub().yields();
            const taskScheduler = new TaskScheduler(processingFunc, ctx => ctx.queueKey, null);
            async.series([
                next => taskScheduler.push({ queueKey: 'key' }, next),
                next => taskScheduler.push({ queueKey: 'key' }, next),
            ], () => {
                assert(processingFunc.calledTwice);
                done();
            });
        });
        it('should ignore non string dedup key', done => {
            let blockedTaskResolve;
            const blockedTaskPromise = new Promise(resolve => { blockedTaskResolve = resolve; });
            const processingFunc = sinon.stub().callsFake((ctx, cb) => {
                if (ctx.id === 'block') {
                    return blockedTaskPromise.then(() => cb());
                }
                return cb();
            });
            const taskScheduler = new TaskScheduler(processingFunc, null, ctx => ctx.dedupeKey);

            const firstTaskCb = sinon.spy();
            taskScheduler.push({ id: 'block', dedupeKey: 1 }, firstTaskCb);

            const secondTaskCb = sinon.spy();
            taskScheduler.push({ dedupeKey: 1 }, secondTaskCb);

            process.nextTick(() => {
                assert(processingFunc.calledTwice);
                assert(firstTaskCb.notCalled);
                assert(secondTaskCb.calledOnce);

                blockedTaskResolve();

                setTimeout(() => {
                    assert(firstTaskCb.calledOnce);
                    assert(processingFunc.calledTwice);
                    assert.strictEqual(taskScheduler._running, 0);
                    assert.strictEqual(taskScheduler._inQueue, 0);
                    done();
                }, 10);
            });
        });
        it('should queue task with the same queue key', done => {
            let blockedTaskResolve;
            const blockedTaskPromise = new Promise(resolve => { blockedTaskResolve = resolve; });
            const processingFunc = sinon.stub().callsFake((ctx, cb) => {
                if (ctx.id === 'block') {
                    return blockedTaskPromise.then(() => cb());
                }
                return cb();
            });
            const taskScheduler = new TaskScheduler(processingFunc, ctx => ctx.key);

            const firstTaskCb = sinon.spy();
            taskScheduler.push({ id: 'block', key: 'key' }, firstTaskCb);

            const secondTaskCb = sinon.spy();
            taskScheduler.push({ key: 'key' }, secondTaskCb);

            const thirdTaskCb = sinon.spy();
            taskScheduler.push({ key: 'different-key' }, thirdTaskCb);

            setTimeout(() => {
                assert.strictEqual(taskScheduler._running, 1);
                assert.strictEqual(taskScheduler._inQueue, 1);
                assert(firstTaskCb.notCalled);
                assert(secondTaskCb.notCalled);
                assert(thirdTaskCb.calledOnce);

                blockedTaskResolve();

                setTimeout(() => {
                    assert(firstTaskCb.calledOnce);
                    assert(secondTaskCb.calledOnce);
                    assert(processingFunc.calledThrice);
                    assert.strictEqual(taskScheduler._running, 0);
                    assert.strictEqual(taskScheduler._inQueue, 0);
                    done();
                }, 10);
            }, 10);
        });
        it('should ignore non string queue key', done => {
            let blockedTaskResolve;
            const blockTaskPromise = new Promise(resolve => { blockedTaskResolve = resolve; });
            const processingFunc = sinon.stub().callsFake((ctx, cb) => {
                if (ctx.id === 'block') {
                    return blockTaskPromise.then(() => cb());
                }
                return cb();
            });
            const taskScheduler = new TaskScheduler(processingFunc, ctx => ctx.key);

            const firstTaskCb = sinon.spy();
            taskScheduler.push({ id: 'block', key: 1 }, firstTaskCb);

            const secondTaskCb = sinon.spy();
            taskScheduler.push({ key: 1 }, secondTaskCb);

            setTimeout(() => {
                assert.strictEqual(taskScheduler._running, 1);
                assert.strictEqual(taskScheduler._inQueue, 0);
                assert(firstTaskCb.notCalled);
                assert(secondTaskCb.calledOnce);

                blockedTaskResolve();

                setTimeout(() => {
                    assert(firstTaskCb.calledOnce);
                    assert(processingFunc.calledTwice);
                    assert.strictEqual(taskScheduler._running, 0);
                    assert.strictEqual(taskScheduler._inQueue, 0);
                    done();
                }, 10);
            }, 10);
        });
        it('should call the drain function if there are no tasks in the queue', done => {
            let blockedTaskResolve;
            const blockTaskPromise = new Promise(resolve => { blockedTaskResolve = resolve; });
            const processingFunc = sinon.stub().callsFake((ctx, cb) => {
                if (ctx.id === 'block') {
                    return blockTaskPromise.then(() => cb());
                }
                return cb();
            });
            const taskScheduler = new TaskScheduler(processingFunc);

            const drain = sinon.stub();
            taskScheduler.setDrain(drain);

            const firstTaskCb = sinon.spy();
            taskScheduler.push({ id: 'block' }, firstTaskCb);

            const secondTaskCb = sinon.spy();
            taskScheduler.push({ id: 'block' }, secondTaskCb);

            const thirdTaskCb = sinon.spy();
            taskScheduler.push({ }, thirdTaskCb);

            setTimeout(() => {
                assert.strictEqual(taskScheduler._running, 2);
                assert.strictEqual(taskScheduler._inQueue, 0);
                assert(firstTaskCb.notCalled);
                assert(secondTaskCb.notCalled);
                assert(thirdTaskCb.calledOnce);
                assert(drain.notCalled);

                blockedTaskResolve();

                setTimeout(() => {
                    assert(firstTaskCb.calledOnce);
                    assert(secondTaskCb.calledOnce);
                    assert(drain.calledOnce);
                    assert.strictEqual(taskScheduler._running, 0);
                    assert.strictEqual(taskScheduler._inQueue, 0);
                    done();
                }, 10);
            }, 10);
        });
    });

    describe('_getTaskQueue', () => {
        it('should return an existing task queue', () => {
            const taskScheduler = new TaskScheduler(() => {});
            const queue = async.queue(() => {});
            taskScheduler._taskQueues.key = queue;
            assert.strictEqual(taskScheduler._getTaskQueue({}, 'key'), queue);
        });
        it('should create a new task queue if one does not exist', () => {
            const taskScheduler = new TaskScheduler(() => {});
            const queue = taskScheduler._getTaskQueue({}, 'key');
            assert.strictEqual(taskScheduler._taskQueues.key, queue);
        });
    });

    describe('_queueProcessingFunc', () => {
        it('should decrement the number of tasks in the queue and increment the number of running tasks', () => {
            const taskScheduler = new TaskScheduler(() => {});
            taskScheduler._inQueue = 1;
            taskScheduler._running = 0;
            taskScheduler._queueProcessingFunc();
            assert.strictEqual(taskScheduler._inQueue, 0);
            assert.strictEqual(taskScheduler._running, 1);
        });
    });
});
