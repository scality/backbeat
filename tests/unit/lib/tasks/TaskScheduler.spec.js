const assert = require('assert');

const TaskScheduler = require('../../../../lib/tasks/TaskScheduler');

describe('TaskScheduler', () => {
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
});
