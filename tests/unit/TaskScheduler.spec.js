const assert = require('assert');
const crypto = require('crypto');

const TaskScheduler = require('../../extensions/utils/TaskScheduler');

function getRandomHash() {
    return crypto.createHash('md5')
        .update(Math.random().toString())
        .digest('hex');
}

function getMockQueueEntry(params) {
    const {
        isReplicationOperation,
        isLifecycleOperation,
        versionId,
        contentMD5,
        objectKey,
    } = params;
    return {
        isReplicationOperation: () => isReplicationOperation === true,
        isLifecycleOperation: () => isLifecycleOperation === true,
        getVersionId: () => versionId,
        getContentMd5: () => contentMD5 || 'd41d8cd98f00b204e9800998ecf8427e',
        getCanonicalKey: () => `test-bucket-name/${objectKey}`,
    };
}

describe('task scheduler', () => {
    const taskScheduler = new TaskScheduler((entry, cb) => {
        setTimeout(() => {
            // eslint-disable-next-line no-param-reassign
            entry.object.value = entry.setValueTo;
            cb();
        }, Math.random() * 100);
    });
    let objects = [];
    let doneCount = 0;

    afterEach(() => {
        objects = [];
        doneCount = 0;
    });

    describe('replication tasks', () => {
        it('should ensure serialization of updates to the same versioned ' +
        'object', done => {
            function doneFunc() {
                ++doneCount;
                if (doneCount === objects.length * 10) {
                    objects.forEach(obj => {
                        assert.strictEqual(obj.value, 9);
                    });
                    return done();
                }
                return undefined;
            }
            for (let i = 0; i < 10; ++i) {
                objects.push({ objectKey: `key_${i}`, value: -1 });
                // the following inner operations shall be executed in
                // order because they have the same objectKey (passed
                // to taskScheduler.push())
                for (let j = 0; j < 10; ++j) {
                    const ctx = {
                        object: objects[i],
                        setValueTo: j,
                        entry: getMockQueueEntry({
                            isReplicationOperation: true,
                            objectKey: objects[i].objectKey,
                        }),
                    };
                    taskScheduler.push(ctx, doneFunc);
                }
            }
        });
    });

    describe('lifecycle tasks', () => {
        // Push the entries to a queue and wait for all to be processed.
        function pushLifecycleEntries(params, cb) {
            // Check that all the objects have been updated to a unique value.
            function doneFunc() {
                ++doneCount;
                if (doneCount !== objects.length) {
                    return undefined;
                }
                objects.forEach((obj, i) => assert.strictEqual(obj.value, i));
                return cb();
            }
            const {
                uniqueKeyCount,
                duplicateKeyCount,
                hasUniqueVersions,
                hasUniqueContent,
                hasDuplicateKeys,
                customDoneFunc,
            } = params;
            for (let i = 0; i < uniqueKeyCount; ++i) {
                for (let j = 0; j < duplicateKeyCount; ++j) {
                    const objectKey = `key_${i}`;
                    const object = {
                        objectKey,
                        value: -1,
                    };
                    const setValueTo = hasDuplicateKeys ?
                        (i * duplicateKeyCount) + j : i;
                    const ctx = {
                        object,
                        setValueTo,
                        entry: getMockQueueEntry({
                            isLifecycleOperation: true,
                            objectKey,
                            versionId: hasUniqueVersions && getRandomHash(),
                            contentMD5: hasUniqueContent && getRandomHash(),
                        }),
                    };
                    objects.push(object);
                    taskScheduler.push(ctx, customDoneFunc || doneFunc);
                }
            }
        }

        it('should each task when keys are unique', done => {
            pushLifecycleEntries({
                uniqueKeyCount: 10,
                duplicateKeyCount: 1,
            }, done);
        });

        it('should process each task if different versions', done => {
            pushLifecycleEntries({
                uniqueKeyCount: 10,
                duplicateKeyCount: 10,
                hasUniqueVersions: true,
                hasDuplicateKeys: true,
            }, done);
        });

        it('should process each task if content MD5 is different', done => {
            pushLifecycleEntries({
                uniqueKeyCount: 10,
                duplicateKeyCount: 10,
                hasUniqueContent: true,
                hasDuplicateKeys: true,
            }, done);
        });

        it('should process each task but skip duplicates', done => {
            const uniqueKeyCount = 10;
            const duplicateKeyCount = 10;
            // Check that all the objects have been updated to a unique value.
            function doneFunc() {
                ++doneCount;
                if (doneCount !== objects.length) {
                    return undefined;
                }
                objects.forEach((object, i) => {
                    // If it's not the first unique object, the value should not
                    // have changed.
                    if (i % duplicateKeyCount) {
                        assert.strictEqual(object.value, -1);
                    } else {
                        const expectedValue = i / duplicateKeyCount;
                        assert.strictEqual(object.value, expectedValue);
                    }
                });
                return done();
            }
            pushLifecycleEntries({
                uniqueKeyCount,
                duplicateKeyCount,
                customDoneFunc: doneFunc,
            });
        });
    });
});
