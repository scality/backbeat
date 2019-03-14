const assert = require('assert');
const uuid = require('uuid/v4');

const ObjectQueueEntry = require('../../../lib/models/ObjectQueueEntry');
const ActionQueueEntry = require('../../../lib/models/ActionQueueEntry');

const ReplicationTaskScheduler =
    require('../../../extensions/replication/utils/ReplicationTaskScheduler');

function getMockQueueEntry(params) {
    const { objectKey, versionId, contentMD5 } = params;
    return new ObjectQueueEntry('test-bucket-name', objectKey)
        .setVersionId(versionId)
        .setContentMd5(contentMD5 || 'd41d8cd98f00b204e9800998ecf8427e');
}

function getMockActionEntry(params) {
    const { objectKey, versionId, contentMD5 } = params;
    return new ActionQueueEntry({
        target: {
            key: `test-bucket-name/${objectKey}`,
            version: versionId,
            contentMd5: contentMD5 || 'd41d8cd98f00b204e9800998ecf8427e',
        }
    });
}

function processingFunc(entry, cb) {
    setTimeout(() => {
        // eslint-disable-next-line no-param-reassign
        entry.object.value = entry.setValueTo;
        cb();
    }, Math.random() * 100);
}

const replicationTaskScheduler = new ReplicationTaskScheduler(processingFunc);

function assertRunningTasksAreCleared() {
    assert.deepStrictEqual(
        Object.keys(replicationTaskScheduler._runningTasksByMasterKey), []);
    assert.deepStrictEqual(
        Object.keys(replicationTaskScheduler._runningTasks), []);
}

const queueEntryClasses = [
    'ObjectQueueEntry',
    'ActionQueueEntry',
];

queueEntryClasses.forEach(mockClass => {
    let results = {};
    let objects = [];
    let doneCount = 0;
    let queueEntryClass;

    function getMockEntry(params) {
        if (queueEntryClass === 'ObjectQueueEntry') {
            return getMockQueueEntry(params);
        }
        if (queueEntryClass === 'ActionQueueEntry') {
            return getMockActionEntry(params);
        }
        return undefined;
    }

    function scheduleEntries(params, cb) {
        const { uniqueKeyCount, hasUniqueVersions, hasUniqueContent } = params;
        const duplicateKeyCount = params.duplicateKeyCount + 1;
        for (let i = 0; i < uniqueKeyCount; ++i) {
            const objectKey = `key_${i}`;
            let versionId = uuid();
            let contentMD5 = uuid();
            for (let j = 0; j < duplicateKeyCount; ++j) {
                if (hasUniqueVersions) {
                    versionId = uuid();
                }
                if (hasUniqueContent) {
                    contentMD5 = uuid();
                }
                const ctx = {
                    object: { objectKey, value: -1 },
                    setValueTo: params.duplicateKeyCount ?
                        (i * duplicateKeyCount) + j : i,
                    entry: getMockEntry({ objectKey, versionId, contentMD5 }),
                };
                objects.push(ctx.object);
                replicationTaskScheduler.push(ctx, objectKey, cb);
            }
        }
    }

    function scheduleDuplicateEntriesRandomized(params, cb) {
        const { uniqueKeyCount, duplicateKeyCount } = params;
        const hash = {};
        for (let i = 0; i < uniqueKeyCount; ++i) {
            const objectKey = `key_${i}`;
            const versionId = uuid();
            const contentMD5 = uuid();
            for (let j = 0; j < duplicateKeyCount + 1; ++j) {
                const setValueTo = uuid();
                const ctx = {
                    setValueTo,
                    object: { objectKey, value: null, setValueTo },
                    entry: getMockEntry({ objectKey, versionId, contentMD5 }),
                };
                hash[setValueTo] = ctx;
            }
        }
        Object.keys(hash)
            .sort()
            .forEach(key => {
                const ctx = hash[key];
                objects.push(ctx.object);
                replicationTaskScheduler.push(ctx, ctx.object.objectKey, cb);
            });
    }

    function testUniqueVersions(params, cb) {
        function doneFunc() {
            ++doneCount;
            if (doneCount !== objects.length) {
                return undefined;
            }
            objects.forEach((obj, i) => assert.strictEqual(obj.value, i));
            return cb();
        }
        scheduleEntries(params, doneFunc);
    }

    function testDuplicateVersions(params, cb) {
        const { uniqueKeyCount } = params;
        function doneFunc() {
            ++doneCount;
            if (doneCount !== objects.length) {
                return undefined;
            }
            objects.forEach((obj, i) => {
                if ((i % (doneCount / uniqueKeyCount)) === 0) {
                    assert.strictEqual(obj.value, i);
                } else {
                    assert.strictEqual(obj.value, -1);
                }
            });
            return cb();
        }
        scheduleEntries(params, doneFunc);
    }

    function testDuplicateVersionsRandomized(params, cb) {
        function doneFunc() {
            ++doneCount;
            if (doneCount !== objects.length) {
                return undefined;
            }
            objects.forEach(object => {
                if (results[object.objectKey]) {
                    assert.strictEqual(object.value, null);
                    assert(object.value !== object.setValueTo);
                } else {
                    results[object.objectKey] = true;
                    assert.strictEqual(object.value, object.setValueTo);
                }
            });
            return cb();
        }
        scheduleDuplicateEntriesRandomized(params, doneFunc);
    }

    describe(`task scheduler with ${mockClass}`, () => {
        before(() => {
            queueEntryClass = mockClass;
        });

        beforeEach(() => {
            results = {};
            objects = [];
            doneCount = 0;
        });

        it('should ensure serialization of updates to unique versioned object',
            done => {
                testUniqueVersions({
                    uniqueKeyCount: 10,
                    duplicateKeyCount: 0,
                }, done);
            });

        it('should ensure serialization of each task if different versions',
            done => {
                testUniqueVersions({
                    uniqueKeyCount: 10,
                    duplicateKeyCount: 9,
                    hasUniqueVersions: true,
                    hasUniqueContent: false,
                }, done);
            });

        it('should ensure serialization of each task if MD5 is different',
            done => {
                testUniqueVersions({
                    uniqueKeyCount: 10,
                    duplicateKeyCount: 9,
                    hasUniqueVersions: false,
                    hasUniqueContent: true,
                }, done);
            });

        it('should ensure serialization if duplicate entries: ordered',
            done => {
                testDuplicateVersions({
                    uniqueKeyCount: 2,
                    duplicateKeyCount: 1,
                    hasUniqueVersions: false,
                    hasUniqueContent: false,
                }, done);
            });

        it('should ensure serialization if duplicate entries: randomized',
            done => {
                testDuplicateVersionsRandomized({
                    uniqueKeyCount: 10,
                    duplicateKeyCount: 9,
                }, done);
            });

        it('should clear the running task queues and filters when finished',
            done => {
                testDuplicateVersionsRandomized({
                    uniqueKeyCount: 10,
                    duplicateKeyCount: 9,
                }, err => {
                    if (err) {
                        return done(err);
                    }
                    // Must wait some time for all queues to finish running.
                    return setTimeout(() => {
                        assertRunningTasksAreCleared();
                        done();
                    }, 1000);
                });
            });

        it('should ensure serialization if duplicate keys and a combination ' +
        'of unique and duplicate versions', done => {
            function doneFunc() {
                ++doneCount;
                if (doneCount !== objects.length) {
                    return undefined;
                }
                assert.deepStrictEqual(objects, [
                    { objectKey: 'key_0', value: 0 },
                    { objectKey: 'key_0', value: 1 },
                    { objectKey: 'key_0', value: -1 },
                ]);
                return done();
            }
            let ctx = {
                object: { objectKey: 'key_0', value: -1 },
                setValueTo: 0,
                entry: getMockEntry({
                    objectKey: 'key_0',
                    versionId: 'a',
                    contentMD5: 'b',
                }),
            };
            objects.push(ctx.object);
            replicationTaskScheduler.push(ctx, ctx.objectKey, doneFunc);

            ctx = {
                object: { objectKey: 'key_0', value: -1 },
                setValueTo: 1,
                entry: getMockEntry({
                    objectKey: 'key_0',
                    versionId: 'b',
                    contentMD5: 'b',
                }),
            };
            objects.push(ctx.object);
            replicationTaskScheduler.push(ctx, ctx.objectKey, doneFunc);

            ctx = {
                object: { objectKey: 'key_0', value: -1 },
                setValueTo: 2,
                entry: getMockEntry({
                    objectKey: 'key_0',
                    versionId: 'b',
                    contentMD5: 'b',
                }),
            };
            objects.push(ctx.object);
            replicationTaskScheduler.push(ctx, ctx.objectKey, doneFunc);
        });
    });
});
