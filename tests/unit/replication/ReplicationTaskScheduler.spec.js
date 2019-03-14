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

const queueEntryClasses = [
    'ObjectQueueEntry',
    'ActionQueueEntry',
];

queueEntryClasses.forEach(mockClass => {
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

    // function scheduleDuplicateVersions(params, cb) {
    //     const { uniqueKeyCount, duplicateKeyCount } = params;
    //     for (let i = 0; i < uniqueKeyCount; ++i) {
    //         objects.push({ objectKey: `key_${i}`, value: -1 });
    //         // the following inner operations shall be executed in
    //         // order because they have the same objectKey (passed
    //         // to replicationTaskScheduler.push())
    //         const versionId = uuid();
    //         for (let j = 0; j < duplicateKeyCount + 1; ++j) {
    //             const ctx = {
    //                 object: objects[i],
    //                 setValueTo: j,
    //                 entry: getMockEntry({
    //                     objectKey: objects[i].objectKey,
    //                     versionId,
    //                     contentMD5: uuid(),
    //                 }),
    //             };
    //             replicationTaskScheduler.push(ctx, objects[i].objectKey, cb);
    //         }
    //     }
    // }
    // 
    // // function testDuplicateVersions(params, cb) {
    // //     const { uniqueKeyCount, duplicateKeyCount } = params;
    // //     function doneFunc() {
    // //         ++doneCount;
    // //         if (doneCount === objects.length * uniqueKeyCount) {
    // //             objects.forEach(obj => {
    // //                 assert.strictEqual(obj.value, duplicateKeyCount);
    // //             });
    // //             return cb();
    // //         }
    // //         return undefined;
    // //     }
    // //     scheduleDuplicateVersions(params, doneFunc);
    // // }

    function scheduleUniqueVersions(params, cb) {
        const { uniqueKeyCount, hasUniqueVersions, hasUniqueContent } = params;
        const duplicateKeyCount =
            params.duplicateKeyCount ? params.duplicateKeyCount + 1 : 1;
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

    function testUniqueVersions(params, cb) {
        function doneFunc() {
            ++doneCount;
            if (doneCount !== objects.length) {
                return undefined;
            }
            objects.forEach((obj, i) => assert.strictEqual(obj.value, i));
            return cb();
        }
        scheduleUniqueVersions(params, doneFunc);
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
        scheduleUniqueVersions(params, doneFunc);
    }

    describe.only(`task scheduler with ${mockClass}`, () => {
        before(() => {
            queueEntryClass = mockClass;
        });

        beforeEach(() => {
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

        it('should ensure serialization if duplicate keys',
            done => {
                testDuplicateVersions({
                    uniqueKeyCount: 10,
                    duplicateKeyCount: 10,
                    hasUniqueVersions: false,
                    hasUniqueContent: false,
                }, done);
            });
    });
});
