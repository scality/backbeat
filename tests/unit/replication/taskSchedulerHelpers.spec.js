const assert = require('assert');

const ObjectQueueEntry = require('../../../lib/models/ObjectQueueEntry');
const ActionQueueEntry = require('../../../lib/models/ActionQueueEntry');
const { ObjectMD } = require('arsenal').models;

const { getTaskSchedulerQueueKey, getTaskSchedulerDedupeKey } = require(
    '../../../extensions/replication/queueProcessor/taskSchedulerHelpers');

function makeObjectQueueEntry({ bucket, key, versionId, contentMd5 }) {
    const objMd = new ObjectMD().setKey(key);
    return new ObjectQueueEntry(bucket, key, objMd)
        .setVersionId(versionId)
        .setContentMd5(contentMd5);
}

function makeActionQueueEntry({ bucket, key, versionId, eTag }) {
    return new ActionQueueEntry({
        action: 'copyLocation',
        target: {
            bucket,
            key,
            version: versionId,
            eTag,
        }
    });
}

function makeQueueEntry(entryClass, { bucket, key, versionId, contentMd5 }) {
    if (entryClass === 'ObjectQueueEntry') {
        return makeObjectQueueEntry({ bucket, key, versionId, contentMd5 });
    }
    if (entryClass === 'ActionQueueEntry') {
        return makeActionQueueEntry({ bucket, key, versionId,
                                      eTag: `"${contentMd5}"` });
    }
    return assert.fail(`bad class ${entryClass}`);
}

function makeTestEntry(entryClass,
                       { bucketSelector, keySelector, versionIdSelector,
                         contentMd5Selector }) {
    const buckets = ['test-bucket-name', 'other-bucket-name'];
    const keys = ['masterkey1', 'masterkey2'];
    const versionIds = ['abcdef', 'ghijkl'];
    const contentMd5s = ['d41d8cd98f00b204e9800998ecf8427e',
                         '93b07384d113edec49eaa6238ad5ff00'];
    return makeQueueEntry(
        entryClass, {
            bucket: buckets[bucketSelector],
            key: keys[keySelector],
            versionId: versionIds[versionIdSelector],
            contentMd5: contentMd5s[contentMd5Selector],
        });
}

function makeTestEntryPair(entryClass, { distinctBucket, distinctKey,
                                         distinctVersionId,
                                         distinctContentMd5 }) {
    return [
        makeTestEntry(entryClass, { bucketSelector: 0,
                                    keySelector: 0,
                                    versionIdSelector: 0,
                                    contentMd5Selector: 0 }),
        makeTestEntry(entryClass, {
            bucketSelector: distinctBucket ? 1 : 0,
            keySelector: distinctKey ? 1 : 0,
            versionIdSelector: distinctVersionId ? 1 : 0,
            contentMd5Selector: distinctContentMd5 ? 1 : 0,
        })];
}

function makeQueueKeyPair(entryClass, params) {
    const [entry1, entry2] = makeTestEntryPair(entryClass, params);
    return [getTaskSchedulerQueueKey(entry1),
            getTaskSchedulerQueueKey(entry2)];
}

function makeDedupeKeyPair(entryClass, params) {
    const [entry1, entry2] = makeTestEntryPair(entryClass, params);
    return [getTaskSchedulerDedupeKey(entry1),
            getTaskSchedulerDedupeKey(entry2)];
}

describe('QueueProcessor::getTaskSchedulerQueueKey', () => {
    ['ObjectQueueEntry', 'ActionQueueEntry'].forEach(entryClass => {
        it(`should return matching keys of ${entryClass} with same master key`,
        () => {
            const [queueKey1, queueKey2] = makeQueueKeyPair(
                entryClass, {
                    distinctVersionId: true,
                    distinctContentMd5: true,
                });
            assert.strictEqual(queueKey1, queueKey2);
        });

        it(`should return different keys of ${entryClass} with different ` +
        'master keys', () => {
            const [queueKey1, queueKey2] = makeQueueKeyPair(
                entryClass, {
                    distinctKey: true,
                    distinctVersionId: true,
                    distinctContentMd5: true,
                });
            assert.notStrictEqual(queueKey1, queueKey2);
        });

        it(`should return different keys of ${entryClass} with the same ` +
        'master key in different buckets', () => {
            const [queueKey1, queueKey2] = makeQueueKeyPair(
                entryClass, {
                    distinctBucket: true,
                });
            assert.notStrictEqual(queueKey1, queueKey2);
        });
    });
});

describe('QueueProcessor::getTaskSchedulerDedupeKey', () => {
    ['ObjectQueueEntry', 'ActionQueueEntry'].forEach(entryClass => {
        it(`should return matching keys of ${entryClass} with same ` +
           'master-key/version/md5', () => {
               const [dedupeKey1, dedupeKey2] = makeDedupeKeyPair(
                   entryClass, {
                   });
               assert.strictEqual(dedupeKey1, dedupeKey2);
           });

        it(`should return different keys of ${entryClass} with different ` +
        'master key', () => {
            const [dedupeKey1, dedupeKey2] = makeDedupeKeyPair(
                entryClass, {
                    distinctKey: true,
                });
               assert.notStrictEqual(dedupeKey1, dedupeKey2);
           });

        it(`should return different keys of ${entryClass} with different ` +
        'version', () => {
            const [dedupeKey1, dedupeKey2] = makeDedupeKeyPair(
                entryClass, {
                    distinctVersionId: true,
                });
            assert.notStrictEqual(dedupeKey1, dedupeKey2);
        });

        it(`should return different keys of ${entryClass} with different ` +
        'md5', () => {
            const [dedupeKey1, dedupeKey2] = makeDedupeKeyPair(
                entryClass, {
                    distinctContentMd5: true,
                });
            assert.notStrictEqual(dedupeKey1, dedupeKey2);
        });

        it(`should return different keys of ${entryClass} with the same ` +
        'master-key/version/md5 in different buckets', () => {
            const [dedupeKey1, dedupeKey2] = makeDedupeKeyPair(
                entryClass, {
                    distinctBucket: true,
                });
            assert.notStrictEqual(dedupeKey1, dedupeKey2);
        });
    });

});

describe('QueueProcessor::getTaskSchedulerDedupeKey of copyLocation actions',
() => {
    const objectParams = {
        bucket: 'test-bucket-name',
        key: 'index.html',
        eTag: '"d41d8cd98f00b204e9800998ecf8427e"',
    };

    it('should return different keys for the same key of non-versioned ' +
    'objects in different buckets', () => {
        const entry1 = makeActionQueueEntry(objectParams);
        const entry2 = makeActionQueueEntry(
            Object.assign({}, objectParams, { bucket: 'other-bucket-name' }));
        assert.notStrictEqual(getTaskSchedulerDedupeKey(entry1),
                              getTaskSchedulerDedupeKey(entry2));
    });

    it('should return different keys for a non-versioned object ' +
    'overwritten with new contents', () => {
        const entry1 = makeActionQueueEntry(objectParams);
        const entry2 = makeActionQueueEntry(
            Object.assign({}, objectParams,
                          { eTag: '"93b07384d113edec49eaa6238ad5ff00"' }));
        assert.notStrictEqual(getTaskSchedulerDedupeKey(entry1),
                              getTaskSchedulerDedupeKey(entry2));
    });

    it('should return matching keys for duplicates of the same action', () => {
        assert.strictEqual(
            getTaskSchedulerDedupeKey(makeActionQueueEntry(objectParams)),
            getTaskSchedulerDedupeKey(makeActionQueueEntry(objectParams)));
    });

    it('should return matching keys whether the eTag is quoted or not',
    () => {
        const quoted = makeActionQueueEntry(objectParams);
        const unquoted = makeActionQueueEntry(
            Object.assign({}, objectParams,
                          { eTag: 'd41d8cd98f00b204e9800998ecf8427e' }));
        assert.strictEqual(getTaskSchedulerDedupeKey(quoted),
                           getTaskSchedulerDedupeKey(unquoted));
    });

    it('should return matching keys for actions without an eTag', () => {
        const params = Object.assign({}, objectParams, { eTag: undefined });
        assert.strictEqual(
            getTaskSchedulerDedupeKey(makeActionQueueEntry(params)),
            getTaskSchedulerDedupeKey(makeActionQueueEntry(params)));
    });

    it('should return different keys for MPU objects differing only by ' +
    'their part count', () => {
        const entry1 = makeActionQueueEntry(
            Object.assign({}, objectParams,
                          { eTag: '"d41d8cd98f00b204e9800998ecf8427e-2"' }));
        const entry2 = makeActionQueueEntry(
            Object.assign({}, objectParams,
                          { eTag: '"d41d8cd98f00b204e9800998ecf8427e-3"' }));
        assert.notStrictEqual(getTaskSchedulerDedupeKey(entry1),
                              getTaskSchedulerDedupeKey(entry2));
    });
});
