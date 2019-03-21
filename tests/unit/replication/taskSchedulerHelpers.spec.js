const assert = require('assert');

const ObjectQueueEntry = require('../../../lib/models/ObjectQueueEntry');
const ActionQueueEntry = require('../../../lib/models/ActionQueueEntry');

const { getTaskSchedulerQueueKey, getTaskSchedulerDedupeKey } = require(
    '../../../extensions/replication/queueProcessor/taskSchedulerHelpers');

function makeObjectQueueEntry({ key, versionId, contentMd5 }) {
    return new ObjectQueueEntry('test-bucket-name', key)
        .setVersionId(versionId)
        .setContentMd5(contentMd5);
}

function makeActionQueueEntry({ key, versionId, contentMd5 }) {
    return new ActionQueueEntry({
        target: {
            key,
            version: versionId,
            contentMd5,
        }
    });
}

function makeQueueEntry(entryClass, { key, versionId, contentMd5 }) {
    if (entryClass === 'ObjectQueueEntry') {
        return makeObjectQueueEntry({ key, versionId, contentMd5 });
    }
    if (entryClass === 'ActionQueueEntry') {
        return makeActionQueueEntry({ key, versionId, contentMd5 });
    }
    return assert.fail(`bad class ${entryClass}`);
}

function makeTestEntry(entryClass,
                       { keySelector, versionIdSelector, contentMd5Selector }) {
    const keys = ['masterkey1', 'masterkey2'];
    const versionIds = ['abcdef', 'ghijkl'];
    const contentMd5s = ['d41d8cd98f00b204e9800998ecf8427e',
                         '93b07384d113edec49eaa6238ad5ff00'];
    return makeQueueEntry(
        entryClass, {
            key: keys[keySelector],
            versionId: versionIds[versionIdSelector],
            contentMd5: contentMd5s[contentMd5Selector],
        });
}

function makeTestEntryPair(entryClass, { distinctKey, distinctVersionId,
                                         distinctContentMd5 }) {
    return [
        makeTestEntry(entryClass, { keySelector: 0,
                                    versionIdSelector: 0,
                                    contentMd5Selector: 0 }),
        makeTestEntry(entryClass, {
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
    });
});
