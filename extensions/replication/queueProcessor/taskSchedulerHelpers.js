const ObjectQueueEntry = require('../../../lib/models/ObjectQueueEntry');
const ActionQueueEntry = require('../../../lib/models/ActionQueueEntry');

function getTaskSchedulerQueueKey(entry) {
    if (entry instanceof ObjectQueueEntry) {
        return entry.getCanonicalKey();
    }
    if (entry instanceof ActionQueueEntry) {
        const { bucket, key } = entry.getAttribute('target');
        return `${bucket}/${key}`;
    }
    return undefined;
}

function getTaskSchedulerDedupeKey(entry) {
    if (entry instanceof ObjectQueueEntry) {
        const key = entry.getCanonicalKey();
        const version = entry.getVersionId();
        const contentMd5 = entry.getContentMd5();
        return `${key}:${version || ''}:${contentMd5}`;
    }
    if (entry instanceof ActionQueueEntry) {
        const { key, version, contentMd5 } =
              entry.getAttribute('target');
        return `${key}:${version || ''}:${contentMd5}`;
    }
    return undefined;
}

module.exports = {
    getTaskSchedulerQueueKey,
    getTaskSchedulerDedupeKey,
};
