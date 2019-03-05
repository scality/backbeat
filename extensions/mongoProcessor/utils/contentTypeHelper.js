'use strict'; // eslint-disable-line

function _getDataContent(entry) {
    const contentLength = entry.getContentLength();
    if (contentLength > 0) {
        return ['DATA', 'METADATA'];
    }
    return ['METADATA'];
}

function _getMPUTagContent(entry) {
    if (entry.isMultipartUpload()) {
        return ['MPU'];
    }
    return [];
}

/**
 * compares object tags between Zenko object and kafka object entry to see
 * if entry contains a object tagging change (for replication Content field)
 * @param {ObjectQueueEntry} entry - object metadata entry from Kafka entry
 * @param {Object} zenkoObjMd - Zenko object metadata currently in Mongo
 * @return {undefined}
 */
function _getObjectTagContent(entry, zenkoObjMd) {
    const kafkaEntryTags = entry.getTags();
    const zenkoTags = zenkoObjMd.tags;
    const kafkaTagLength = Object.keys(kafkaEntryTags).length;
    const zenkoTagLength = Object.keys(zenkoTags).length;

    // if tagging does not exist on past entry vs present entry, identify as
    // duplicate entry
    if (kafkaTagLength === 0 && zenkoTagLength === 0) {
        return [];
    }
    // When comparing `kafkaEntryTags` and `zenkoTags`
    // Cases for PUT_TAGGING:
    // - If tag length matches and any key values differ
    // - If kafka entry tags exist and zenko object tags do not
    // - If tag length differ
    // Cases for DELETE_TAGGING:
    // - If zenko object tags exist and kafka entry tags do not
    if (kafkaTagLength === zenkoTagLength) {
        const hasChanged = Object.keys(kafkaEntryTags).some(tag =>
            kafkaEntryTags[tag] !== zenkoTags[tag]
        );
        if (hasChanged) {
            return ['METADATA', 'PUT_TAGGING'];
        }
    }
    // Check delete tags before checking next conditional given tag
    // length difference check.
    if (kafkaTagLength === 0 && zenkoTagLength !== 0) {
        return ['METADATA', 'DELETE_TAGGING'];
    }
    if ((kafkaTagLength !== 0 && zenkoTagLength === 0) ||
         kafkaTagLength !== zenkoTagLength) {
        return ['METADATA', 'PUT_TAGGING'];
    }
    return [];
}

/**
 * Get the replicationInfo Content field for a newly ingested object entry
 * @param {ObjectQueueEntry} entry - object queue entry object
 * @param {Object|undefined} zenkoObjMd - object metadata currently saved in
 *    mongo. Can be undefined if this entry has never been ingested before
 * @return {Array} array of ReplicationInfo Content Type
 */
function getContentType(entry, zenkoObjMd) {
    // if object already exists in mongo, only check for md-only updates.
    if (zenkoObjMd) {
        return _getObjectTagContent(entry, zenkoObjMd);
    }
    // object does not exist in mongo or is a master key entry
    const content = [];
    content.push(..._getDataContent(entry));
    content.push(..._getMPUTagContent(entry));
    return content;
}

module.exports = getContentType;
