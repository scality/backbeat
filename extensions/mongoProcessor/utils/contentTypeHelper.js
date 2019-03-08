'use strict'; // eslint-disable-line

const { isMasterKey } = require('arsenal/lib/versioning/Version');

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
    if ((kafkaTagLength !== 0 && zenkoTagLength === 0) ||
         kafkaTagLength !== zenkoTagLength) {
        return ['METADATA', 'PUT_TAGGING'];
    }
    if (kafkaTagLength === 0 && zenkoTagLength !== 0) {
        return ['METADATA', 'DELETE_TAGGING'];
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
    const content = [];
    // if object already exists in mongo, only check for md-only updates
    if (zenkoObjMd) {
        return _getObjectTagContent(entry, zenkoObjMd);
    }
    /*
        TODO:
        I think this logic is important to get right, and I'm not 100% sure yet.
        Reasoning for current implementation:
        - Above, we check if the object md (for given version) exists in Mongo.
          If it does exist, we can say this is now a situation where the current
          kafka entry is a duplicate entry or an entry where md-only change was
          made. Only metadata change I check is tagging.
          Note that the check is specifically for the Mongo entry where the key
          name is the `${key}${versionId}`. We do not check the master version.

        - If no md-only is identified, below, we check if this current entry
          is a master key.
          - If not a master key, then this is a duplicate entry.
          - If master key, we want to update regardless. This is a situation
            where the kafka entry for a master key could seem like a duplicate.
            But, it might just be the master key trying to update itself.
            TODO: One check to make is the `zenkoObjMd` replicationInfo fields.
                  If this field is set and shows 'COMPLETED' or 'FAILED', we
                  know this entry has already attempted replication.
                  So (I think) we don't need to update master to 'PENDING' in
                  this case.

        - At bottom, it either means we are dealing with master key or a
          version key that has not yet been stored in Mongo. This case means we
          should just check the usual 'DATA', 'METADATA', and 'MPU' as this
          version has yet to be added as a "new version" in Zenko
    */
    if (!isMasterKey(entry)) {
        // if the entry is not a master key entry and we identified no
        // md-only updates, we identify this as a duplicate entry to be
        // ignored. Send empty array.
        return content;
    }
    // object does not exist in mongo or is a master key entry
    content.push(..._getDataContent(entry));
    content.push(..._getMPUTagContent(entry));
    return content;
}

module.exports = getContentType;
