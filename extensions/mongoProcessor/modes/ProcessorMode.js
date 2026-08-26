'use strict';

const assert = require('assert');

/**
 * A processor mode holds the decisions that differ between the streams the
 * mongo-processor writes: out-of-band ingestion, where identity and placement
 * are rewritten to local values because the source system's accounts and
 * locations do not exist here, and D/R, where they are replicated and so are
 * applied as they arrive.
 *
 * Everything else about processing an entry is shared.
 */
class ProcessorMode {
    /**
     * Whether the entry's existing metadata has to be read before processing.
     *
     * This method must be implemented by subclasses of ProcessorMode
     * @param {ObjectQueueEntry} entry - object queue entry object
     * @param {BucketInfo} bucketInfo - bucket info object
     * @return {boolean} true if the stored document is needed
     */
    needsExistingMetadata(entry, bucketInfo) { // eslint-disable-line no-unused-vars
        assert(false,
            'sub-classes of ProcessorMode must implement ' +
            'the needsExistingMetadata() method');
    }

    /**
     * What changed between the entry and the object already stored, as
     * replicationInfo content values. An empty list means the entry carries no
     * change and is not written.
     *
     * This method must be implemented by subclasses of ProcessorMode
     * @param {ObjectQueueEntry} entry - object queue entry object
     * @param {Object|undefined} zenkoObjMd - metadata fetched from mongo
     * @return {Array} array of ReplicationInfo Content Type
     */
    getChangedContent(entry, zenkoObjMd) { // eslint-disable-line no-unused-vars
        assert(false,
            'sub-classes of ProcessorMode must implement ' +
            'the getChangedContent() method');
    }

    /**
     * Apply the metadata fields an object gets when it is first written here.
     *
     * This method must be implemented by subclasses of ProcessorMode
     * @param {ObjectQueueEntry} entry - object queue entry object
     * @param {string} location - zenko storage location name
     * @param {BucketInfo} bucketInfo - bucket info object
     * @return {undefined}
     */
    applyNewObjectMetadata(entry, location, bucketInfo) { // eslint-disable-line no-unused-vars
        assert(false,
            'sub-classes of ProcessorMode must implement ' +
            'the applyNewObjectMetadata() method');
    }

    /**
     * Merge the entry into the object already stored, deciding which fields the
     * entry brings and which the stored document keeps.
     *
     * This method must be implemented by subclasses of ProcessorMode
     * @param {ObjectQueueEntry} entry - object queue entry object
     * @param {Object} zenkoObjMd - metadata fetched from mongo
     * @return {undefined}
     */
    mergeExistingMetadata(entry, zenkoObjMd) { // eslint-disable-line no-unused-vars
        assert(false,
            'sub-classes of ProcessorMode must implement ' +
            'the mergeExistingMetadata() method');
    }

    /**
     * Which version of the object the entry acts on, given the version id it
     * carries and the `x-amz-meta-scal-version-id` it may carry alongside.
     *
     * This method must be implemented by subclasses of ProcessorMode
     * @param {string|undefined} scalVersionId - encoded scal version id
     * @param {string|undefined} versionId - version id the entry carries
     * @return {string|undefined} version id to act on
     */
    resolveVersionId(scalVersionId, versionId) { // eslint-disable-line no-unused-vars
        assert(false,
            'sub-classes of ProcessorMode must implement ' +
            'the resolveVersionId() method');
    }

    /**
     * Whether a delete entry still applies to the object as stored.
     *
     * This method must be implemented by subclasses of ProcessorMode
     * @param {Object} zenkoObjMd - metadata fetched from mongo
     * @param {string} location - zenko storage location name
     * @param {string} key - object key
     * @param {string|undefined} versionId - decoded version id of the entry
     * @return {boolean} true if the object should be deleted
     */
    shouldProcessDelete(zenkoObjMd, location, key, versionId) { // eslint-disable-line no-unused-vars
        assert(false,
            'sub-classes of ProcessorMode must implement ' +
            'the shouldProcessDelete() method');
    }
}

module.exports = ProcessorMode;
