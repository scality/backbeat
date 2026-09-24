'use strict';

const assert = require('assert');

/**
 * A metadata policy holds the decisions that differ between the streams the
 * mongo-processor writes: out-of-band ingestion, where identity and placement
 * are rewritten to local values because the source system's accounts and
 * locations do not exist here, and pull replication, where they are replicated
 * and so are applied as they arrive.
 *
 * Everything else about processing an entry is shared.
 */
class MetadataPolicy {
    /**
     * Whether the metadata already stored for the object can be left unread.
     *
     * This method must be implemented by subclasses of MetadataPolicy
     * @param {ObjectQueueEntry} entry - object queue entry object
     * @param {BucketInfo} bucketInfo - bucket info object
     * @return {boolean} true if the stored metadata is not needed
     */
    skipsStoredMetadata(entry, bucketInfo) { // eslint-disable-line no-unused-vars
        assert(false,
            'sub-classes of MetadataPolicy must implement ' +
            'the skipsStoredMetadata() method');
    }

    /**
     * Which version of the object on this site an entry acts on: the one a
     * put reads, and the one a delete reads and removes.
     *
     * This method must be implemented by subclasses of MetadataPolicy
     * @param {ObjectQueueEntry|DeleteOpQueueEntry} entry - queue entry object
     * @return {string|undefined} version id, or undefined for the master
     */
    targetVersionId(entry) { // eslint-disable-line no-unused-vars
        assert(false,
            'sub-classes of MetadataPolicy must implement ' +
            'the targetVersionId() method');
    }

    /**
     * Apply the entry onto the metadata already stored, leaving the entry
     * holding the document to write, and report what the write changes as
     * replicationInfo content values, and which version it is written under.
     *
     * An entry that changes nothing is not written at all, so it is left
     * untouched.
     *
     * This method must be implemented by subclasses of MetadataPolicy
     * @param {ObjectQueueEntry} entry - object queue entry object
     * @param {Object|undefined} objMD - metadata fetched from mongo
     * @param {string} location - zenko storage location name
     * @param {BucketInfo} bucketInfo - bucket info object
     * @return {Object|null} null if the entry changes nothing, otherwise
     *   `content`, the array of ReplicationInfo Content Type, and `versionId`,
     *   the version the document is written under, the master following it,
     *   or undefined to write the master alone
     */
    apply(entry, objMD, location, bucketInfo) { // eslint-disable-line no-unused-vars
        assert(false,
            'sub-classes of MetadataPolicy must implement ' +
            'the apply() method');
    }

    /**
     * Whether a delete entry no longer applies to the object as stored.
     *
     * This method must be implemented by subclasses of MetadataPolicy
     * @param {DeleteOpQueueEntry} entry - delete object entry
     * @param {Object} objMD - metadata fetched from mongo
     * @param {string} location - zenko storage location name
     * @return {boolean} true if the object should be left alone
     */
    skipsDelete(entry, objMD, location) { // eslint-disable-line no-unused-vars
        assert(false,
            'sub-classes of MetadataPolicy must implement ' +
            'the skipsDelete() method');
    }
}

module.exports = MetadataPolicy;
