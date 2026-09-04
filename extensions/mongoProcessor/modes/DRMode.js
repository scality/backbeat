'use strict';

const { isDeepStrictEqual } = require('util');

const { ObjectMD } = require('arsenal').models;

const locations = require('../../../lib/util/locations');
const ProcessorMode = require('./ProcessorMode');
const getContentType = require('../utils/contentTypeHelper');

class DRMode extends ProcessorMode {
    /**
     * The stored document tells a first write from an update: a source insert
     * is redelivered on replay and overlaps the bootstrap dump.
     *
     * @param {ObjectQueueEntry} entry - object queue entry object
     * @param {BucketInfo} bucketInfo - bucket info object
     * @return {boolean} true if the stored document is needed
     */
    needsExistingMetadata(entry, bucketInfo) { // eslint-disable-line no-unused-vars
        return true;
    }

    /**
     * The ingestion diff only covers tags; a replicated object can also change
     * its object-lock state and, until localized, its placement. Content is
     * compared too: a version is immutable, so the same version id over
     * different content is an object overwritten in place, in a bucket that is
     * not versioned or whose versioning is suspended.
     *
     * @param {ObjectQueueEntry} entry - object queue entry object
     * @param {Object|undefined} zenkoObjMd - metadata fetched from mongo
     * @return {Array} array of ReplicationInfo Content Type
     */
    getChangedContent(entry, zenkoObjMd) {
        const content = getContentType(entry, zenkoObjMd);
        if (!zenkoObjMd || content.length !== 0) {
            return content;
        }

        if (this.replacesExistingMetadata(entry, zenkoObjMd)) {
            return this._replacesStoredDocument(entry, zenkoObjMd) ? ['METADATA'] : [];
        }

        return this._hasMutableChange(entry, zenkoObjMd) ? ['METADATA'] : [];
    }

    /**
     * Whether writing the entry would leave a different document behind. The
     * fields compared are the ones the write carries, so a replayed entry is
     * told from one that changes anything at all, down to a header the older
     * diff never looked at.
     *
     * replicationInfo is left out: the processor resolves it against this
     * site's own bucket configuration once the entry has been applied, so it is
     * not a difference the entry answers for.
     *
     * @param {ObjectQueueEntry} entry - object queue entry object
     * @param {Object} zenkoObjMd - metadata fetched from mongo
     * @return {boolean} true if the stored document would change
     */
    _replacesStoredDocument(entry, zenkoObjMd) {
        const written = { ...entry.getValue(), acl: new ObjectMD().getAcl() };
        const stored = { ...zenkoObjMd };

        delete written.replicationInfo;
        delete stored.replicationInfo;

        return !isDeepStrictEqual(written, stored);
    }

    /**
     * @param {ObjectQueueEntry} entry - object queue entry object
     * @param {Object} zenkoObjMd - metadata fetched from mongo
     * @return {boolean} true if the entry changes mutable metadata
     */
    _hasMutableChange(entry, zenkoObjMd) {
        return entry.getContentMd5() !== zenkoObjMd['content-md5'] ||
            entry.getRetentionMode() !== zenkoObjMd.retentionMode ||
            entry.getRetentionDate() !== zenkoObjMd.retentionDate ||
            entry.getLegalHold() !== !!zenkoObjMd.legalHold ||
            (this._isNotLocalized(zenkoObjMd) &&
                entry.getDataStoreName() !== zenkoObjMd.dataStoreName);
    }

    /**
     * A version whose data still lives on the remote site.
     *
     * @param {Object} zenkoObjMd - metadata fetched from mongo
     * @return {boolean} true if the stored version is not localized
     */
    _isNotLocalized(zenkoObjMd) {
        return locations.isCRRLocation(zenkoObjMd.dataStoreName);
    }

    /**
     * An object with no version of its own is rewritten in place, so the entry
     * describes a document that replaced the stored one wholesale: every field
     * of it comes from the source, as it did when the Kafka Connect sink
     * replaced the document.
     *
     * A versioning suspended bucket carries a version id on the master it
     * writes in place, and marks it null.
     *
     * Nothing of the stored document is kept. Placement is the one field this
     * site could own, and it cannot here: a cold object holds the placement the
     * source pipeline derives from its storage class, and clean room, which
     * localizes placement, replicates versioned buckets only. This is where a
     * guard would go if that changed.
     *
     * @param {ObjectQueueEntry} entry - object queue entry object
     * @param {Object} zenkoObjMd - metadata fetched from mongo
     * @return {boolean} true if the entry replaces the stored object
     */
    replacesExistingMetadata(entry, zenkoObjMd) { // eslint-disable-line no-unused-vars
        return !entry.getVersionId() || entry.getIsNull();
    }

    /**
     * The source-side pipeline shaped everything this object keeps. ACLs are
     * not replicated, so they are reset as they are for an ingested object.
     *
     * @param {ObjectQueueEntry} entry - object queue entry object
     * @param {string} location - zenko storage location name
     * @param {BucketInfo} bucketInfo - bucket info object
     * @return {undefined}
     */
    applyNewObjectMetadata(entry, location, bucketInfo) { // eslint-disable-line no-unused-vars
        entry.setAcl(new ObjectMD().getAcl());
    }

    /**
     * An update brings tags and object-lock state, cleared values included. A
     * localized version keeps the placement the copy engine gave it, one still
     * on the remote site takes the entry's.
     *
     * @param {ObjectQueueEntry} entry - object queue entry object
     * @param {Object} zenkoObjMd - metadata fetched from mongo
     * @return {undefined}
     */
    mergeExistingMetadata(entry, zenkoObjMd) {
        const tags = entry.getTags();
        const retentionMode = entry.getRetentionMode();
        const retentionDate = entry.getRetentionDate();
        const legalHold = entry.getLegalHold();
        const notLocalized = this._isNotLocalized(zenkoObjMd);
        const dataStoreName = entry.getDataStoreName();
        const location = entry.getLocation();

        entry._data = { ...zenkoObjMd }; // eslint-disable-line no-param-reassign

        entry.setTags(tags);
        entry.setRetentionMode(retentionMode);
        entry.setRetentionDate(retentionDate);
        entry.setLegalHold(legalHold);

        if (notLocalized) {
            entry.setDataStoreName(dataStoreName);
            entry.setLocation(location);
        }
    }

    /**
     * Version ids are identical on both sides, so the entry's is authoritative:
     * a scal version id names a version of another system entirely.
     *
     * @param {string|undefined} scalVersionId - encoded scal version id
     * @param {string|undefined} versionId - version id the entry carries
     * @return {string|undefined} version id to act on
     */
    resolveVersionId(scalVersionId, versionId) {
        return versionId;
    }

    /**
     * A replicated object's location legitimately differs, so the ingestion
     * guard would ignore every deletion.
     *
     * @param {Object} zenkoObjMd - metadata fetched from mongo
     * @param {string} location - zenko storage location name
     * @param {string} key - object key
     * @param {string|undefined} versionId - version id the entry carries
     * @return {boolean} true if the object should be deleted
     */
    shouldProcessDelete(zenkoObjMd, location, key, versionId) { // eslint-disable-line no-unused-vars
        return true;
    }
}

module.exports = DRMode;
