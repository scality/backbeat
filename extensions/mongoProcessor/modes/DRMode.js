'use strict';

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
     * its object-lock state and, until localized, its placement.
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

        return this._hasMutableChange(entry, zenkoObjMd) ? ['METADATA'] : [];
    }

    /**
     * @param {ObjectQueueEntry} entry - object queue entry object
     * @param {Object} zenkoObjMd - metadata fetched from mongo
     * @return {boolean} true if the entry changes mutable metadata
     */
    _hasMutableChange(entry, zenkoObjMd) {
        return entry.getRetentionMode() !== zenkoObjMd.retentionMode ||
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
