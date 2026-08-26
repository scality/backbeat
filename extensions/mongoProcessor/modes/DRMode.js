'use strict';

const { ObjectMD } = require('arsenal').models;

const locations = require('../../../lib/util/locations');
const ProcessorMode = require('./ProcessorMode');
const getContentType = require('../utils/contentTypeHelper');

class DRMode extends ProcessorMode {
    needsExistingMetadata(entry, bucketInfo) { // eslint-disable-line no-unused-vars
        // always: only the stored document tells a first write from an update,
        // and a source insert is redelivered on replay and overlaps the
        // bootstrap dump
        return true;
    }

    getChangedContent(entry, zenkoObjMd) {
        // the shared diff only covers tags, while a replicated object can also
        // change its object-lock state and, until localized, its placement
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

    applyNewObjectMetadata(entry, location, bucketInfo) { // eslint-disable-line no-unused-vars
        // the source-side pipeline shaped everything else this object keeps;
        // ACLs are not replicated, so they reset as for an ingested object
        entry.setAcl(new ObjectMD().getAcl());
    }

    mergeExistingMetadata(entry, zenkoObjMd) {
        // an update brings tags and object-lock state, cleared values included
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

        // a localized version keeps the placement the copy engine gave it; one
        // still on the remote site takes the entry's
        if (notLocalized) {
            entry.setDataStoreName(dataStoreName);
            entry.setLocation(location);
        }
    }

    resolveVersionId(scalVersionId, versionId) {
        // version ids are identical on both sides, so the entry's is
        // authoritative; a scal version id names a version of another system
        // entirely, the one an out-of-band production object was ingested from
        return versionId;
    }

    shouldProcessDelete(zenkoObjMd, location, key, versionId) { // eslint-disable-line no-unused-vars
        // always: a replicated object's location legitimately differs -- cold,
        // on the source, or localized -- so the ingestion guard would ignore
        // every deletion
        return true;
    }
}

module.exports = DRMode;
