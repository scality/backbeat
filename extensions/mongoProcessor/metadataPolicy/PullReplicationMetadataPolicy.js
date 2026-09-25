'use strict';

const { isDeepStrictEqual } = require('util');

const { ObjectMD } = require('arsenal').models;

const MetadataPolicy = require('./MetadataPolicy');
const DeleteOpQueueEntry = require('../../../lib/models/DeleteOpQueueEntry');
const { extractVersionId } = require('../../../lib/util/versioning');
const getContentType = require('../utils/contentTypeHelper');
const locationsConfig = require('../../../conf/locationConfig.json') || {};

class PullReplicationMetadataPolicy extends MetadataPolicy {
    skipsStoredMetadata(entry, bucketInfo) { // eslint-disable-line no-unused-vars
        // never: only the stored document tells a first write from an update,
        // and a source insert is redelivered on replay and overlaps the
        // bootstrap dump
        return false;
    }

    targetVersionId(entry) {
        // version ids are identical on both sides, so the entry's is
        // authoritative; a scal version id names a version of another system
        // entirely, the one an out-of-band production object was ingested from.
        // The source pipeline keys an entry by the document it comes from: a
        // version document is acted on as a version, a null one included -- the
        // master would not hold it once a newer version lands. A master is the
        // object itself when it has no version of its own, and a copy of its
        // latest version otherwise.
        const documentVersionId = extractVersionId(entry.getObjectVersionedKey());
        if (documentVersionId) {
            return documentVersionId;
        }
        if (entry instanceof DeleteOpQueueEntry || entry.getIsNull()) {
            return undefined;
        }
        return entry.getVersionId();
    }

    apply(entry, objMD) {
        if (objMD && this._describesStoredObject(entry, objMD)) {
            // the shared diff only covers tags, while a replicated object can
            // also change its object-lock state and, until localized, its
            // placement
            const content = getContentType(entry, objMD);
            if (content.length === 0 && !this._hasMutableChange(entry, objMD)) {
                return null;
            }

            // an update brings tags and object-lock state, cleared values
            // included
            this._mergeStoredMetadata(entry, objMD);
            return {
                content: content.length !== 0 ? content : ['METADATA'],
                versionId: this.targetVersionId(entry),
            };
        }

        // a first write, or an object overwritten in place: the source-side
        // pipeline shaped everything else this object keeps; ACLs are not
        // replicated, so they reset as for an ingested object
        entry.setAcl(new ObjectMD().getAcl());
        return {
            content: getContentType(entry),
            versionId: this.targetVersionId(entry),
        };
    }

    /**
     * Whether the entry describes the object already stored, rather than one
     * that replaced it.
     *
     * A version is immutable, so an entry for one always does, a null version
     * included. An object with no version of its own is rewritten in place,
     * and so is the master a versioning suspended bucket marks null. Only cold
     * objects replicate, and an overwrite is not one until its data is
     * archived anew, so it comes with an archive of its own. The archive alone
     * does not tell: the archival of an overwritten object can still complete,
     * and stamp its archive onto the object that replaced it. An overwrite
     * also moves the modification date, which nothing else that replicates
     * does.
     *
     * Nothing of a replaced document is kept. Placement is the one field this
     * site could own, and cannot here: a cold object holds what the source
     * pipeline derives from its storage class, and clean room -- which
     * localizes placement -- replicates versioned buckets only. A guard would
     * go here if that changed.
     *
     * @param {ObjectQueueEntry} entry - object queue entry object
     * @param {Object} objMD - metadata fetched from mongo
     * @return {boolean} true if the entry describes the stored object
     */
    _describesStoredObject(entry, objMD) {
        if (this.targetVersionId(entry)) {
            return true;
        }

        return isDeepStrictEqual(entry.getArchive()?.archiveInfo,
            objMD.archive?.archiveInfo) &&
            entry.getLastModified() === objMD['last-modified'];
    }

    /**
     * @param {ObjectQueueEntry} entry - object queue entry object
     * @param {Object} objMD - metadata fetched from mongo
     * @return {boolean} true if the entry changes mutable metadata
     */
    _hasMutableChange(entry, objMD) {
        return entry.getRetentionMode() !== objMD.retentionMode ||
            entry.getRetentionDate() !== objMD.retentionDate ||
            entry.getLegalHold() !== !!objMD.legalHold ||
            (this._isNotLocalized(objMD) &&
                entry.getDataStoreName() !== objMD.dataStoreName);
    }

    /**
     * A version whose data still lives on the remote site.
     *
     * @param {Object} objMD - metadata fetched from mongo
     * @return {boolean} true if the stored version is not localized
     */
    _isNotLocalized(objMD) {
        return Boolean(locationsConfig[objMD.dataStoreName]?.isCRR);
    }

    /**
     * Keep the stored document and apply what an update brings.
     *
     * @param {ObjectQueueEntry} entry - object queue entry object
     * @param {Object} objMD - metadata fetched from mongo
     * @return {undefined}
     */
    _mergeStoredMetadata(entry, objMD) {
        const tags = entry.getTags();
        const retentionMode = entry.getRetentionMode();
        const retentionDate = entry.getRetentionDate();
        const legalHold = entry.getLegalHold();
        const notLocalized = this._isNotLocalized(objMD);
        const dataStoreName = entry.getDataStoreName();
        const location = entry.getLocation();

        entry._data = { ...objMD }; // eslint-disable-line no-param-reassign

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

    skipsDelete(entry, objMD, location) { // eslint-disable-line no-unused-vars
        // never: a replicated object's location legitimately differs -- cold,
        // on the source, or localized -- so the ingestion guard would ignore
        // every deletion
        return false;
    }
}

module.exports = PullReplicationMetadataPolicy;
