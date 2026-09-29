'use strict';

const { ObjectMD } = require('@scality/arsenal').models;

const MetadataPolicy = require('./MetadataPolicy');
const DeleteOpQueueEntry = require('../../../lib/models/DeleteOpQueueEntry');
const { extractVersionId } = require('../../../lib/util/versioning');
const getContentType = require('../utils/contentTypeHelper');
const locationsConfig = require('../../../conf/locationConfig.json') || {};

class PullReplicationMetadataPolicy extends MetadataPolicy {
    skipsMetadataFetch(entry, bucketInfo) { // eslint-disable-line no-unused-vars
        // never: only the stored document tells a first write from an update,
        // and a source insert is redelivered on replay and overlaps the
        // bootstrap dump
        return false;
    }

    targetVersionId(entry) {
        // the document the entry comes from: a version by its key, a master
        // as the master document itself
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
        const versionId = this.targetVersionId(entry);
        const stored = this._storedObject(versionId, objMD);

        if (stored && this._isMetadataUpdate(entry, stored)) {
            const content = getContentType(entry, stored);
            this._mergeStoredMetadata(entry, stored);
            return { content: content.length !== 0 ? content : ['METADATA'], versionId };
        }

        // a first write, or an object overwritten in place: the source-side
        // pipeline shaped everything else this object keeps; ACLs are not
        // replicated, so they reset as for an ingested object
        entry.setAcl(new ObjectMD().getAcl());
        return { content: getContentType(entry), versionId };
    }

    /**
     * The stored document, when it is the object the entry targets. The
     * master document is only that object when it has no version of its own:
     * a master that is a copy of a version, or the latest version mongo
     * returns in place of a missing master, is not.
     *
     * @param {string|undefined} versionId - the version the entry targets
     * @param {Object|undefined} objMD - metadata fetched from mongo
     * @return {Object|undefined} the stored object, if any
     */
    _storedObject(versionId, objMD) {
        if (versionId === undefined && objMD?.versionId && !objMD.isNull) {
            return undefined;
        }
        return objMD;
    }

    /**
     * Whether the entry updates the metadata of the object already stored,
     * rather than describing one that replaced it.
     *
     * A version is immutable, so an entry for one always updates it, a null
     * version included. An object with no version of its own is rewritten in
     * place, and so is the master a versioning suspended bucket marks null:
     * an overwrite moves the modification date, which a metadata update
     * keeps.
     *
     * Nothing of a replaced document is kept. Placement is the one field this
     * site could own, and cannot here: a cold object holds what the source
     * pipeline derives from its storage class, and clean room -which pulls
     * the data to this site- replicates versioned buckets only.
     *
     * @param {ObjectQueueEntry} entry - object queue entry object
     * @param {Object} objMD - metadata fetched from mongo
     * @return {boolean} true if the entry updates the stored object
     */
    _isMetadataUpdate(entry, objMD) {
        if (this.targetVersionId(entry)) {
            return true;
        }

        return entry.getLastModified() === objMD['last-modified'];
    }

    /**
     * Keep the stored document and apply what an update brings.
     *
     * @param {ObjectQueueEntry} entry - object queue entry object
     * @param {Object} objMD - metadata fetched from mongo
     * @return {undefined}
     */
    _mergeStoredMetadata(entry, objMD) {
        // as the entry carries them, so that a value it leaves out is left
        // out of the document too
        const { tags, retentionMode, retentionDate, legalHold } = entry.getValue();
        const dataStoreName = entry.getDataStoreName();
        const location = entry.getLocation();

        // eslint-disable-next-line no-param-reassign
        entry._data = { ...objMD, tags, retentionMode, retentionDate, legalHold };

        // update data location if data has not been pulled yet
        if (locationsConfig[objMD.dataStoreName]?.isCRR) {
            entry.setDataStoreName(dataStoreName);
            entry.setLocation(location);
        }
    }

    skipsDelete(entry, objMD, location) { // eslint-disable-line no-unused-vars
        // must process every deletion, whatever the state of the object
        return false;
    }
}

module.exports = PullReplicationMetadataPolicy;
