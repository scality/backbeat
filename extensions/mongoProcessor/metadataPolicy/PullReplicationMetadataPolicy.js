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
        if (entry instanceof DeleteOpQueueEntry) {
            return extractVersionId(entry.getObjectVersionedKey());
        }

        // the internal version id: getVersionId() names a null version
        // 'null', as the S3 API does, while mongo keys it by its own id
        return entry.getValue().versionId;
    }

    apply(entry, objMD) {
        const versionId = this.targetVersionId(entry);

        if (objMD && this._isMetadataUpdate(entry, objMD, versionId)) {
            const content = getContentType(entry, objMD);
            this._mergeStoredMetadata(entry, objMD);
            return { content: content.length !== 0 ? content : ['METADATA'], versionId };
        }

        // a first write, or an object overwritten in place: the source-side
        // pipeline shaped everything else this object keeps; ACLs are not
        // replicated, so they reset as for an ingested object
        entry.setAcl(new ObjectMD().getAcl());
        return { content: getContentType(entry), versionId };
    }

    /**
     * Whether the entry updates the metadata of the object already stored,
     * rather than describing one that replaced it.
     *
     * A version is immutable, so an entry for one always updates it. An
     * object with no version of its own is rewritten in place under the same
     * key, and so is the master a versioning suspended bucket marks null: an
     * overwrite moves the modification date, which a metadata update keeps.
     * A master that copies a real version, or the latest version mongo
     * returns in place of a missing master, carries that version's date and
     * is replaced the same way.
     *
     * Nothing of a replaced document is kept. Placement is the one field this
     * site could own, and cannot here: a cold object holds what the source
     * pipeline derives from its storage class, and clean room -which pulls
     * the data to this site- replicates versioned buckets only.
     *
     * @param {ObjectQueueEntry} entry - object queue entry object
     * @param {Object} objMD - metadata fetched from mongo
     * @param {string|undefined} versionId - the version the entry targets
     * @return {boolean} true if the entry updates the stored object
     */
    _isMetadataUpdate(entry, objMD, versionId) {
        if (versionId) {
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
