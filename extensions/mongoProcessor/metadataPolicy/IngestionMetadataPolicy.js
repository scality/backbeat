'use strict';

const { emptyFileMd5 } = require('arsenal').constants;
const { ObjectMD } = require('arsenal').models;
const { VersionID } = require('arsenal').versioning;

const MetadataPolicy = require('./MetadataPolicy');
const getContentType = require('../utils/contentTypeHelper');
const DeleteOpQueueEntry = require('../../../lib/models/DeleteOpQueueEntry');
const { extractVersionId } = require('../../../lib/util/versioning');

const scalVersionIdHeader = 'x-amz-meta-scal-version-id';

class IngestionMetadataPolicy extends MetadataPolicy {
    skipsStoredMetadata(entry, bucketInfo) {
        // the stored document is only read to update replication info and to
        // validate the `x-amz-meta-scal-version-id` header of a restored
        // object, so with neither in play the fetch is skipped
        const bucketRepInfo = bucketInfo.getReplicationConfiguration();

        return !this._scalVersionId(entry) &&
            !bucketRepInfo?.rules?.some(r => r.enabled);
    }

    targetVersionId(entry) {
        // x-amz-meta-scal-version-id wins where it is set, which happens only
        // for a restored object: otherwise the source and the ingested object
        // carry the same version id and the header is absent
        const scalVersionId = this._scalVersionId(entry);

        if (entry instanceof DeleteOpQueueEntry) {
            return scalVersionId ?
                VersionID.decode(scalVersionId) :
                extractVersionId(entry.getObjectVersionedKey());
        }

        // master keys with a 'null' version id comming from
        // a versioning suspended bucket are considered a version
        // we should not specify the version id in this case
        if (entry.getIsNull()) {
            return undefined;
        }
        return scalVersionId ?
            VersionID.decode(scalVersionId) : entry.getVersionId();
    }

    /**
     * The encoded `x-amz-meta-scal-version-id` of an entry, which an object
     * carries in its metadata and a delete in its overhead fields.
     *
     * @param {ObjectQueueEntry|DeleteOpQueueEntry} entry - queue entry object
     * @return {string|undefined} encoded scal version id, if any
     */
    _scalVersionId(entry) {
        if (entry instanceof DeleteOpQueueEntry) {
            return entry.getOverheadField(scalVersionIdHeader);
        }
        return entry.getValue()[scalVersionIdHeader];
    }

    /**
     * The version a composed entry is written under: its own -- the stored
     * version's once merged into it -- or none for a master.
     *
     * @param {ObjectQueueEntry} entry - object queue entry object
     * @return {string|undefined} version id, or undefined for the master
     */
    _writtenVersionId(entry) {
        // Versioning suspended entries will have a version id but also a isNull tag.
        // These master keys are considered a version and do not have a duplicate version,
        // we don't specify the version id and repairMaster in this case
        if (entry.getVersionId() && !entry.getIsNull()) {
            return entry.getVersionId();
        }
        return undefined;
    }

    apply(entry, objMD, location, bucketInfo) {
        const content = getContentType(entry, objMD);
        if (content.length === 0) {
            return null;
        }

        if (objMD) {
            // Keep existing metadata fields, only need to update the tags
            const tags = entry.getTags();
            entry._data = { ...objMD }; // eslint-disable-line no-param-reassign
            entry.setTags(tags);
        } else {
            // Update necessary metadata fields before saving to Zenko MongoDB
            this._updateOwnerMD(entry, bucketInfo);
            this._updateObjectDataStoreName(entry, location);
            this._updateLocations(entry, location);
            this._updateAcl(entry);
        }

        return { content, versionId: this._writtenVersionId(entry) };
    }

    /**
     * Update ingested entry metadata fields: owner-id, owner-display-name
     * @param {ObjectQueueEntry} entry - object queue entry object
     * @param {BucketInfo} bucketInfo - bucket info object
     * @return {undefined}
     */
    _updateOwnerMD(entry, bucketInfo) {
        // zenko bucket owner information is being set on ingested md
        entry.setOwnerDisplayName(bucketInfo.getOwnerDisplayName());
        entry.setOwnerId(bucketInfo.getOwner());
    }

    /**
     * Update ingested entry metadata fields: dataStoreName
     * @param {ObjectQueueEntry} entry - object queue entry object
     * @param {string} location - owner details
     * @return {undefined}
     */
    _updateObjectDataStoreName(entry, location) {
        entry.setDataStoreName(location);
    }

    /**
     * Update ingested entry metadata location field. Each location change
     * includes: key, dataStoreName, dataStoreType, dataStoreVersionId
     * @param {ObjectQueueEntry} entry - object queue entry object
     * @param {string} zenkoLocation - zenko storage location name
     * @return {undefined}
     */
    _updateLocations(entry, zenkoLocation) {
        const locations = entry.getLocation();
        // if version id is undefined, we have a single null object.
        // To hold reference to this null object, we need to encode "null"
        // as its dataStoreVersionId
        const dataStoreVersionId = entry.getVersionId() ?
            entry.getEncodedVersionId() : 'null';
        let zenkoDataLocations;
        if (!locations || locations.length === 0) {
            zenkoDataLocations = [{
                key: entry.getObjectKey(),
                size: 0,
                start: 0,
                dataStoreName: zenkoLocation,
                dataStoreType: 'aws_s3',
                dataStoreETag: `1:${emptyFileMd5}`,
                dataStoreVersionId,
            }];
        } else {
            zenkoDataLocations = [{
                key: entry.getObjectKey(),
                size: entry.getContentLength(),
                start: 0,
                dataStoreName: zenkoLocation,
                dataStoreType: 'aws_s3',
                dataStoreETag: `1:${entry.getContentMd5()}`,
                dataStoreVersionId,
            }];
        }
        entry.setLocation(zenkoDataLocations);
    }

    /**
     * Update acl info on ingested object MD
     * @param {ObjectQueueEntry} entry - object queue entry object
     * @return {undefined}
     */
    _updateAcl(entry) {
        // reset acl info
        const objectMDModel = new ObjectMD();
        entry.setAcl(objectMDModel.getAcl());
    }

    skipsDelete(entry, objMD, location) {
        // an object stored elsewhere is left alone: the delete was caused by
        // restored-object expiration or transition, both of which update
        // dataStoreName before sending the object to GC
        return !this._isStoredHere(objMD, location, entry.getObjectKey(),
            extractVersionId(entry.getObjectVersionedKey()));
    }

    /**
     * Whether the stored object still holds the data the delete entry names.
     *
     * @param {Object} objMD - metadata fetched from mongo
     * @param {string} location - zenko storage location name
     * @param {string} key - object key
     * @param {string|null} versionId - the entry's own decoded version id,
     *   which the data location names
     * @return {boolean} true if the object is stored where the entry says
     */
    _isStoredHere(objMD, location, key, versionId) {
        const encode = vid => (vid ? VersionID.encode(vid) : 'null');

        return objMD.dataStoreName === location &&
            objMD.location?.length === 1 &&
            objMD.location[0].dataStoreName === location &&
            objMD.location[0].key === key &&
            (objMD.location[0].dataStoreVersionId || 'null') ===
                encode(versionId);
    }
}

module.exports = IngestionMetadataPolicy;
