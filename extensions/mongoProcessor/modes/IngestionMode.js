'use strict';

const { emptyFileMd5 } = require('arsenal').constants;
const { ObjectMD } = require('arsenal').models;
const { VersionID } = require('arsenal').versioning;

const ProcessorMode = require('./ProcessorMode');
const getContentType = require('../utils/contentTypeHelper');

class IngestionMode extends ProcessorMode {
    needsExistingMetadata(entry, bucketInfo) {
        // the stored document is only read to update replication info and to
        // validate the `x-amz-meta-scal-version-id` header of a restored
        // object, so with neither in play the fetch is skipped
        const scalVersionId = entry.getValue()['x-amz-meta-scal-version-id'];
        const bucketRepInfo = bucketInfo.getReplicationConfiguration();

        return !!scalVersionId || !!bucketRepInfo?.rules?.some(r => r.enabled);
    }

    getChangedContent(entry, zenkoObjMd) {
        return getContentType(entry, zenkoObjMd);
    }

    applyNewObjectMetadata(entry, location, bucketInfo) {
        this._updateOwnerMD(entry, bucketInfo);
        this._updateObjectDataStoreName(entry, location);
        this._updateLocations(entry, location);
        this._updateAcl(entry);
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

    replacesExistingMetadata(entry, zenkoObjMd) { // eslint-disable-line no-unused-vars
        // an ingested object is always merged into the one stored: out-of-band
        // writes are not told apart here, as they were not before modes existed
        return false;
    }

    mergeExistingMetadata(entry, zenkoObjMd) {
        // Keep existing metadata fields, only need to update the tags
        const tags = entry.getTags();
        entry._data = { ...zenkoObjMd }; // eslint-disable-line no-param-reassign
        entry.setTags(tags);
    }

    resolveVersionId(scalVersionId, versionId) {
        // x-amz-meta-scal-version-id wins where it is set, which happens only
        // for a restored object: otherwise the source and the ingested object
        // carry the same version id and the header is absent
        return scalVersionId ? VersionID.decode(scalVersionId) : versionId;
    }

    shouldProcessDelete(zenkoObjMd, location, key, versionId) {
        // an object stored elsewhere is left alone: the delete was caused by
        // restored-object expiration or transition, both of which update
        // dataStoreName before sending the object to GC
        const encode = vid => (vid ? VersionID.encode(vid) : 'null');

        return zenkoObjMd.dataStoreName === location &&
            zenkoObjMd.location?.length === 1 &&
            zenkoObjMd.location[0].dataStoreName === location &&
            zenkoObjMd.location[0].key === key &&
            (zenkoObjMd.location[0].dataStoreVersionId || 'null') ===
                encode(versionId);
    }
}

module.exports = IngestionMode;
