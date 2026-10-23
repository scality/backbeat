const { versioning } = require('@scality/arsenal');

const VID_SEP = versioning.VersioningConstants.VersionId.Separator;
const { isMasterKey } = versioning;

/**
 * @param {string} key full object key (_id in metadata)
 * @returns {string} object key, without the version id
 */
function extractBaseKey(key) {
    return key.split(VID_SEP)[0];
}

/**
 * @param {string} key full object key (_id in metadata)
 * @returns {string | null} version id
 */
function extractVersionId(key) {
    if (key.includes(VID_SEP)) {
        return key.split(VID_SEP)[1];
    }
    return null;
}

/**
 * Whether the object is stored under a version key of its own.
 *
 * A null version (put while versioning is suspended or before it was enabled)
 * lives in the master key only, even when it carries an internal version id.
 * Checks the raw fields: ObjectMD.getVersionId() returns 'null' for a null
 * version, which would defeat the test.
 *
 * @param {Object} value - object metadata, decoded
 * @return {boolean} true if a version key holds this object
 */
function hasVersionKey(value) {
    return !!value.versionId && !value.isNull;
}

/**
 * Whether a log entry is the master key of an object whose version key is
 * also logged: the master is then a copy of the version entry, which is the
 * one to process.
 *
 * @param {string} key - object key as logged, without the v1 prefix
 * @param {Object} value - object metadata, decoded
 * @return {boolean} true if the entry duplicates a version key entry
 */
function isShadowedMaster(key, value) {
    return isMasterKey(key) && hasVersionKey(value);
}

module.exports = {
    extractBaseKey,
    extractVersionId,
    hasVersionKey,
    isShadowedMaster,
};
