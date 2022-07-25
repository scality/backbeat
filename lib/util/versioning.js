const VID_SEP = require('arsenal').versioning.VersioningConstants
          .VersionId.Separator;

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

module.exports = {
    extractVersionId,
};
