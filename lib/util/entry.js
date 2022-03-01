const { versioning } = require('arsenal');
const { DbPrefixes } = versioning.VersioningConstants;

/**
 * Function that strips v1 prefixes from object keys
 * @param {string} key entry key
 * @returns {string} entry key without v1 prefix
 */
function transformKey(key) {
    if (key.startsWith(DbPrefixes.Master)) {
        return key.slice(DbPrefixes.Master.length);
    }
    if (key.startsWith(DbPrefixes.Version)) {
        return key.slice(DbPrefixes.Version.length);
    }
    return key;
}

module.exports = {
    transformKey,
};
