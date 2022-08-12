const { encode } = require('arsenal').versioning.VersionID;
const ObjectMD = require('arsenal').models.ObjectMD;

function getVersionIdForReplication(sourceEntry, encoding = false) {
    const nullCheckFn = sourceEntry instanceof ObjectMD ? entry => entry.getIsNull() : entry => entry.isNull;
    const getVIDFn = sourceEntry instanceof ObjectMD ? entry => entry.getVersionId() : entry => entry.versionId;
    const realVID = nullCheckFn(sourceEntry) ? 'null' : getVIDFn(sourceEntry);
    if (encoding) {
      if (realVID) {
        return encode(realVID);
      }
      return undefined;
    }
    return realVID;
}

module.exports = { getVersionIdForReplication };
