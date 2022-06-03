const VID_SEP = require('arsenal').versioning.VersioningConstants
          .VersionId.Separator;

module.exports = {
    isMasterKey: (key) => !key.includes(VID_SEP),
};
