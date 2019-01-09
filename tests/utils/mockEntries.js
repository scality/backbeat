const sourceEntry = {
    getLogInfo: () => {},
    getLocation: () => ([]),
    getUserMetadata: () => {},
    getContentType: () => {},
    getCacheControl: () => {},
    getContentDisposition: () => {},
    getContentEncoding: () => {},
};

const destEntry = {
    getLogInfo: () => {},
    getBucket: () => {},
    getObjectKey: () => {},
    getReplicationStorageType: () => {},
    getEncodedVersionId: () => {},
};

module.exports = { sourceEntry, destEntry };
