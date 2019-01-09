const sourceEntry = {
    getLogInfo: () => {},
    getLocation: () => ([]),
    getUserMetadata: () => {},
    getContentType: () => {},
    getCacheControl: () => {},
    getContentDisposition: () => {},
    getContentEncoding: () => {},
    getReplicationIsNFS: () => {},
};

const destEntry = {
    getLogInfo: () => {},
    getBucket: () => {},
    getObjectKey: () => {},
    getReplicationStorageType: () => {},
    getEncodedVersionId: () => {},
};

module.exports = { sourceEntry, destEntry };
