const sourceEntry = {
    getBucket: () => {},
    getObjectKey: () => {},
    getReplicationStorageType: () => {},
    getEncodedVersionId: () => {},
    getLastModified: () => new Date().toJSON(),
    getLogInfo: () => {},
    getLocation: () => ([]),
    getUserMetadata: () => {},
    getContentType: () => {},
    getContentLength: () => 0,
    getCacheControl: () => {},
    getContentDisposition: () => {},
    getContentEncoding: () => {},
    getReplicationIsNFS: () => {},
    getTags: () => {},
    getObjectVersionedKey: () => {},
    getValue: () => {},
    getVersionId: () => {},
    toFailedEntry: () => {},
    getReplayCount: () => {},
    setReplayCount: () => {},
};

module.exports = { sourceEntry };
