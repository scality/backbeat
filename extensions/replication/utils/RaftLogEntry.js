class RaftLogEntry {
    createPutEntry(bucket, objectKey, objectVal) {
        return { type: 'put', bucket, key: objectKey, value: objectVal };
    }
}

module.exports = RaftLogEntry;
