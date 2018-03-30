class RaftLogEntry {
    createPutEntry(bucket, objectKey, objectVal) {
        return JSON.stringify({ type: 'put', bucket, key: objectKey, value: JSON.stringify(objectVal) });
    }

    createPutBucketEntry(bucket) {
        return JSON.stringify({
            type: 'put',
            Bucket: 'metastore',
            key: bucket._name,
            Value: bucket.serialize(),
        });
    }
}

module.exports = RaftLogEntry;
