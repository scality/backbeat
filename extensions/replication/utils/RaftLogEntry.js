const BucketInfo = require('arsenal').models.BucketInfo;

class RaftLogEntry {
    createPutEntry(bucket, objectKey, objectVal) {
        return { type: 'put', bucket, key: objectKey, value: objectVal };
    }

    createPutBucketEntry(bucket) {
        return {
            type: 'put',
            Bucket: '',
            key: '',
            Value: bucket.serialize(),
        };
    }
}

module.exports = RaftLogEntry;
