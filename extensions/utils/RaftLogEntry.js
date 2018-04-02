const constants = require('../../constants');

class RaftLogEntry {
    createPutEntry(objectMd) {
        return JSON.stringify({
            type: 'put',
            bucket: objectMd.bucketName,
            key: objectMd.objectKey,
            value: JSON.stringify(objectMd.res),
        });
    }

    createPutBucketEntry(bucket) {
        return JSON.stringify({
            type: 'put',
            bucket: constants.usersBucket,
            key: bucket.key,
            value: JSON.stringify(bucket.value),
        });
    }

    createPutBucketMdEntry(bucket) {
        return JSON.stringify({
            type: 'put',
            bucket: bucket._name,
            key: bucket._name,
            value: bucket.serialize(),
        });
    }
}

module.exports = RaftLogEntry;
