const constants = require('../../constants');
class RaftLogEntry {
    createPutEntry(bucket, objectKey, objectVal) {
        return JSON.stringify({
            type: 'put', bucket,
            key: objectKey,
            value: JSON.stringify(objectVal),
        });
    }

    createPutBucketEntry(bucket) {
        console.log('CREATEPUTBUCKETENTRY', bucket);
        console.log(bucket.value);
        console.log('JSON STRINGIFY', JSON.stringify(bucket.value));
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
