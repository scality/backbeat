const { usersBucket } = require('arsenal').constants;

class RaftLogEntry {

    /**
     * method to format an entry for object metadata
     *
     * @param {object} objectMd - object info to format entry
     * @param {string} bucketPrefix - prefix for bucketname to avoid name clash
     * @return {object} JSON.stringified entry value to be sent to kafka
     */
    createPutEntry(objectMd, bucketPrefix) {
        return {
            type: 'put',
            bucket: `${bucketPrefix}_${objectMd.bucketName}`,
            key: objectMd.objectKey,
            value: JSON.stringify(objectMd.res),
        };
    }

    /**
     * method to format an entry for a bucket - formatted as an object that is
     * a part of the usersBucket
     *
     * @param {object} bucket - bucket info to format entry
     * @param {string} bucketPrefix - prefix for bucketname to avoid name clash
     * @return {object} formatted entry for bucket as an object
     */
    createPutBucketEntry(bucket, bucketPrefix) {
        return {
            type: 'put',
            bucket: usersBucket,
            key: `${bucketPrefix}_${bucket}`,
            value: null,
        };
    }

    /**
     * method to format an entry for bucket metadadta
     *
     * @param {object} bucket - bucket info to format entry
     * @param {string} bucketPrefix - prefix for bucketname to avoid name clash
     * @return {object} formatted entry for bucket metadata
     */
    createPutBucketMdEntry(bucket, bucketPrefix) {
        return {
            type: 'put',
            bucket: `${bucketPrefix}_${bucket._name}`,
            key: `${bucketPrefix}_${bucket._name}`,
            value: bucket.serialize(),
        };
    }
}

module.exports = RaftLogEntry;
