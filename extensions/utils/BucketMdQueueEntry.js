// TODO -- pull this from unified arsenal constant with name of master bucket
// where bucket metadata should be stored in mongo
const masterBucket = '__metastore';

class BucketMdQueueEntry {

    /**
     * @constructor
     * @param {string} instanceBucket - entry's object key which is actually
     * the name of the bucket whose metadata is being updated
     * @param {object} bucketMd - entry's bucket metadata
     */
    constructor(instanceBucket, bucketMd) {
        this._masterBucket = masterBucket;
        this._instanceBucket = instanceBucket;
        this._bucketMdValue = bucketMd;
    }

    checkSanity() {
        if (typeof this._instanceBucket !== 'string') {
            return { message: 'missing instance bucket name' };
        }
        if (typeof this._bucketMdValue !== 'object') {
            return { message: 'missing bucket md value' };
        }
        return undefined;
    }

    getMasterBucket() {
        return this._masterBucket;
    }

    getInstanceBucket() {
        return this._instanceBucket;
    }

    getValue() {
        // TODO: actually use the bucket md model from arsenal to
        // turn the value into a full instance of bucket metadata.
        // otherwise, will be missing bucket metadata fields
        return this._bucketMdValue;
    }

    getLogInfo() {
        return {
            masterBucket: this.getMasterBucket(),
            instanceBucket: this.getInstanceBucket(),
        };
    }
}

module.exports = BucketMdQueueEntry;
