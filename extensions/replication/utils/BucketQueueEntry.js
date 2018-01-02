const { splitter } = require('arsenal').constants;

class BucketQueueEntry {

    /**
     * @constructor
     * @param {string} key - entry key in database
     */
    constructor(key) {
        const [ownerCanonicalID, bucketName] = key ? key.split(splitter) : [];
        this._ownerCanonicalID = ownerCanonicalID;
        this._bucket = bucketName;
    }

    checkSanity() {
        if (typeof this._ownerCanonicalID !== 'string') {
            return { message: 'missing owner canonical ID' };
        }
        if (typeof this._bucket !== 'string') {
            return { message: 'missing bucket name' };
        }
        return undefined;
    }

    getBucket() {
        return this._bucket;
    }

    getCanonicalKey() {
        return this.getBucket();
    }

    getOwnerCanonicalID() {
        return this._ownerCanonicalID;
    }

    getLogInfo() {
        return {
            bucket: this.getBucket(),
        };
    }
}

module.exports = BucketQueueEntry;
