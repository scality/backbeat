const { splitter } = require('arsenal').constants;

class BucketQueueEntry {

    /**
     * @constructor
     * @param {string} key - entry key in database
     * @param {string} [value] - JSON string of value
     */
    constructor(key, value) {
        const [ownerCanonicalID, bucketName] = key ? key.split(splitter) : [];
        this._ownerCanonicalID = ownerCanonicalID;
        this._bucket = bucketName;
        this._bucketOwnerKey = key;
        this._value = undefined;
        if (value) {
            try {
                this._value = JSON.parse(value);
            } catch (err) {
                this._value = {};
            }
        }
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

    getBucketOwnerKey() {
        return this._bucketOwnerKey;
    }

    getValue() {
        return this._value;
    }

    getLogInfo() {
        return {
            bucket: this.getBucket(),
        };
    }
}

module.exports = BucketQueueEntry;
