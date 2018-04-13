const VID_SEP = require('arsenal').versioning.VersioningConstants
          .VersionId.Separator;

function _extractVersionedBaseKey(key) {
    return key.split(VID_SEP)[0];
}

class DeleteOpQueueEntry {

    /**
     * @constructor
     * @param {string} bucket - entry bucket
     * @param {string} key - entry key (a versioned key if the entry
     * was for a versioned object or a regular key if no versioning)
     */
    constructor(bucket, key) {
        this._bucket = bucket;
        this._key = key;
        this._objectVersionedKey = key;
        this._objectKey = _extractVersionedBaseKey(key);
    }

    checkSanity() {
        if (typeof this.getBucket() !== 'string') {
            return { message: 'missing bucket name' };
        }
        if (typeof this.getObjectKey() !== 'string') {
            return { message: 'missing key name' };
        }
        return undefined;
    }

    getBucket() {
        return this._bucket;
    }

    getObjectKey() {
        return this._objectKey;
    }

    getObjectVersionedKey() {
        return this._objectVersionedKey;
    }

    isVersion() {
        return this.getObjectKey() === this.getObjectVersionedKey();
    }

    getLogInfo() {
        return {
            bucket: this.getBucket(),
            key: this.getKey(),
        };
    }
}

module.exports = DeleteOpQueueEntry;
