'use strict'; // eslint-disable-line strict

class MetricsModel {
    /**
     * constructor
     * @param {Number} ops - number of operations
     * @param {Number} bytes - data size in bytes
     * @param {String} extension - extension
     * @param {String} type - operation indicator (queued or processed)
     * @param {String} bucket - bucket name
     */

    constructor(ops, bytes, extension, type, bucket) {
        this._timestamp = Date.now();
        this._ops = ops;
        this._bytes = bytes;
        this._extension = extension;
        this._type = type;
        this._bucket = bucket;
    }

    serialize() {
        return JSON.stringify({
            timestamp: this._timestamp,
            ops: this._ops,
            bytes: this._bytes,
            extension: this._extension,
            type: this._type,
            bucket: this._bucket,
        });
    }
}

module.exports = MetricsModel;
