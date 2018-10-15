'use strict'; // eslint-disable-line strict

class MetricsModel {
    /**
     * constructor
     * @param {Number} ops - number of operations
     * @param {Number} bytes - data size in bytes
     * @param {String} extension - extension
     * @param {String} type - operation indicator (queued, completed, failed)
     * @param {String} site - site name
     */

    constructor(ops, bytes, extension, type, site) {
        this._timestamp = Date.now();
        this._ops = ops;
        this._bytes = bytes;
        this._extension = extension;
        this._type = type;
        this._site = site;
    }

    serialize() {
        return JSON.stringify({
            timestamp: this._timestamp,
            ops: this._ops,
            bytes: this._bytes,
            extension: this._extension,
            type: this._type,
            site: this._site,
        });
    }
}

module.exports = MetricsModel;
