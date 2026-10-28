const crypto = require('crypto');

const { isMasterKey } = require('@scality/arsenal').versioning;

const safeJsonParse = require('../util/safeJsonParse');
const { extractBaseKey } = require('../util/versioning');

// the pair is adjacent in the stream: the margin only covers interleaved
// writes to other objects of the same partition
const DEFAULT_CAPACITY = 100;

/**
 * Drops the master key entry of a null version shadowed by the version key
 * entry written just before it with the same value.
 *
 * On MongoDB the version-specific update of a current null version upserts
 * both keys with the same value in one ordered bulk write, the version first
 * (`putObjectVerCase3`): no field tells that copy from a lone null version
 * key, but the two events are adjacent in the change stream and identical,
 * and a no-op write emits no event at all. Regular versioned writes have the
 * same shape, but the extensions skip their master on their own, so they are
 * left alone.
 *
 * Only master entries are ever dropped, so a version key entry always gets
 * through. The window is in memory and holds a digest of the last entry per
 * object: a restart between the two events lets the master through, which
 * the consumers tolerate.
 */
class ShadowedMasterFilter {
    /**
     * @param {Object} [params] - parameters
     * @param {number} [params.capacity] - number of objects remembered
     */
    constructor(params = {}) {
        this._capacity = params.capacity || DEFAULT_CAPACITY;
        this._lastValues = new Map();
    }

    /**
     * Whether the entry is the master key of a null version and the previous
     * entry seen for its object is the version key with the same value, and
     * records it.
     *
     * @param {Object} entry - log entry
     * @param {string} entry.bucket - bucket name
     * @param {string} entry.key - object key, without the v1 prefix
     * @param {string} entry.type - entry type
     * @param {string} [entry.value] - object metadata, serialized
     * @return {boolean} true if the entry is to be dropped
     */
    isShadowed(entry) {
        if (!entry.key || entry.value === undefined) {
            return false;
        }
        const objectId = `${entry.bucket}/${extractBaseKey(entry.key)}`;
        const digest = crypto.createHash('sha1').update(entry.value).digest('base64');
        const last = this._lastValues.get(objectId);
        const shadowed = last !== undefined && isMasterKey(entry.key) &&
            last.key !== entry.key &&
            last.type === entry.type && last.digest === digest &&
            safeJsonParse(entry.value).result?.isNull === true;

        // re-insert to keep the map ordered by last use
        this._lastValues.delete(objectId);
        this._lastValues.set(objectId, { key: entry.key, type: entry.type, digest });
        if (this._lastValues.size > this._capacity) {
            this._lastValues.delete(this._lastValues.keys().next().value);
        }
        return shadowed;
    }
}

module.exports = ShadowedMasterFilter;
