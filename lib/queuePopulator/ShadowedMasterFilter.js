const crypto = require('crypto');

const { isMasterKey } = require('@scality/arsenal').versioning;

const safeJsonParse = require('../util/safeJsonParse');
const { extractBaseKey, extractVersionId } = require('../util/versioning');

// the pair is adjacent in the stream: the margin only covers interleaved
// writes to other objects of the same partition
const DEFAULT_CAPACITY = 100;

/**
 * Drops the master key entry shadowed by the version key entry written just
 * before it for the same version.
 *
 * On MongoDB the version-specific update of a current null version upserts
 * both keys with the same value in one ordered bulk write, the version first
 * (`putObjectVerCase3`): no field tells that copy from a lone null version
 * key, but the two events are adjacent in the change stream and identical,
 * and a no-op write emits no event at all. Regular versioned writes have the
 * same shape, but the extensions skip their master on their own, so they are
 * left alone.
 *
 * Deleting the current version (`deleteObjectVerMaster`) deletes its version
 * key, then the master, turned into a placeholder with a version id of its
 * own, when no other version is left. The placeholder is matched against the
 * version key by giving it back the version id of that key and dropping the
 * flag, which rests on the writer rather than on the events: the placeholder
 * keeps the other fields of the master it replaces (`isNull` included), and
 * any other operation on the object in between rewrites a document and emits
 * an event. The placeholder delete of a null version with no copy is kept: it
 * is then the only event carrying its metadata.
 *
 * Only master entries are ever dropped, so a version key entry always gets
 * through. The window is in memory and holds a digest of the last entry per
 * object: a restart between the two events, or the eviction of the object,
 * lets the master through, and the extensions see the version twice.
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
     * Whether the entry is a master key and the previous entry seen for its
     * object is the version key of that version with the same value (for a
     * put, of a null version only; for a delete, the value the placeholder
     * replaced), and records it.
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
        const digest = this._digest(entry.value);
        const last = this._lastValues.get(objectId);
        let shadowed = false;
        if (last !== undefined && last.type === entry.type && isMasterKey(entry.key) && !isMasterKey(last.key)) {
            shadowed = entry.type === 'delete'
                ? this._isPlaceholderOf(entry, last)
                : last.digest === digest && safeJsonParse(entry.value).result?.isNull === true;
        }

        // re-insert to keep the map ordered by last use
        this._lastValues.delete(objectId);
        this._lastValues.set(objectId, { key: entry.key, type: entry.type, digest });
        if (this._lastValues.size > this._capacity) {
            this._lastValues.delete(this._lastValues.keys().next().value);
        }
        return shadowed;
    }

    // the master was deleted right after the version key: compare it as it
    // was before being turned into a placeholder, the field order is that of
    // ObjectMD on both sides
    _isPlaceholderOf(entry, last) {
        const md = safeJsonParse(entry.value).result;
        if (md?.isPHD !== true) {
            return false;
        }
        delete md.isPHD;
        md.versionId = extractVersionId(last.key);
        return this._digest(JSON.stringify(md)) === last.digest;
    }

    _digest(value) {
        return crypto.createHash('sha1').update(value).digest('base64');
    }
}

module.exports = ShadowedMasterFilter;
