'user strict';

// Canonical ids which failed to resolve are only remembered for a short
// while: an account may be (re)created at any time, e.g. when data arrives
// before the accounts in a DR scenario.
const DEFAULT_MISS_TTL_MS = 5 * 60 * 1000;
// misses are only keys, and are mostly bounded by their TTL: this is a safety
// net against a burst of unresolvable ids, not a working-set size
const DEFAULT_MAX_MISSES = 1000;

class AccountIdCache extends Map {
    constructor(maxSize, missTTLms = DEFAULT_MISS_TTL_MS, maxMisses = DEFAULT_MAX_MISSES) {
        super();
        this.maxSize = maxSize;
        this.missTTLms = missTTLms;
        this.maxMisses = maxMisses;
        this.misses = new Map(); // canonical id -> expiration timestamp
    }

    expireOldest() {
        let overflowKeysNum = this.size - this.maxSize;
        const keyIterator = this.keys();
        let current = keyIterator.next();

        while (overflowKeysNum > 0 && !current.done) {
            this.delete(current.value);
            current = keyIterator.next();
            overflowKeysNum--;
        }

        this._expireMisses();
    }

    _expireMisses() {
        // all misses use the same TTL and are (re)inserted on `miss()`, so the
        // map is ordered by expiration: stop at the first live entry
        const now = Date.now();
        for (const [key, expiresAt] of this.misses) {
            if (expiresAt > now) {
                break;
            }
            this.misses.delete(key);
        }
    }

    miss(key) {
        // re-insert, to keep the map ordered by expiration
        this.misses.delete(key);
        this.misses.set(key, Date.now() + this.missTTLms);

        while (this.misses.size > this.maxMisses) {
            this.misses.delete(this.misses.keys().next().value);
        }
    }

    isMiss(key) {
        const expiresAt = this.misses.get(key);
        if (expiresAt === undefined) {
            return false;
        }

        if (expiresAt <= Date.now()) {
            this.misses.delete(key);
            return false;
        }

        return true;
    }

    isKnown(key) {
        return this.has(key) || this.isMiss(key);
    }

    get missCount() {
        this._expireMisses();
        return this.misses.size;
    }
}

module.exports = {
    AccountIdCache,
    DEFAULT_MISS_TTL_MS,
    DEFAULT_MAX_MISSES,
};
