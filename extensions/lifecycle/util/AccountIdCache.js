'user strict';

class AccountIdCache extends Map {
    constructor(maxSize) {
        super();
        this.maxSize = maxSize;
        this.misses = new Set();
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
    }

    miss(key) {
        this.misses.add(key);
    }

    isMiss(key) {
        return this.misses.has(key);
    }

    isKnown(key) {
        return this.has(key) || this.isMiss(key);
    }

    getMisses() {
        return [...this.misses].sort();
    }
}

module.exports = {
    AccountIdCache,
};
