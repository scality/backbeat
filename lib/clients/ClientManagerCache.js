/**
 * Holds the ClientManager instances shared by every entry a processor
 * handles, keyed by whatever identifies the credentials they carry
 * (typically endpoint and role).
 *
 * A manager that has drained its credentials is dropped and closed, so an
 * endpoint or a role that stops being replicated to does not keep its
 * agents and its sweep timer around for the life of the process.
 */
class ClientManagerCache {
    constructor() {
        this._managers = new Map();
    }

    /**
     * Return the manager cached under `key`, creating it on first use.
     * @param {String} key - identifies the credentials the manager carries
     * @param {Function} create - builds the manager, called on a miss only
     * @return {ClientManager} the cached manager
     */
    getOrCreate(key, create) {
        const cached = this._managers.get(key);
        if (cached) {
            return cached;
        }

        const manager = create();
        this._managers.set(key, manager);
        manager.once('idle', () => {
            // an in-flight task may still hold it: closing only releases the
            // sweep and the idle sockets, the clients keep working
            if (this._managers.get(key) === manager) {
                this._managers.delete(key);
            }
            manager.close();
        });
        return manager;
    }

    get size() {
        return this._managers.size;
    }

    /**
     * Close every manager held and forget them.
     * @return {undefined}
     */
    close() {
        this._managers.forEach(manager => manager.close());
        this._managers.clear();
    }
}

module.exports = ClientManagerCache;
