'use strict'; // eslint-disable-line strict

const { EventEmitter } = require('events');

/**
 * Probe used for deep healthchecks to store data and catch event data
 *
 * @class
 */
class Probe extends EventEmitter {
    /**
     * @constructor
     * @param {string} id - uuid assigned to a probe
     */
    constructor(id) {
        super();
        this.id = id;
        this.store = null;
    }

    /**
     * Get the number of undefined values for partitions in store
     * @return {number} - count of current unset values for partitions
     */
    checkStore() {
        let count = 0;
        Object.keys(this.store).forEach(partition => {
            if (this.store[partition] === undefined) {
                count++;
            }
        });
        return count;
    }

    /**
     * Get unique identifier
     * @return {string} unique identifier for this probe
     */
    getId() {
        return this.id;
    }

    /**
     * Get store, can be null
     * @return {object|null} - internal store data for this probe
     */
    getStore() {
        return this.store;
    }

    setStoreData(key, value) {
        this.store[key] = value;
    }

    /**
     * Set all undefined values in store to 'error'
     * @return {undefined}
     */
    setStoreErrors() {
        for (const partition in this.store) {
            if (this.store[partition] === undefined) {
                this.setStoreData(partition, 'error');
            }
        }
    }

    /**
     * Initial setup for internal store, setting all values to undefined
     * @param {array} partitions - array of partition numbers
     * @return {undefined}
     */
    setupStore(partitions) {
        this.store = partitions.reduce((store, partition) => {
            // eslint-disable-next-line
            store[partition] = undefined;
            return store;
        }, {});
    }
}

module.exports = Probe;
