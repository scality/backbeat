const lowest = (acc, cur) => (acc < cur ? acc : cur);

class RulesReducer {
    /**
     * Constructor of RulesReducer
     *
     * @constructor
     * @param {string} versioningStatus - bucket's version status: 'Enabled', 'Disabled' or 'Suspended'
     * @param {Date} currentDate - current date
     * @param {Object} bucketLCRules - lifecycle rules
     * @param {Object} options - rule lifecycle options
     * @param {boolean} options.expireOneDayEarlier - moves lifecycle expiration deadlines 1 day earlier
     * @param {boolean} options.transitionOneDayEarlier - moves lifecycle transition deadlines 1 day earlier
    */
    constructor(versioningStatus, currentDate, bucketLCRules, options) {
        this._isVersioning = versioningStatus === 'Enabled' || versioningStatus === 'Suspended';
        this._currentDate = currentDate;
        this._bucketLCRules = this._sortByPrefix(bucketLCRules);
        this._expireOneDayEarlier = options.expireOneDayEarlier;
        this._transitionOneDayEarlier = options.transitionOneDayEarlier;
    }

    /**
     * _getRulePrefix: retrieve prefix from a rule
     * @param {object} r - lifecycle rule
     * @return {string} prefix
     */
    _getRulePrefix(r) {
        return r.Prefix || (r.Filter && (r.Filter.And ? r.Filter.And.Prefix : r.Filter.Prefix)) || '';
    }

    /**
     * _sortByPrefix: sorts the lifecycle rules in ascending order based on the value of the Prefix property
     * @param {array} rules - lifecycle rules
     * @return {array} returns the sorted rules
     */
    _sortByPrefix(rules) {
        return rules.sort((a, b) => this._getRulePrefix(a).localeCompare(this._getRulePrefix(b)));
    }

    /**
     * toListings: determines the list of API calls needed to list all eligible objects based on
     * the bucket lifecycle rules.
     * @return {object} aggregatedRules - aggregated rules for each type
     * @return {array} aggregatedRules.currents - array of rules
     * @return {array} aggregatedRules.nonCurrents - array of rules
     * @return {array} aggregatedRules.orphans - array of rules
     */
    toListings() {
        if (this._isVersioning) {
            return this._getListingsForVersionedBucket();
        }
        return this._getListingsForNonVersionedBucket();
    }

    /**
     * _decrementExpirationDay: moves lifecycle expiration deadlines 1 day earlier.
     * @param {number} days - Indicates the lifetime, in days, of the objects that are subject to the rule.
     * @return {number} days
     */
    _decrementExpirationDay(days) {
        if (days > 0 && this._expireOneDayEarlier) {
            return days - 1;
        }
        return days;
    }

    /**
     * _decrementTransitionDay: moves lifecycle transition deadlines 1 day earlier.
     * @param {number} days - Indicates the lifetime, in days, of the objects that are subject to the rule.
     * @return {number} days
     */
    _decrementTransitionDay(days) {
        if (days > 0 && this._transitionOneDayEarlier) {
            return days - 1;
        }
        return days;
    }

    /**
     * _getListingsForNonVersionedBucket: gets listings infos from lifecycle rules of a non-versioned bucket
     * NOTE: On a non-versioned bucket, only one type of listing can be performed:
     * - "current" targeting current objects (Expiration and Transitions).
     * @return {object} aggregatedRules - aggregated rules for each type
     * @return {array} aggregatedRules.currents - array of rules
     */
    _getListingsForNonVersionedBucket() {
        const reducedRules = this._bucketLCRules.reduce((accumulator, r) => {
            const currents = accumulator.currents;
            const reducedCurrents = this._reduceCurrentRules(r, currents);
            return { currents: reducedCurrents };
        }, { currents: [] });

        return reducedRules;
    }

    /**
     * _getListingsForVersionedBucket: gets listings infos from lifecycle rules of a versioned bucket
     * NOTE: On a versioned bucket, three types of listing can be performed:
     * - "current" targeting current objects (Expiration and Transitions).
     * - "noncurrent" targeting non-current obejcts (NonCurrentExpiration and NonCurrentTransitions).
     * - "orphan" targeting orphan delete markers (Expiration and Expiration.ExpiredObjectDeleteMarker).
     * @return {object} aggregatedRules - aggregated rules for each type
     * @return {array} aggregatedRules.currents - array of rules
     * @return {array} aggregatedRules.nonCurrents - array of rules
     * @return {array} aggregatedRules.orphans - array of rules
     */
    _getListingsForVersionedBucket() {
        const reducedRules = this._bucketLCRules.reduce((accumulator, r) => {
            const nonCurrents = accumulator.nonCurrents;
            const currents = accumulator.currents;
            const orphans = accumulator.orphans;

            const reducedCurrents = this._reduceCurrentRules(r, currents);
            const reducedNonCurrents = this._reduceNonCurrentRules(r, nonCurrents);
            const reducedOrphans = this._reduceOrphanDeleteMarkerRule(r, orphans);

            return { currents: reducedCurrents, nonCurrents: reducedNonCurrents, orphans: reducedOrphans };
        }, { currents: [], nonCurrents: [], orphans: [] });

        return reducedRules;
    }

    /**
     * _reduceCurrentRules: checks if the rule targets current versions and, if so,
     * adds its result to the listings array.
     * The listings will be used to defined the parameters of "list current versions".
     * @param {object} r - lifecycle rule to be evaluated
     * @param {array} currents - array of current listings information
     * @return {array} aggregatedListings - array of current listings information after evaluation
     */
    _reduceCurrentRules(r, currents) {
        // handle the case when rule is disabled.
        if (r.Status !== 'Enabled') {
            return currents;
        }
        const prefix = this._getRulePrefix(r);
        const isTransitions = r.Transitions && r.Transitions[0];
        let days;

        // IMPROVEMENT: ZENKO-4541 If only one transition (no expiration) rule is set for a given prefix,
        // we can add the filter "value.dataStoreName != destinationLocationName" to our listing query
        // to only return keys that have not been transitioned yet.

        if (r.Expiration) {
            // NOTE: Expiration Days cannot be 0.
            if (r.Expiration.Days) {
                days = this._decrementExpirationDay(r.Expiration.Days);
            } else if (r.Expiration.Date) {
                if (r.Expiration.Date <= this._currentDate) {
                    days = 0;
                }
            }
        }

        if (isTransitions) {
            // NOTE: Transitions Days cannot be 0.
            // NOTE: Cannot mixed 'Date' and 'Days' based Transition actions.
            if (r.Transitions[0].Days !== undefined) {
                let lowestTransitionDays = r.Transitions.map(t => t.Days).reduce(lowest);
                lowestTransitionDays = this._decrementTransitionDay(lowestTransitionDays);
                days = days === undefined ? lowestTransitionDays : Math.min(days, lowestTransitionDays);

            } else if (r.Transitions[0].Date) {
                const lowestDate = r.Transitions.map(t => t.Date).reduce(lowest);

                if (lowestDate <= this._currentDate) {
                    days = 0;
                }
            }
        }

        return this._aggregateByPrefix(currents, prefix, days);
    }

    /**
     * _reduceNonCurrentRules: checks if the rule targets non-current versions and, if so,
     * add its result to the listings array.
     * The listings will be used to defined the parameters of "list non-current versions".
     * @param {object} r - lifecycle rule to be evaluated
     * @param {array} nonCurrents - array of non-current listings information
     * @return {array} aggregatedListings - array of non-current listings information after evaluation
     */
    _reduceNonCurrentRules(r, nonCurrents) {
        if (r.Status !== 'Enabled') {
            return nonCurrents;
        }

        const prefix = this._getRulePrefix(r);
        const isTransitions = r.NoncurrentVersionTransitions && r.NoncurrentVersionTransitions.length > 0;
        let days;

        if (r.NoncurrentVersionExpiration) {
            // 'NoncurrentDays' for NoncurrentVersionExpiration action is a positive integer
            if (r.NoncurrentVersionExpiration.NoncurrentDays) {
                days = this._decrementExpirationDay(r.NoncurrentVersionExpiration.NoncurrentDays);
            }
        }

        if (isTransitions) {
            // NoncurrentDays for NoncurrentVersionTransitions action can be 0
            if (r.NoncurrentVersionTransitions[0].NoncurrentDays !== undefined) {
                let lowestTransitionDays = r.NoncurrentVersionTransitions
                    .map(t => t.NoncurrentDays).reduce(lowest);
                lowestTransitionDays = this._decrementTransitionDay(lowestTransitionDays);
                days = days === undefined ? lowestTransitionDays : Math.min(days, lowestTransitionDays);
            }
        }

        return this._aggregateByPrefix(nonCurrents, prefix, days);
    }

    /**
     * _reduceOrphanDeleteMarkerRule: checks if the rule targets orphan delete markers and, if so,
     * adds its result to the listings array.
     * The listings will be used to defined the parameters of "list orphan delete markers".
     * @param {object} r - lifecycle rule to be evaluated
     * @param {array} orphans - array of "orphan delete markers" listings information
     * @return {array} aggregatedListings - array of "orphan delete markers" listings information after evaluation
     */
    _reduceOrphanDeleteMarkerRule(r, orphans) {
        if (r.Status !== 'Enabled') {
            return orphans;
        }

        const prefix = this._getRulePrefix(r);
        let days;

        // NOTE: When you specify the Days tag, Amazon S3 automatically performs ExpiredObjectDeleteMarker
        // cleanup when the delete markers are old enough to satisfy the age criteria.
        if (r.Expiration) {
            if (r.Expiration.Days) {
                days = this._decrementExpirationDay(r.Expiration.Days);
            } else if (r.Expiration.Date) {
                if (r.Expiration.Date <= this._currentDate) {
                    days = 0;
                }
            } else if (r.Expiration.ExpiredObjectDeleteMarker) {
                days = 0;
            }
        }

        return this._aggregateByPrefix(orphans, prefix, days);
    }

    /**
     * _aggregateByPrefix: adds a new listing or aggregate by prefix
     * e.g [{ prefix: 's', days: 10 }, { prefix: 's/c', days: 5 }, { prefix: 's/c/a', days: 1 }]
     * should return [{ prefix: 's', days: 1 }] after aggregating by prefix
     * @param {array} listings - [listings] listings
     * @param {string} listings.prefix - rules' prefix
     * @param {string} listings.days - number of days after which action should apply
     * @param {string} prefix - prefix of the currently evaluated rule
     * @param {string} days - days of the currently evaluated rule
     * @return {array} aggregatedListings
     */
    _aggregateByPrefix(listings, prefix, days) {
        // NOTE: if days is undefined, no listing created.
        if (days === undefined) {
            return listings;
        }

        const previousListing = listings[listings.length - 1];
        const shareListing = previousListing && prefix.startsWith(previousListing.prefix);
        if (shareListing) {
            if (days < previousListing.days) {
                previousListing.days = days;
            }
        } else {
            listings.push({
                prefix,
                days,
            });
        }

        return listings;
    }
}

module.exports = {
    RulesReducer,
};
