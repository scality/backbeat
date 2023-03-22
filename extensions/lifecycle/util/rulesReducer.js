// moves lifecycle transition deadlines 1 day earlier, only for testing
const transitionOneDayEarlier = process.env.TRANSITION_ONE_DAY_EARLIER === 'true';
// moves lifecycle expiration deadlines 1 day earlier, only for testing
const expireOneDayEarlier = process.env.EXPIRE_ONE_DAY_EARLIER === 'true';

const lowest = (acc, cur) => (acc < cur ? acc : cur);

function sortByPrefix(rules) {
    return rules.sort((a, b) => {
        if (a.Prefix > b.Prefix) {
            return -1;
        }
        if (b.Prefix > a.Prefix) {
            return 1;
        }

        return 0;
    });
}

class RulesReducer {
    /**
     * Constructor of RuleReducer
     *
     * @constructor
     * @param {string} versioningStatus - bucket's version status
     * @param {Date} currentDate - current date
     * @param {Object} bucketLCRules - lifecycle rules
    */
    constructor(versioningStatus, currentDate, bucketLCRules) {
        this.isVersioning = versioningStatus === 'Enabled' || versioningStatus === 'Suspended';
        this.currentDate = currentDate;
        this.bucketLCRules = bucketLCRules;
    }

    /**
     * toListings: get listing informations (list params...) from lifecycle rules.
     * @return {object} aggregatedRules - aggregated rules for each type
     * @return {array} aggregatedRules.currents - array of rules
     * @return {array} aggregatedRules.nonCurrents - array of rules
     * @return {array} aggregatedRules.orphans - array of rules
     */
    toListings() {
        if (this.isVersioning) {
            return this._getListingsForVersionedBucket();
        }
        return this._getListingsForNonVersionedBucket();
    }

    /**
     * _getListingsForNonVersionedBucket: get listings infos from lifecycle rules of a non-versioned bucket
     * On a non-versioned bucket we can perform one type of listing:
     * - "current" targeting current objects (Expiration and Transitions).
     * @return {object} aggregatedRules - aggregated rules for each type
     * @return {array} aggregatedRules.currents - array of rules
     */
    _getListingsForNonVersionedBucket() {
        // TODO: check status
        const reducedRules = sortByPrefix(this.bucketLCRules).reduce((accumulator, r) => {
            const currents = accumulator.currents;
            const reducedCurrents = this._reduceCurrentRules(r, currents);
            return { currents: reducedCurrents };
        }, { currents: [] });

        return reducedRules;
    }

    /**
     * _getListingsForVersionedBucket: get listings infos from lifecycle rules of a versioned bucket
     * On a versioned bucket we can perform three types of listing:
     * - "current" targeting current objects (Expiration and Transitions).
     * - "noncurrent" targeting non-current obejcts (NonCurrentExpiration and NonCurrentTransitions).
     * - "orphan" targeting orphan delete markers (Expiration and Expiration.ExpiredObjectDeleteMarker).
     * @return {object} aggregatedRules - aggregated rules for each type
     * @return {array} aggregatedRules.currents - array of rules
     * @return {array} aggregatedRules.nonCurrents - array of rules
     * @return {array} aggregatedRules.orphans - array of rules
     */
    _getListingsForVersionedBucket() {
        // TODO: check status
        const reducedRules = sortByPrefix(this.bucketLCRules).reduce((accumulator, r) => {
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
     * _reduceCurrentRules: evaluate a given rule and add its result to the listings array.
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
        const prefix = r.Prefix;
        const isTransitions = r.Transitions && r.Transitions[0];
        let days;

        // TODO: Handle the case when only one transition (no expiration) rule for a given prefix.
        //       It will introcude DataStoreName to the listing params.

        if (r.Expiration) {
            // NOTE: Expiration Days cannot be 0.
            if (r.Expiration.Days) {
                days = r.Expiration.Days;
            } else if (r.Expiration.Date) {
                if (r.Expiration.Date <= this.currentDate) {
                    days = 0;
                }
            }
        }

        if (isTransitions) {
            // NOTE: Transitions Days cannot be 0.
            // NOTE: Cannot mixed 'Date' and 'Days' based Transition actions.
            if (r.Transitions[0].Days !== undefined) {
                const lowestTransitionDays = r.Transitions.map(t => t.Days).reduce(lowest);
                days = days === undefined ? lowestTransitionDays : Math.min(days, lowestTransitionDays);

            } else if (r.Transitions[0].Date) {
                const lowestDate = r.Transitions.map(t => t.Date).reduce(lowest);

                if (lowestDate <= this.currentDate) {
                    days = 0;
                }
            }
        }

        return this._aggregateByPrefix(currents, prefix, days);
    }

    /**
     * _reduceNonCurrentRules: evaluate a given rule and add its result to the listings array.
     * The listings will be used to defined the parameters of "list non-current versions".
     * @param {object} r - lifecycle rule to be evaluated
     * @param {array} nonCurrents - array of non-current listings information
     * @return {array} aggregatedListings - array of non-current listings information after evaluation
     */
    _reduceNonCurrentRules(r, nonCurrents) {
        if (r.Status !== 'Enabled') {
            return nonCurrents;
        }

        const prefix = r.Prefix;
        const isTransitions = r.NoncurrentVersionTransitions && r.NoncurrentVersionTransitions.length > 0;
        let days;

        if (r.NoncurrentVersionExpiration) {
            // 'NoncurrentDays' for NoncurrentVersionExpiration action is a positive integer
            if (r.NoncurrentVersionExpiration.NoncurrentDays) {
                days = r.NoncurrentVersionExpiration.NoncurrentDays;
            }
        }

        if (isTransitions) {
            // NoncurrentDays for NoncurrentVersionTransitions action can be 0
            if (r.NoncurrentVersionTransitions[0].NoncurrentDays !== undefined) {
                const lowestTransitionDays = r.NoncurrentVersionTransitions
                .map(t => t.NoncurrentDays).reduce(lowest);
                days = days === undefined ? lowestTransitionDays : Math.min(days, lowestTransitionDays);
            }
        }

        return this._aggregateByPrefix(nonCurrents, prefix, days);
    }

    /**
     * _reduceOrphanDeleteMarkerRule: evaluate a given rule and add its result to the listings array.
     * The listings will be used to defined the parameters of "list orphan delete markers".
     * @param {object} r - lifecycle rule to be evaluated
     * @param {array} orphans - array of "orphan delete markers" listings information
     * @return {array} aggregatedListings - array of "orphan delete markers" listings information after evaluation
     */
    _reduceOrphanDeleteMarkerRule(r, orphans) {
        if (r.Status !== 'Enabled') {
            return orphans;
        }

        const prefix = r.Prefix;
        let days;

        // When you specify the Days tag, Amazon S3 automatically performs ExpiredObjectDeleteMarker
        // cleanup when the delete markers are old enough to satisfy the age criteria.
        if (r.Expiration) {
            if (r.Expiration.Days) {
                days = r.Expiration.Days;
            } else if (r.Expiration.Date) {
                if (r.Expiration.Date <= this.currentDate) {
                    days = 0;
                }
            } else if (r.Expiration.ExpiredObjectDeleteMarker) {
                days = 0;
            }
        }

        return this._aggregateByPrefix(orphans, prefix, days);
    }

    /**
     * _aggregateByPrefix: add a new listing and aggregate by prefix
     * e.g [{ prefix: 's/c/a', days: 1 }, { prefix: 's/c', days: 5 },  { prefix: 's', days: 10 }]
     * should return [{ prefix: 's', days: 1 }] after aggregating by prefix
     * @param {array} listings - [listings] listings
     * @param {string} listings.prefix - rules' prefix
     * @param {string} listings.days - number of days after which action should apply
     * @param {string} prefix - currently evaluated rule prefix
     * @param {string} days - currently evaluated rule days
     * @return {array} aggregatedListings
     */
    _aggregateByPrefix(listings, prefix, days) {
        // if days is undefined, no rule matches.
        if (days === undefined) {
            return listings;
        }

        let aggregatedListings = listings;
        let finalDays = days;
        const withPrefix = listings.filter(m => m.prefix.startsWith(prefix));
        // NOTE: rules are sorted by prefix in descending alphabetical order.
        // (rule with prefix "toto/titi" is evaluated before "toto").
        // If the prefix of the previously evaluated rules starts with the currently evaluated
        // (by definition widest) prefix, we can remove the previously evaluated rules that start with the prefix
        // and add a new rule with the evaluated (widest) prefix.

        if (withPrefix.length) {
            // To make sure we do not miss any key, the value "days" of the new rule will be the minimum number of days
            // allowed by any aggregated rule.
            const lowestDays = withPrefix.map(r => r.days).reduce(lowest);

            if (lowestDays < days) {
                finalDays = lowestDays;
            }

            aggregatedListings = listings.filter(m => !m.prefix.startsWith(prefix));
        }

        if ((transitionOneDayEarlier || expireOneDayEarlier) && finalDays > 0) {
            finalDays -= 1;
        }

        aggregatedListings.push({
            prefix,
            days: finalDays,
        });

        return aggregatedListings;
    }
}

module.exports = {
    RulesReducer,
};
