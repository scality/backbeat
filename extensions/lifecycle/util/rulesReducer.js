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
    constructor(versioningStatus, currentDate, bucketLCRules) {
        this.isVersioning = versioningStatus === 'Enabled' || versioningStatus === 'Suspended';
        this.currentDate = currentDate;
        this.bucketLCRules = bucketLCRules;
        console.log('isVesioning!!!', this.isVersioning);
    }

    reduce() {
        if (this.isVersioning) {
            return this._reduceRulesForVersionedBucket();
        }
        return this._reduceRulesForNonVersionedBucket();
    }

    _reduceRulesForNonVersionedBucket() {
        // TODO: check status
        const reducedRules = sortByPrefix(this.bucketLCRules).reduce((accumulator, r) => {
            const currents = accumulator.currents;
            const reducedCurrents = this._reduceCurrentRules(r, currents);
            return { currents: reducedCurrents };
        }, { currents: [] });

        return reducedRules;
    }

    _reduceRulesForVersionedBucket() {
        // TODO: check status
        const reducedRules = sortByPrefix(this.bucketLCRules).reduce((accumulator, r) => {
            const nonCurrents = accumulator.nonCurrents;
            const currents = accumulator.currents;
            const orphans = accumulator.orphans;

            const reducedCurrents = this._reduceCurrentRules(r, currents);
            const reducedNonCurrents = this._reduceNonCurrentRules(r, nonCurrents);
            console.log('r1!!!', r);
            const reducedOrphans = this._reduceOrphanDeleteMarkerRule(r, orphans);

            return { currents: reducedCurrents, nonCurrents: reducedNonCurrents, orphans: reducedOrphans };
        }, { currents: [], nonCurrents: [], orphans: [] });

        return reducedRules;
    }

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
                if (new Date(r.Expiration.Date) <= this.currentDate) {
                    days = 0;
                }
            }
        }

        console.log('days after Expiration!!!', days);

        if (isTransitions) {
            // NOTE: Transitions Days cannot be 0.
            // NOTE: Cannot mixed 'Date' and 'Days' based Transition actions.
            if (r.Transitions[0].Days !== undefined) {
                const lowestTransitionDays = r.Transitions.map(t => t.Days).reduce(lowest);
                days = days === undefined ? lowestTransitionDays : Math.min(days, lowestTransitionDays);

            } else if (r.Transitions[0].Date) {
                const lowestDate = r.Transitions.map(t => new Date(t.Date)).reduce(lowest);
                console.log('Transition lowestDate!!!', lowestDate);
                console.log('this.currentDate!!!', this.currentDate);

                if (lowestDate <= this.currentDate) {
                    days = 0;
                }
            }
        }

        console.log('ALMOST FINAL DAYS!!!', days);

        return this._aggregateByPrefix(currents, prefix, days);
    }

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
            // 'NoncurrentDays' for NoncurrentVersionExpiration action can be 0
            if (r.NoncurrentVersionTransitions[0].NoncurrentDays !== undefined) {
                const lowestTransitionDays = r.NoncurrentVersionTransitions
                .map(t => t.NoncurrentDays).reduce(lowest);
                days = days === undefined ? lowestTransitionDays : Math.min(days, lowestTransitionDays);
            }
        }

        return this._aggregateByPrefix(nonCurrents, prefix, days);
    }

    _reduceOrphanDeleteMarkerRule(r, orphans) {
        if (r.Status !== 'Enabled') {
            return orphans;
        }

        const prefix = r.Prefix;
        let days;
        console.log('r!!!', r);

        // When you specify the Days tag, Amazon S3 automatically performs ExpiredObjectDeleteMarker
        // cleanup when the delete markers are old enough to satisfy the age criteria.
        if (r.Expiration) {
            if (r.Expiration.Days) {
                days = r.Expiration.Days;
            } else if (r.Expiration.Date) {
                if (new Date(r.Expiration.Date) <= this.currentDate) {
                    days = 0;
                }
            } else if (r.Expiration.ExpiredObjectDeleteMarker) {
                days = 0;
            }
        }
        console.log('days!!!', days);

        return this._aggregateByPrefix(orphans, prefix, days);
    }

    _aggregateByPrefix(rules, prefix, days) {
        // if days is undefined, no rule matches.
        if (days === undefined) {
            return rules;
        }

        let aggregatedRules = rules;
        let finalDays = days;
        const withPrefix = rules.filter(m => m.prefix.startsWith(prefix));

        if (withPrefix.length) {
            // lowest days
            const lowestDays = withPrefix.map(r => r.days).reduce(lowest);

            if (lowestDays < days) {
                finalDays = lowestDays;
            }

            aggregatedRules = rules.filter(m => !m.prefix.startsWith(prefix));
        }

        if ((transitionOneDayEarlier || expireOneDayEarlier) && finalDays > 0) {
            finalDays -= 1;
        }

        aggregatedRules.push({
            prefix,
            days: finalDays,
        });

        return aggregatedRules;
    }
}

module.exports = {
    RulesReducer,
};
