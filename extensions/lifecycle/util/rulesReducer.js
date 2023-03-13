const { lifecycleListing: { NON_CURRENT_TYPE, CURRENT_TYPE, ORPHAN_TYPE } } = require('../../../lib/constants');

// const bucketLCRules = [{
//     Expiration: { Days: 5 },
//     ID: '456',
//     Prefix: 'toto',
//     Status: 'Enabled',
//     Transitions: [],
//     NoncurrentVersionTransitions: [],
// }, {
//     Expiration: { Date: '2024-01-01T00:00:00.019Z' },
//     ID: '456',
//     Prefix: 'rtoto',
//     Status: 'Enabled',
//     Transitions: [],
//     NoncurrentVersionExpiration: {
//         NoncurrentDays: 1
//     },
//     NoncurrentVersionTransitions: [{
//         NoncurrentDays: 10,
//         StorageClass: 'aws-location',
//     }],
// }, {
//     Expiration: { Days: 1 },
//     ID: '456',
//     Prefix: 'vto',
//     Status: 'Enabled',
//     Transitions: [],
//     NoncurrentVersionTransitions: [],
// }, {
//     Expiration: { Days: 20 },
//     ID: '456',
//     Prefix: 't',
//     Status: 'Enabled',
//     Transitions: [],
//     NoncurrentVersionTransitions: [{
//         NoncurrentDays: 2,
//         StorageClass: 'aws-location',
//     }, {
//         NoncurrentDays: 10,
//         StorageClass: 'aws-location2',
//     }],
// }, {
//     Expiration: { Days: 10 },
//     ID: '456',
//     Prefix: 't',
//     Status: 'Enabled',
//     Transitions: [{
//         Date: '2022-01-01T00:00:00.019Z',
//         StorageClass: 'aws-location',
//     }, {
//         Date: '2024-01-01T00:00:00.019Z',
//         StorageClass: 'aws-location2',
//     }],
//     NoncurrentVersionTransitions: [],
//     NoncurrentVersionExpiration: {
//         NoncurrentDays: 12,
//     }
// }];

// Default max AWS limit is 1000 for both list objects and list object versions
// TODO: MAX_KEYS TO MOVE BACK TO 1000
const MAX_KEYS = process.env.CI === 'true' ? 3 : 1;

const bucketLCRules = [{
    Expiration: { Days: 5 },
    ID: '456',
    Prefix: 'toto',
    Status: 'Enabled',
    Transitions: [],
    NoncurrentVersionTransitions: [],
}, {
    Expiration: { Days: 20 },
    ID: '456',
    Prefix: 't',
    Status: 'Enabled',
    Transitions: [],
    NoncurrentVersionTransitions: [{
        NoncurrentDays: 2,
        StorageClass: 'aws-location',
    }, {
        NoncurrentDays: 10,
        StorageClass: 'aws-location2',
    }],
}, {
    Expiration: { Days: 10 },
    ID: '456',
    Prefix: 't',
    Status: 'Enabled',
    Transitions: [{
        Date: '2022-01-01T00:00:00.019Z',
        StorageClass: 'aws-location',
    }, {
        Date: '2024-01-01T00:00:00.019Z',
        StorageClass: 'aws-location2',
    }],
    NoncurrentVersionTransitions: [],
    NoncurrentVersionExpiration: {
        NoncurrentDays: 12,
    }
}];


const oneDay = 24 * 60 * 60 * 1000;

// moves lifecycle transition deadlines 1 day earlier, only for testing
const transitionOneDayEarlier = process.env.TRANSITION_ONE_DAY_EARLIER === 'true';
// moves lifecycle expiration deadlines 1 day earlier, only for testing
const expireOneDayEarlier = process.env.EXPIRE_ONE_DAY_EARLIER === 'true';


// const bucketLCRules = [{
//     // Expiration: { Days: 5 },
//     ID: '456',
//     Prefix: 'tata3',
//     Status: 'Enabled',
//     Expiration: {
//         Days: 100,
//     },
//     NoncurrentVersionTransitions: [],
// }, {
//     // Expiration: { Days: 5 },
//     ID: '456',
//     Prefix: 'hello3',
//     Status: 'Enabled',
//     Expiration: {
//         Date: '2021-01-01T00:00:00.019Z',
//     },
//     NoncurrentVersionTransitions: [],
// }, {
//     // Expiration: { Days: 5 },
//     ID: '456',
//     Prefix: 'toto3',
//     Status: 'Enabled',
//     Expiration: {
//         //  Date: '2021-01-01T00:00:00.019Z',
//         Days: 100,
//     },
//     NoncurrentVersionTransitions: [],
// }, {
//     // Expiration: { Days: 5 },
//     ID: '456',
//     Prefix: 'toto',
//     Status: 'Enabled',
//     Expiration: {
//         //  Date: '2021-01-01T00:00:00.019Z',
//         ExpiredObjectDeleteMarker: true,
//     },
//     NoncurrentVersionTransitions: [],
// }];

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

function aggregateByPrefix(rules, prefix, days) {
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
        finalDays,
    });

    return aggregatedRules;
}

function reduceTransitionAndExpirationRule(currentDate, r, currents) {
    // handle the case when rule is disabled.
    if (r.Status !== 'Enabled') {
        return currents;
    }
    const prefix = r.Prefix;
    const isTransitions = r.Transitions && r.Transitions[0];
    let days;

    // TODO: handle the case when only one transition (no expiration) rule for a given prefix -> 
    //       introcude DataStoreName to the listing params.

    if (r.Expiration) {
        // Expiration Days cannot be 0
        if (r.Expiration.Days) {
            days = r.Expiration.Days;
        } else if (r.Expiration.Date) {
            if (new Date(r.Expiration.Date) <= currentDate) {
                days = 0;
            }
        }
    }

    if (isTransitions) {
        // Transitions Days cannot be 0
        // Cannot mixed 'Date' and 'Days' based Transition actions
        if (r.Transitions[0].Days !== undefined) {
            const lowestTransitionDays = r.Transitions.map(t => t.Days).reduce(lowest);
            days = days === undefined ? lowestTransitionDays : Math.min(days, lowestTransitionDays);

        } else if (r.Transitions[0].Date) {
            const lowestDate = r.Transitions.map(t => new Date(t.Date)).reduce(lowest);

            if (lowestDate <= currentDate) {
                days = 0;
            }
        }
    }

    return aggregateByPrefix(currents, prefix, days);
}

function reduceNonCurrentVersionRule(r, nonCurrents) {
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

    return aggregateByPrefix(nonCurrents, prefix, days);
}

function reduceOrphanDeleteMarkerRule(currentDate, r, orphans) {
    const prefix = r.Prefix;
    let days;

    // When you specify the Days tag, Amazon S3 automatically performs ExpiredObjectDeleteMarker
    // cleanup when the delete markers are old enough to satisfy the age criteria.
    if (r.Expiration) {
        if (r.Expiration.Days) {
            days = r.Expiration.Days;
        } else if (r.Expiration.Date) {
            if (new Date(r.Expiration.Date) <= currentDate) {
                days = 0;
            }
        } else if (r.Expiration.ExpiredObjectDeleteMarker) {
            days = 0;
        }
    }

    return aggregateByPrefix(orphans, prefix, days);
}

function _reduceRulesForNonVersionedBucket(currentDate, bucketLCRules) {
    // TODO: check status
    const reducedRules = sortByPrefix(bucketLCRules).reduce((accumulator, r) => {
        const currents = accumulator.currents;
        const reducedCurrents = reduceTransitionAndExpirationRule(currentDate, r, currents);
        return { currents: reducedCurrents };
    }, { currents: [] });

    return reducedRules;
}

function _reduceRulesForVersionedBucket(currentDate, bucketLCRules) {
    // TODO: check status

    const reducedRules = sortByPrefix(bucketLCRules).reduce((accumulator, r) => {
        const nonCurrents = accumulator.nonCurrents;
        const currents = accumulator.currents;
        const orphans = accumulator.orphans;

        const reducedCurrents = reduceTransitionAndExpirationRule(currentDate, r, currents);
        const reducedNonCurrents = reduceNonCurrentVersionRule(r, nonCurrents);
        const reducedOrphans = reduceOrphanDeleteMarkerRule(currentDate, r, orphans);

        return { currents: reducedCurrents, nonCurrents: reducedNonCurrents, orphans: reducedOrphans };
    }, { currents: [], nonCurrents: [], orphans: [] });

    return reducedRules;
}

function _reduceRules(versioningStatus, currentDate, bucketLCRules) {
    if (versioningStatus === 'Enabled' || versioningStatus === 'Suspended') {
        return _reduceRulesForVersionedBucket(currentDate, bucketLCRules);
    }
    return _reduceRulesForNonVersionedBucket(currentDate, bucketLCRules);
}

function _getBeforeDate(currentDate, days) {
    return new Date(currentDate - (days * oneDay)).toISOString();
}

function _makeListParams(listType, currentDate, rule) {
    const p = {
        prefix: rule.prefix,
        listType,
    };
    if (rule.days > 0) {
        p.beforeDate = _getBeforeDate(currentDate, rule.days);
    }

    return p;
}

function _rulesToParams(currentDate, r) {
    const listings = [];

    (r.currents || []).forEach(rule => {
        const p = _makeListParams(CURRENT_TYPE, currentDate, rule);
        listings.push(p);
    });

    (r.nonCurrents || []).forEach(rule => {
        const p = _makeListParams(NON_CURRENT_TYPE, currentDate, rule);
        listings.push(p);
    });

    (r.orphans || []).forEach(rule => {
        const p = _makeListParams(ORPHAN_TYPE, currentDate, rule);
        listings.push(p);
    });

    return listings;
}

function rulesToParams(versioningStatus, currentDate, bucketLCRules, bucketData) {
    // TODO: check bucketData.details.listType is valid
    if (bucketData.details.listType) {
        const prefix = bucketData.details.prefix;
        const beforeDate = bucketData.details.beforeDate;
        const params = {
            Bucket: bucketData.target.bucket,
            Prefix: prefix,
            MaxKeys: MAX_KEYS,
            BeforeDate: beforeDate
        };

        if (bucketData.details.listType === CURRENT_TYPE) {
            params.Marker = bucketData.details.marker;
        }

        if (bucketData.details.listType === ORPHAN_TYPE) {
            params.Marker = bucketData.details.marker;
        }

        if (bucketData.details.listType === NON_CURRENT_TYPE) {
            params.KeyMarker = bucketData.details.keyMarker;
            params.VersionIdMarker = bucketData.details.versionIdMarker;
        }

        const listingDetails = {
            listType: bucketData.details.listType,
            prefix,
            beforeDate,
        };

        return { params, listingDetails, remainings: [] };
    }

    // TODO: Separate rulesToParams (in rule.js) and implement RulesReducer.js class

    const rules = _reduceRules(versioningStatus, currentDate, bucketLCRules);
    const listingsParams = _rulesToParams(currentDate, rules);
    const { prefix, listType, beforeDate } = listingsParams[0];
    const params = {
        Bucket: bucketData.target.bucket,
        Prefix: prefix,
        MaxKeys: MAX_KEYS,
        BeforeDate: beforeDate,
    };
    const listingDetails = {
        listType,
        prefix,
        beforeDate,
    };

    listingsParams.shift(); // remove the first element

    return { params, listingDetails, remainings: listingsParams };
}

// const final = reduceRulesForListMasters(Date.now(), bucketLCRules);
// const final = reduceRulesForVersionedBucket(Date.now(), bucketLCRules);
// console.log('final!!!', final);

module.exports = {
    rulesToParams,
};
