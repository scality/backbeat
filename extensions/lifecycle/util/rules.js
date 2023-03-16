const { RulesReducer } = require('./RulesReducer');
const { lifecycleListing: { NON_CURRENT_TYPE, CURRENT_TYPE, ORPHAN_DM_TYPE } } = require('../../../lib/constants');

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
const MAX_KEYS = process.env.CI === 'true' ? 3 : 1000;

// const bucketLCRules = [{
//     Expiration: { Days: 5 },
//     ID: '456',
//     Prefix: 'toto',
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


const oneDay = 24 * 60 * 60 * 1000;

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

// const lowest = (acc, cur) => (acc < cur ? acc : cur);

// function sortByPrefix(rules) {
//     return rules.sort((a, b) => {
//         if (a.Prefix > b.Prefix) {
//             return -1;
//         }
//         if (b.Prefix > a.Prefix) {
//             return 1;
//         }

//         return 0;
//     });
// }

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

function _mergeParams(currentDate, r) {
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
        const p = _makeListParams(ORPHAN_DM_TYPE, currentDate, rule);
        listings.push(p);
    });

    return listings;
}

function _getParamsFromRules(bucketName, currentDate, r) {
    const listingsParams = _mergeParams(currentDate, r);
    let params;
    let listingDetails;

    if (listingsParams.length > 0) {
        const { prefix, listType, beforeDate } = listingsParams[0];
        params = {
            Bucket: bucketName,
            Prefix: prefix,
            MaxKeys: MAX_KEYS,
        };

        listingDetails = {
            listType,
            prefix,
        };

        if (beforeDate) {
            params.BeforeDate = beforeDate;
            listingDetails.beforeDate = beforeDate;
        }

        listingsParams.shift(); // remove the first element
    }

    return { params, listingDetails, remainings: listingsParams };
}

function _getParamsFromDetails(bucketName, details) {
    const { prefix, beforeDate, listType } = details;
        const params = {
            Bucket: bucketName,
            Prefix: prefix,
            MaxKeys: MAX_KEYS,
        };

        if (listType === CURRENT_TYPE) {
            params.Marker = details.marker;
        }

        if (listType === ORPHAN_DM_TYPE) {
            params.Marker = details.marker;
        }

        if (listType === NON_CURRENT_TYPE) {
            params.KeyMarker = details.keyMarker;
            params.VersionIdMarker = details.versionIdMarker;
        }

        const listingDetails = {
            listType,
            prefix,
        };

        if (beforeDate) {
            params.BeforeDate = beforeDate;
            listingDetails.beforeDate = beforeDate;
        }

        return { params, listingDetails, remainings: [] };
}

function rulesToParams(versioningStatus, currentDate, bucketLCRules, bucketData) {
    // TODO: check bucketData.details.listType is valid
    const bucketName = bucketData.target.bucket;
    console.log('>>>>> bucketData.details!!!!', bucketData.details);
    if (bucketData.details && bucketData.details.listType) {
        return _getParamsFromDetails(bucketName, bucketData.details);
    }

    const rulesReducer = new RulesReducer(versioningStatus, currentDate, bucketLCRules)
    const rules = rulesReducer.reduce();

    return _getParamsFromRules(bucketName, currentDate, rules);
}

module.exports = {
    rulesToParams,
};
