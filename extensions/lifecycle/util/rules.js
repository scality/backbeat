const { RulesReducer } = require('./RulesReducer');
const { lifecycleListing: { NON_CURRENT_TYPE, CURRENT_TYPE, ORPHAN_DM_TYPE } } = require('../../../lib/constants');

// Default max AWS limit is 1000 for both list objects and list object versions
// TODO: MAX_KEYS TO MOVE BACK TO 1000
const MAX_KEYS = process.env.CI === 'true' ? 3 : 1000;

function _getBeforeDate(currentDate, days) {
    const beforeDate = new Date(currentDate);
    beforeDate.setDate(beforeDate.getDate() - days);
    return beforeDate.toISOString();
}

function _makeListParams(listType, currentDate, listing) {
    const p = {
        prefix: listing.prefix,
        listType,
    };
    if (listing.days > 0) {
        p.beforeDate = _getBeforeDate(currentDate, listing.days);
    }

    return p;
}

/**
 * _mergeParams: flatten the listings array
 * @param {Date} currentDate           - current date
 * @param {Object} listings            - rules grouped by list type (current, noncurrent, orphan)
 * @param {array} listings.currents    - array of listing: { prefix, days }
 * @param {array} listings.nonCurrents - array of listing: { prefix, days }
 * @param {array} listings.orphans     - array of listing: { prefix, days }
 * @return {array} mergedListings      - listing informations, array of { prefix, listType, beforeDate }
 */
function _mergeParams(currentDate, listings) {
    const mergedListings = [];

    (listings.currents || []).forEach(l => {
        const p = _makeListParams(CURRENT_TYPE, currentDate, l);
        mergedListings.push(p);
    });

    (listings.nonCurrents || []).forEach(l => {
        const p = _makeListParams(NON_CURRENT_TYPE, currentDate, l);
        mergedListings.push(p);
    });

    (listings.orphans || []).forEach(l => {
        const p = _makeListParams(ORPHAN_DM_TYPE, currentDate, l);
        mergedListings.push(p);
    });

    return mergedListings;
}

/**
 * _getParamsFromListings: retrieve the lifecycle listing informations from listings
 * @param {string} bucketName          - name of the bucket
 * @param {Date} currentDate           - current date
 * @param {Object} listings            - rules grouped by list type
 * @param {array} listings.currents    - array of listing { prefix, days }
 * @param {array} listings.nonCurrents - array of listing { prefix, days }
 * @param {array} listings.orphans     - array of listing { prefix, days }
 * @return {Object} info               - listing informations { params, listType, remainings }
 */
function _getParamsFromListings(bucketName, currentDate, listings) {
    const listingsParams = _mergeParams(currentDate, listings);
    let params;
    let listType;

    if (listingsParams.length > 0) {
        const firstParams = listingsParams[0];
        const { prefix, beforeDate } = firstParams;
        listType = firstParams.listType;
        params = {
            Bucket: bucketName,
            Prefix: prefix,
            MaxKeys: MAX_KEYS,
        };

        if (beforeDate) {
            params.BeforeDate = beforeDate;
        }

        listingsParams.shift(); // remove the first element
    }

    return { params, listType, remainings: listingsParams };
}

/**
 * _getParamsFromDetails: retrieve the lifecycle listing parameters from details
 * @param {string} bucketName - name of the bucket
 * @param {Object} details - listing details
 * @param {string} details.prefix - prefix
 * @param {string} details.beforeDate - before date
 * @param {string} details.keyMarker - next key marker for versioned buckets
 * @param {string} details.versionIdMarker - next version id marker for versioned buckets
 * @param {string} details.marker - next marker for non-versioned buckets
 * @param {string} details.objectName - used specifically for handling versioned buckets
 * @return {Object} info - listing informations { params, listType, remainings }
 */
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

    if (beforeDate) {
        params.BeforeDate = beforeDate;
    }

    return { params, listType, remainings: [] };
}

/**
 * rulesToParams: retrieve the lifecycle listing parameters from the lifecycle rules
 * @param {string} versioningStatus - bucket's version status
 * @param {Date} currentDate - current date
 * @param {Object} bucketLCRules - lifecycle rules
 * @param {object} bucketData - bucket data from Kafka bucketTasks topic
 * @param {object} bucketData.target - target bucket info
 * @param {string} bucketData.target.bucket - bucket name
 * @param {string} bucketData.target.owner - owner id
 * @param {string} [bucketData.details.prefix] - prefix
 * @param {string} [bucketData.details.beforeDate] - before date
 * @param {string} [bucketData.details.keyMarker] - next key marker for versioned buckets
 * @param {string} [bucketData.details.versionIdMarker] - next version id marker for versioned buckets
 * @param {string} [bucketData.details.marker] - next marker for non-versioned buckets
 * @param {string} [bucketData.details.objectName] - used specifically for handling versioned buckets
 * @param {Object} options - lifecycle options
 * @param {boolean} options.expireOneDayEarlier - moves lifecycle expiration deadlines 1 day earlier
 * @param {boolean} options.transitionOneDayEarlier - moves lifecycle transition deadlines 1 day earlier
 *
 * @return {Object} info - listings informations { params, listingDetails, remainings }
 * @return {Object} info.params - params of the first lifecycle listing
 * @return {string} info.params.Bucket - bucket name
 * @return {string} info.params.Prefix - limits the response to keys that begin with the specified prefix
 * @return {string} info.params.MaxKeys - maximum number of keys returned in the response
 * @return {string} info.params.Marker - for non-versioned buckets, where to start listing from
 * @return {string} info.params.KeyMarker - for versioned buckets, where to start listing from
 * @return {string} info.params.Marker - for versioned buckets, where to start listing from
 * @return {string} [info.params.BeforeDate] - limit keys with last-modified older than beforeDate
 * @return {string} info.listType -  type of listing (current, noncurrent or orphan)
 * @return {Array} info.remainings - array of remaining listings
 */
function rulesToParams(versioningStatus, currentDate, bucketLCRules, bucketData, options) {
    // TODO: check bucketData.details.listType is valid
    const bucketName = bucketData.target.bucket;
    if (bucketData.details && bucketData.details.listType) {
        return _getParamsFromDetails(bucketName, bucketData.details);
    }

    const rulesReducer = new RulesReducer(versioningStatus, currentDate, bucketLCRules, options);
    const listings = rulesReducer.toListings();

    return _getParamsFromListings(bucketName, currentDate, listings);
}

module.exports = {
    rulesToParams,
};
