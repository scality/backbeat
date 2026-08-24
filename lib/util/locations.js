const locationsConfig = require('../../conf/locationConfig.json') || {};

/**
 * Tell whether a location holds data owned by a remote site.
 *
 * Data stored on such a location is remote production data: we may read it
 * (e.g. to copy it locally), but we must never delete it.
 *
 * @param {String} dataStoreName - location name
 * @return {Boolean} true if the location is a CRR (remote) location
 */
function isCRRLocation(dataStoreName) {
    return Boolean(dataStoreName && locationsConfig[dataStoreName] &&
                   locationsConfig[dataStoreName].isCRR);
}

/**
 * Remove from a list of location parts those living on a CRR location,
 * i.e. those which must never be garbage-collected.
 *
 * @param {Object[]} locations - array of location parts
 * @return {Object[]} the location parts which are safe to delete
 */
function filterOutCRRLocations(locations) {
    if (!Array.isArray(locations)) {
        return [];
    }
    return locations.filter(location => !isCRRLocation(location && location.dataStoreName));
}

/**
 * List the distinct CRR location names found in a list of location parts,
 * for logging purposes.
 *
 * @param {Object[]} locations - array of location parts
 * @return {String[]} distinct CRR location names
 */
function getCRRLocationNames(locations) {
    if (!Array.isArray(locations)) {
        return [];
    }
    const names = locations
        .map(location => location && location.dataStoreName)
        .filter(dataStoreName => isCRRLocation(dataStoreName));
    return [...new Set(names)];
}

module.exports = {
    isCRRLocation,
    filterOutCRRLocations,
    getCRRLocationNames,
};
