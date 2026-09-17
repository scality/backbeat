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
    return Boolean(locationsConfig[dataStoreName]?.isCRR);
}

module.exports = {
    isCRRLocation,
};
