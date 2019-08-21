const config = require('../../lib/Config');

/**
 * Mock the setting of ingestion buckets. We use the existing bootstrap list
 * from test config.json but change the first location to be a compatible type.
 * Also, just to indicate a change, the site names are appended with
 * "-ingestion". This is to keep 2 sites to stay compatible with existing tests.
 * @return {undefined}
 */
function setupIngestionSiteMock() {
    const bootstrapList = config.getBootstrapList();
    /* eslint-disable no-param-reassign */
    const ingestionLocations = bootstrapList.reduce((obj, loc) => {
        obj[`${loc.site}-ingestion`] = {
            details: {},
            locationType: 'location-scality-ring-s3-v1',
        };
        return obj;
    }, {});
    /* eslint-enable no-param-reassign */
    const ingestionBuckets = [];
    Object.keys(ingestionLocations).forEach(locationName => {
        ingestionBuckets.push({
            name: `bucket-${locationName}`,
            ingestion: { status: 'enabled' },
            locationConstraint: locationName,
        });
    });
    config.setIngestionBuckets(ingestionLocations, ingestionBuckets);
}

module.exports = setupIngestionSiteMock;
