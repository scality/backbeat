const assert = require('assert');

const {
    isCRRLocation,
    filterOutCRRLocations,
    getCRRLocationNames,
} = require('../../../../lib/util/locations');

const crrPart = {
    key: 'crrKey',
    size: 10,
    start: 0,
    dataStoreName: 'location-crr-source',
    dataStoreType: 'scality',
};
const localPart = {
    key: 'localKey',
    size: 10,
    start: 0,
    dataStoreName: 'us-east-1',
    dataStoreType: 'file',
};

describe('locations util', () => {
    describe('isCRRLocation', () => {
        it('should return true for a location flagged isCRR', () => {
            assert.strictEqual(isCRRLocation('location-crr-source'), true);
        });

        it('should return false for a regular location', () => {
            assert.strictEqual(isCRRLocation('us-east-1'), false);
        });

        it('should return false for an unknown or missing location', () => {
            assert.strictEqual(isCRRLocation('does-not-exist'), false);
            assert.strictEqual(isCRRLocation(undefined), false);
        });
    });

    describe('filterOutCRRLocations', () => {
        it('should drop the parts living on a CRR location', () => {
            assert.deepStrictEqual(
                filterOutCRRLocations([localPart, crrPart]), [localPart]);
        });

        it('should keep all parts when none is on a CRR location', () => {
            assert.deepStrictEqual(
                filterOutCRRLocations([localPart]), [localPart]);
        });

        it('should return an empty array when locations is not an array', () => {
            assert.deepStrictEqual(filterOutCRRLocations(undefined), []);
        });
    });

    describe('getCRRLocationNames', () => {
        it('should list the distinct CRR location names', () => {
            assert.deepStrictEqual(
                getCRRLocationNames([localPart, crrPart, crrPart]),
                ['location-crr-source']);
        });

        it('should return an empty array when there is no CRR location', () => {
            assert.deepStrictEqual(getCRRLocationNames([localPart]), []);
        });
    });
});
