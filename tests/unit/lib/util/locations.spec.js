const assert = require('assert');

const { isCRRLocation } = require('../../../../lib/util/locations');

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
});
