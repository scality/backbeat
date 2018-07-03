const assert = require('assert');
const getLocationsFromStorageClass =
require('../../../extensions/replication/utils/getLocationsFromStorageClass');

describe('getLocationsFromStorageClass', () => {
    it('should return correct locations if preferred read location is not ' +
    'specified inside the location storage class',
    () => {
        const replicationStorageClass = 'awslocation,gcplocation';
        const locations = getLocationsFromStorageClass(replicationStorageClass);
        const expectedLocations = ['awslocation', 'gcplocation'];
        assert.deepStrictEqual(locations, expectedLocations);
    });
    it('should return correct locations if preferred read location is ' +
    'specified inside the location storage class',
    () => {
        const replicationStorageClass =
          'awslocation,gcplocation:preferred_read';
        const locations = getLocationsFromStorageClass(replicationStorageClass);
        const expectedLocations = ['awslocation', 'gcplocation'];
        assert.deepStrictEqual(locations, expectedLocations);
    });
});
