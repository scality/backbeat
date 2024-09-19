const assert = require('assert');
const werelogs = require('werelogs');
const AllocationStrategy = require('../../../../extensions/oplogPopulator/allocationStrategy/AllocationStrategy');

const logger = new werelogs.Logger('LeastFullConnector');

describe('AllocationStrategy', () => {
    it('should throw NotImplemented when calling getConnector', async () => {
        const allocationStrategy = new AllocationStrategy({ logger });
        assert.throws(() => allocationStrategy.getConnector(), {
            name: 'Error',
            type: 'NotImplemented',
        });
    });

    it('should throw NotImplemented when calling canUpdate', async () => {
        const allocationStrategy = new AllocationStrategy({ logger });
        assert.throws(() => allocationStrategy.canUpdate(), {
            name: 'Error',
            type: 'NotImplemented',
        });
    });
});
