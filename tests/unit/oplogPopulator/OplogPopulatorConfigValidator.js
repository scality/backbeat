const assert = require('assert');
const { OplogPopulatorConfigJoiSchema} = require('../../../extensions/oplogPopulator/OplogPopulatorConfigValidator');

const defaultConfig = {
    topic: 'backbeat-oplog',
    kafkaConnectHost: '127.0.0.1',
    kafkaConnectPort: 8083,
    numberOfConnectors: 1,
    probeServer: {
        bindAddress: '0.0.0.0',
        port: 8556,
    },
};

describe('OplogPopulatorConfigValidator', () => {
    describe('locationStrippingBytesThreshold validation', () => {
        it('should accept valid threshold', () => {
            const config = {
                ...defaultConfig,
                locationStrippingBytesThreshold: 100 * 1000000,
            };
            const result = OplogPopulatorConfigJoiSchema.validate(config);
            assert.ifError(result.error);
            assert.strictEqual(result.value.locationStrippingBytesThreshold, 100 * 1000000);
        });
    });
});
