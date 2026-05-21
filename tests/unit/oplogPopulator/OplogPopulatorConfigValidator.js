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

    describe('transformObjectKey validation', () => {
        it('should default to false when not provided', () => {
            const result = OplogPopulatorConfigJoiSchema.validate(defaultConfig);
            assert.ifError(result.error);
            assert.strictEqual(result.value.transformObjectKey, false);
        });

        it('should accept an explicit boolean', () => {
            const result = OplogPopulatorConfigJoiSchema.validate({
                ...defaultConfig,
                transformObjectKey: true,
            });
            assert.ifError(result.error);
            assert.strictEqual(result.value.transformObjectKey, true);
        });

        it('should reject a non-boolean', () => {
            const result = OplogPopulatorConfigJoiSchema.validate({
                ...defaultConfig,
                transformObjectKey: 'yes',
            });
            assert(result.error);
        });
    });
});
