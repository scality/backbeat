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

    describe('ingestion validation', () => {
        it('should default to hashed ingestion', () => {
            const result = OplogPopulatorConfigJoiSchema.validate(defaultConfig);
            assert.ifError(result.error);
            assert.strictEqual(result.value.ingestion, 'hashed');
        });

        it('should reject unknown ingestion modes', () => {
            const result = OplogPopulatorConfigJoiSchema.validate({ ...defaultConfig, ingestion: 'other' });
            assert(result.error);
        });

        it('should use a single connector in hashed ingestion when the count is 0 or unset', () => {
            const zero = OplogPopulatorConfigJoiSchema.validate({ ...defaultConfig, numberOfConnectors: 0 });
            assert.ifError(zero.error);
            assert.strictEqual(zero.value.numberOfConnectors, 1);
            const unset = { ...defaultConfig };
            delete unset.numberOfConnectors;
            const result = OplogPopulatorConfigJoiSchema.validate(unset);
            assert.ifError(result.error);
            assert.strictEqual(result.value.numberOfConnectors, 1);
        });

        it('should reconcile hashed connectors every 15 seconds by default', () => {
            const result = OplogPopulatorConfigJoiSchema.validate(defaultConfig);
            assert.ifError(result.error);
            assert.strictEqual(result.value.connectorsUpdateCronRule, '*/15 * * * * *');
        });

        it('should keep updating per-bucket connectors every second by default', () => {
            const result = OplogPopulatorConfigJoiSchema.validate({ ...defaultConfig, ingestion: 'perBucket' });
            assert.ifError(result.error);
            assert.strictEqual(result.value.connectorsUpdateCronRule, '*/1 * * * * *');
        });

        it('should keep an explicit update cron rule', () => {
            const result = OplogPopulatorConfigJoiSchema.validate({
                ...defaultConfig,
                connectorsUpdateCronRule: '*/5 * * * * *',
            });
            assert.ifError(result.error);
            assert.strictEqual(result.value.connectorsUpdateCronRule, '*/5 * * * * *');
        });

        it('should accept no connector count in per-bucket ingestion', () => {
            const result = OplogPopulatorConfigJoiSchema.validate({
                ...defaultConfig,
                ingestion: 'perBucket',
                numberOfConnectors: 0,
            });
            assert.ifError(result.error);
        });
    });
});
