const assert = require('assert');
const werelogs = require('werelogs');

const Connector =
    require('../../../../extensions/oplogPopulator/modules/Connector');
const ImmutableConnector =
    require('../../../../extensions/oplogPopulator/allocationStrategy/ImmutableConnector');

const logger = new werelogs.Logger('LeastFullConnector');

const defaultConnectorParams = {
    config: {},
    isRunning: true,
    logger,
    kafkaConnectHost: '127.0.0.1',
    kafkaConnectPort: 8083,
};

const connector1 = new Connector({
    name: 'example-connector-1',
    buckets: [],
    ...defaultConnectorParams,
});

const connector2 = new Connector({
    name: 'example-connector-2',
    buckets: ['bucket1', 'bucket2'],
    ...defaultConnectorParams,
});

const connector3 = new Connector({
    name: 'example-connector-3',
    buckets: ['bucket3'],
    ...defaultConnectorParams,
});

describe('ImmutableConnector', () => {
    it('should return null for getConnector', async () => {
        const immutableConnector = new ImmutableConnector({ logger });
        const result = await immutableConnector.getConnector(
            [connector1, connector2, connector3], 'bucket1');
        assert.strictEqual(result, null);
    });

    it('should return false for canUpdate', async () => {
        const immutableConnector = new ImmutableConnector({ logger });
        const result = await immutableConnector.canUpdate();
        assert.strictEqual(result, false);
    });

    it('should return 1 for maximumBucketsPerConnector', async () => {
        const immutableConnector = new ImmutableConnector({ logger });
        const result = await immutableConnector.maximumBucketsPerConnector;
        assert.strictEqual(result, 1);
    });
});
