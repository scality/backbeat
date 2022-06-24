const assert = require('assert');
const werelogs = require('werelogs');

const Connector =
    require('../../../../extensions/oplogPopulator/modules/Connector');
const LeastFullConnector =
    require('../../../../extensions/oplogPopulator/allocationStrategy/LeastFullConnector');

const logger = new werelogs.Logger('LeastFullConnector');

const defaultConnectorParams = {
    config: {},
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

describe('LeastFullConnector', () => {
    let strategy;
    beforeEach(() => {
        strategy = new LeastFullConnector({
            logger,
        });
    });

    describe('getConnector', () => {
        it('Should return connector with fewest buckets', () => {
            const connector = strategy.getConnector([connector1, connector2]);
            assert.strictEqual(connector.name, connector1.name);
        });
    });
});
