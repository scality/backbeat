const assert = require('assert');
const sinon = require('sinon');
const werelogs = require('werelogs');

const Connector =
    require('../../../../extensions/oplogPopulator/modules/Connector');
const LeastFullConnector =
    require('../../../../extensions/oplogPopulator/allocationStrategy/LeastFullConnector');
const constants = require('../../../../extensions/oplogPopulator/constants');

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

describe('LeastFullConnector', () => {
    let strategy;
    beforeEach(() => {
        strategy = new LeastFullConnector({
            logger,
        });
    });

    describe('getConnector', () => {
        afterEach(() => {
            sinon.restore();
        });

        it('should return connector with fewest buckets', () => {
            const connector = strategy.getConnector([connector1, connector2]);
            assert.strictEqual(connector.name, connector1.name);
        });

        it('should return null if no connectors', () => {
            const connector = strategy.getConnector([]);
            assert.strictEqual(connector, null);
        });

        it('should return null if the smallest connector is full', () => {
            sinon.stub(strategy, 'maximumBucketsPerConnector').value(1);
            const connector = strategy.getConnector([connector2]);
            assert.strictEqual(connector, null);
        });
    });

    describe('canUpdate', () => {
        it('should return true', () => {
            assert.strictEqual(strategy.canUpdate(), true);
        });
    });

    describe('maximumBucketsPerConnector', () => {
        it('should return the maximum number of buckets per connector', () => {
            assert.strictEqual(strategy.maximumBucketsPerConnector, constants.maxBucketsPerConnector);
        });
    });
});
