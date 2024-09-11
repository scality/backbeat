const assert = require('assert');
const werelogs = require('werelogs');

const Connector =
    require('../../../../extensions/oplogPopulator/modules/Connector');
const LeastFullConnector =
    require('../../../../extensions/oplogPopulator/allocationStrategy/LeastFullConnector');

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
    name: 'example-connector-2',
    buckets: ['bucket3', 'bucket4'],
    ...defaultConnectorParams,
});

describe('LeastFullConnector (with multiple buckets per connector)', () => {
    let strategy;
    beforeEach(() => {
        strategy = new LeastFullConnector({
            addConnector: () => new Connector({
                name: 'example-connector-3',
                buckets: [],
                ...defaultConnectorParams,
            }),
            logger,
        });
    });

    describe('getConnector', () => {
        it('Should return connector with fewest buckets', () => {
            const connector = strategy.getConnector([connector1, connector2]);
            assert.strictEqual(connector.name, connector1.name);
        });

        it('Should return connector with fewest buckets (single stream case)', () => {
            strategy = new LeastFullConnector({
                maximumBucketsPerConnector: Infinity,
                addConnector: () => new Connector({
                    name: 'example-connector-3',
                    buckets: [],
                    ...defaultConnectorParams,
                }),
                logger,
            });
            const connector = strategy.getConnector([connector1, connector2]);
            assert.strictEqual(connector.name, connector1.name);
        });
    });
});

describe('LeastFullConnector (with one bucket per connector)', () => {
    let strategy;
    beforeEach(() => {
        strategy = new LeastFullConnector({
            maximumBucketsPerConnector: 1,
            addConnector: () => new Connector({
                name: 'example-connector-3',
                buckets: [],
                ...defaultConnectorParams,
            }),
            logger,
        });
    });

    describe('getConnector', () => {
        it('Should return connector with fewest buckets', () => {
            const connector = strategy.getConnector([connector2, connector3]);
            assert.strictEqual(connector.name, 'example-connector-3');
        });
    });
});
