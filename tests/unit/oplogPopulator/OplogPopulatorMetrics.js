const assert = require('assert');
const werelogs = require('werelogs');

const OplogPopulatorMetrics = require('../../../extensions/oplogPopulator/OplogPopulatorMetrics');

describe('OplogPopulatorMetrics', () => {
    const metrics = new OplogPopulatorMetrics(new werelogs.Logger('OplogPopulatorMetrics'));
    metrics.registerMetrics();

    it('should expose migration counters before any migration', async () => {
        const { values } = await metrics.migrations.get();
        assert.deepStrictEqual(values.map(({ labels, value }) => [String(labels.success), value]).sort(),
            [['false', 0], ['true', 0]]);
    });

    it('should record the reconfigurations of all hashed connectors from 0', async () => {
        const hashedValues = async metric => (await metric.get()).values
            .filter(({ labels }) => labels.connector === 'hashed')
            .map(({ labels, value }) => [labels.success === undefined ? '' : String(labels.success), value]).sort();
        metrics.onHashedConnectorsStarted();
        assert.deepStrictEqual(await hashedValues(metrics.connectorConfigurationApplied), [['false', 0], ['true', 0]]);
        assert.deepStrictEqual(await hashedValues(metrics.requestSize), [['', 0]]);
        const config = { pipeline: '[]' };
        metrics.onHashedConnectorReconfiguration(true, 0.5, config);
        metrics.onHashedConnectorReconfiguration(false, null);
        assert.deepStrictEqual(await hashedValues(metrics.connectorConfigurationApplied), [['false', 1], ['true', 1]]);
        assert.deepStrictEqual(await hashedValues(metrics.requestSize), [['', JSON.stringify(config).length]]);
        const lag = (await metrics.reconfigurationLag.get()).values
            .find(({ labels, metricName }) => labels.connector === 'hashed' && metricName.endsWith('_count'));
        assert.strictEqual(lag.value, 1);
    });

    it('should count hashed and outdated connectors by state', async () => {
        metrics.onHashedConnectorsObserved({ running: 2, paused: 1, failed: 0, outdated: 3 });
        const { values } = await metrics.connectors.get();
        assert.deepStrictEqual(values.map(({ labels, value }) => [labels.state, value]).sort(),
            [['failed', 0], ['outdated', 3], ['paused', 1], ['running', 2]]);
    });

    it('should record the pipeline size of hashed connectors', async () => {
        metrics.onHashedConnectorPipeline('c2', '[{}]');
        const size = (await metrics.mongoPipelineSize.get()).values.find(({ labels }) => labels.connector === 'c2');
        assert.strictEqual(size.value, JSON.stringify('[{}]').length);
    });
});
