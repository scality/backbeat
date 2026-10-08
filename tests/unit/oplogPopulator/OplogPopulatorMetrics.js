const assert = require('assert');
const sinon = require('sinon');
const werelogs = require('werelogs');

const OplogPopulatorMetrics = require('../../../extensions/oplogPopulator/OplogPopulatorMetrics');

describe('OplogPopulatorMetrics', () => {
    const logger = new werelogs.Logger('OplogPopulatorMetrics');
    const metrics = new OplogPopulatorMetrics(logger);
    metrics.registerMetrics();

    // the registry is shared: each test uses its own connector names
    const valueOf = async (metric, connector, filter = () => true) => (await metric.get()).values
        .find(({ labels, metricName }) => labels.connector === connector && filter({ labels, metricName }))?.value;

    afterEach(() => {
        sinon.restore();
    });

    it('should count per-bucket connectors as running', async () => {
        const running = async () => (await metrics.connectors.get()).values
            .find(({ labels }) => labels.state === 'running')?.value || 0;
        const before = await running();
        metrics.onConnectorsInstantiated(true, 3);
        metrics.onConnectorDestroyed();
        assert.strictEqual(await running(), before + 2);
        assert((await metrics.connectors.get()).values.every(({ labels }) => labels.state !== undefined));
    });

    it('should count connectors by state', async () => {
        [['running', 2], ['paused', 1], ['failed', 0], ['outdated', 3]]
            .forEach(([state, count]) => metrics.onConnectorsCounted(state, count));
        const { values } = await metrics.connectors.get();
        assert.deepStrictEqual(values.map(({ labels, value }) => [labels.state, value]).sort(),
            [['failed', 0], ['outdated', 3], ['paused', 1], ['running', 2]]);
    });

    it('should refuse an unknown connector state', async () => {
        const error = sinon.spy(logger, 'error');
        metrics.onConnectorsCounted('stopped', 1);
        assert(error.calledOnce);
        const { values } = await metrics.connectors.get();
        assert(values.every(({ labels }) => labels.state !== 'stopped'));
    });

    it('should report the expected connectors and their pipeline size when started', async () => {
        const specs = [
            { name: 'started-0', config: { pipeline: '[]' } },
            { name: 'started-1', config: { pipeline: '[{}]' } },
        ];
        metrics.onHashedConnectorsStarted(specs);
        assert.strictEqual((await metrics.expectedConnectors.get()).values[0].value, 2);
        assert.strictEqual(await valueOf(metrics.mongoPipelineSize, 'started-0'), JSON.stringify('[]').length);
        assert.strictEqual(await valueOf(metrics.mongoPipelineSize, 'started-1'), JSON.stringify('[{}]').length);
        assert.strictEqual(await valueOf(metrics.connectorConfigurationApplied, 'started-0'), undefined);
    });

    it('should record the request size of a reconfiguration', async () => {
        metrics.onConnectorReconfiguration({ name: 'reconfigured-0' }, true, 0.5, 42);
        metrics.onConnectorReconfiguration({ name: 'reconfigured-1' }, false, 0.5, 10);
        assert.strictEqual(await valueOf(metrics.requestSize, 'reconfigured-0'), 42);
        assert.strictEqual(await valueOf(metrics.requestSize, 'reconfigured-1'), 10);
        assert.strictEqual(await valueOf(metrics.connectorConfigurationApplied, 'reconfigured-0',
            ({ labels }) => labels.success === true), 1);
        assert.strictEqual(await valueOf(metrics.connectorConfigurationApplied, 'reconfigured-1',
            ({ labels }) => labels.success === false), 1);
        assert.strictEqual(await valueOf(metrics.reconfigurationLag, 'reconfigured-0',
            ({ metricName }) => metricName.endsWith('_count')), 1);
        assert.strictEqual(await valueOf(metrics.buckets, 'reconfigured-0'), undefined);
    });

    it('should record the buckets of a per-bucket connector on success only', async () => {
        metrics.onConnectorReconfiguration({ name: 'per-bucket-0', bucketCount: 3 }, true, 0.5);
        metrics.onConnectorReconfiguration({ name: 'per-bucket-1', bucketCount: 3 }, false);
        assert.strictEqual(await valueOf(metrics.buckets, 'per-bucket-0'), 3);
        assert.strictEqual(await valueOf(metrics.buckets, 'per-bucket-1'), undefined);
        assert.strictEqual(await valueOf(metrics.requestSize, 'per-bucket-0'), undefined);
        assert.strictEqual(await valueOf(metrics.requestSize, 'per-bucket-1'), undefined);
    });
});
