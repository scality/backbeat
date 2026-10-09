const assert = require('assert');
const sinon = require('sinon');
const werelogs = require('werelogs');
const { errors } = require('@scality/arsenal');

const HashedConnectorsManager = require('../../../extensions/oplogPopulator/modules/HashedConnectorsManager');
const HashedPipelineFactory = require('../../../extensions/oplogPopulator/pipeline/HashedPipelineFactory');
const OplogPopulatorMetrics = require('../../../extensions/oplogPopulator/OplogPopulatorMetrics');
const { hashedConnectorName, parseHashedConnectorName } =
    require('../../../extensions/oplogPopulator/modules/connectorNaming');
const { formatStartupTime, timestampFromStartupTime } =
    require('../../../extensions/oplogPopulator/modules/resumeToken');
const { resumeTokenAt } = require('../../utils/resumeTokens');

const logger = new werelogs.Logger('HashedConnectorsManager');
const prefix = 'backbeat-oplog-';
const startupTimeKey = 'startup.mode.timestamp.start.at.operation.time';

function httpError(statusCode) {
    const err = errors.InternalError.customizeDescription(`HTTP ${statusCode}`);
    err.statusCode = statusCode;
    return err;
}

/**
 * In-memory Kafka Connect: connectors with a config, a state, and the
 * offsets committed under their current partition name
 */
class FakeKafkaConnect {
    constructor() {
        this.connectors = {};
        this.offsetsSupported = true;
        this.calls = [];
        this.listings = 0;
    }

    add(name, { config = {}, state = 'RUNNING', taskState = null, offsetTime = null } = {}) {
        const partition = config['offset.partition.name'] || `partition-${name}`;
        this.connectors[name] = {
            config: { 'offset.partition.name': partition, ...config },
            state,
            taskState,
            offsets: offsetTime === null ? [] :
                [{ partition: { ns: partition }, offset: { _id: resumeTokenAt(offsetTime) } }],
        };
    }

    _get(name) {
        if (!this.connectors[name]) {
            throw httpError(404);
        }
        return this.connectors[name];
    }

    async getConnectors() {
        return Object.keys(this.connectors);
    }

    async getConnectorsWithStatus() {
        this.listings += 1;
        return Object.fromEntries(Object.entries(this.connectors).map(([name, c]) => [name, {
            info: { name, config: { ...c.config } },
            status: { connector: { state: c.state }, tasks: [{ id: 0, state: c.taskState || c.state }] },
        }]));
    }

    async getConnectorOffsets(name) {
        this.calls.push(['offsets', name]);
        if (!this.offsetsSupported) {
            throw httpError(404);
        }
        return this._get(name).offsets;
    }

    async pauseConnector(name) {
        this.calls.push(['pause', name]);
        this._get(name).state = 'PAUSED';
    }

    async createConnector({ name, config }) {
        this.calls.push(['create', name]);
        if (this.connectors[name]) {
            throw httpError(409);
        }
        this.connectors[name] = { config, state: 'RUNNING', offsets: [] };
    }

    async deleteConnector(name) {
        this.calls.push(['delete', name]);
        this._get(name);
        delete this.connectors[name];
    }

    async restartConnector(name) {
        this.calls.push(['restart', name]);
        this._get(name).state = 'RUNNING';
    }

    async updateConnectorConfig(name, config) {
        this.calls.push(['update', name]);
        this._get(name).config = config;
    }

    callsOf(type) {
        return this.calls.filter(([t]) => t === type).map(([, name]) => name);
    }
}

describe('HashedConnectorsManager', () => {
    const now = { t: 1000, i: 7 };
    let kafkaConnect;
    let clock;
    let metrics;

    function newManager(nbConnectors, locationStrippingBytesThreshold = 0) {
        const manager = new HashedConnectorsManager({
            nbConnectors,
            database: 'metadata',
            mongoUrl: 'mongodb://localhost:27017',
            oplogTopic: 'oplog',
            cronRule: '* * * * * *',
            prefix,
            heartbeatIntervalMs: 10000,
            kafkaConnectHost: 'localhost',
            kafkaConnectPort: 8083,
            pipelineFactory: new HashedPipelineFactory(locationStrippingBytesThreshold),
            metricsHandler: metrics,
            getClusterTime: async () => now,
            logger,
        });
        manager._kafkaConnect = kafkaConnect;
        return manager;
    }

    async function reconcile(manager) {
        const [result] = await Promise.all([manager.reconcile(), clock.runAllAsync()]);
        return result;
    }

    function addConnectors(nbConnectors, opts = {}) {
        newManager(nbConnectors)._desired.forEach(d => kafkaConnect.add(d.name, {
            ...opts,
            config: { ...d.config, ...opts.config },
        }));
    }

    function connectorNames(nbConnectors, locationStrippingBytesThreshold = 0) {
        return newManager(nbConnectors, locationStrippingBytesThreshold)._desired.map(d => d.name);
    }

    function startOf(name) {
        return timestampFromStartupTime(kafkaConnect.connectors[name].config[startupTimeKey]);
    }

    beforeEach(() => {
        kafkaConnect = new FakeKafkaConnect();
        clock = sinon.useFakeTimers();
        metrics = new OplogPopulatorMetrics(logger);
        sinon.stub(metrics);
    });

    afterEach(() => {
        sinon.restore();
    });

    it('should create the connectors at the current cluster time on a fresh install', async () => {
        await reconcile(newManager(2));
        const names = connectorNames(2);
        assert.deepStrictEqual(Object.keys(kafkaConnect.connectors), names);
        names.forEach((name, k) => {
            const { config } = kafkaConnect.connectors[name];
            assert.deepStrictEqual(startOf(name), now);
            assert.match(config['offset.partition.name'], /^partition-/);
            assert.strictEqual(config['connection.uri'], 'mongodb://localhost:27017');
            assert.deepStrictEqual(JSON.parse(config.pipeline)[0].$match.$expr.$in[1], [k, k - 2]);
        });
        assert.strictEqual(Date.now(), 0);
    });

    it('should leave healthy connectors alone', async () => {
        addConnectors(2);
        await reconcile(newManager(2));
        assert.deepStrictEqual(kafkaConnect.calls, []);
    });

    it('should ignore connectors it does not own', async () => {
        kafkaConnect.add('other-connector');
        addConnectors(1);
        await reconcile(newManager(1));
        assert.deepStrictEqual(kafkaConnect.calls, []);
    });

    it('should migrate per-bucket connectors from their earliest offset', async () => {
        const perBucket = [
            `${prefix}source-connector-3f1c7a52-9b1e-4c1d-8f2e-6a7b8c9d0e1f`,
            `${prefix}source-connector-0d6c1a2b-3c4d-4e5f-8a9b-0c1d2e3f4a5b`,
        ];
        kafkaConnect.add(perBucket[0], { offsetTime: 900 });
        kafkaConnect.add(perBucket[1], { offsetTime: 950 });
        await reconcile(newManager(2));
        assert.deepStrictEqual(kafkaConnect.callsOf('pause').sort(), [...perBucket].sort());
        assert.strictEqual(Date.now(), 60000);
        assert.deepStrictEqual(Object.keys(kafkaConnect.connectors), connectorNames(2));
        connectorNames(2).forEach(name => assert.deepStrictEqual(startOf(name), { t: 900, i: 0 }));
        assert(metrics.onMigration.calledOnceWith(true, 100));
    });

    it('should create connectors before deleting the ones they replace', async () => {
        kafkaConnect.add(`${prefix}source-connector-3f1c7a52-9b1e-4c1d-8f2e-6a7b8c9d0e1f`, { offsetTime: 900 });
        await reconcile(newManager(1));
        const order = kafkaConnect.calls.map(([type]) => type).filter(t => t === 'create' || t === 'delete');
        assert.deepStrictEqual(order, ['create', 'delete']);
    });

    it('should replace each connector from its own offset when the pipeline changes', async () => {
        const oldNames = connectorNames(2, 1000);
        kafkaConnect.add(oldNames[0], { offsetTime: 500 });
        kafkaConnect.add(oldNames[1], { offsetTime: 700 });
        await reconcile(newManager(2));
        const names = connectorNames(2);
        assert.deepStrictEqual(Object.keys(kafkaConnect.connectors), names);
        assert.deepStrictEqual(startOf(names[0]), { t: 500, i: 0 });
        assert.deepStrictEqual(startOf(names[1]), { t: 700, i: 0 });
    });

    it('should start every connector from the earliest offset when the number of connectors changes', async () => {
        const oldNames = connectorNames(2);
        kafkaConnect.add(oldNames[0], { offsetTime: 500 });
        kafkaConnect.add(oldNames[1], { offsetTime: 700 });
        await reconcile(newManager(3));
        const names = connectorNames(3);
        assert.deepStrictEqual(Object.keys(kafkaConnect.connectors), names);
        names.forEach(name => assert.deepStrictEqual(startOf(name), { t: 500, i: 0 }));
    });

    it('should not touch anything when the offsets API is unavailable', async () => {
        kafkaConnect.add(`${prefix}source-connector-3f1c7a52-9b1e-4c1d-8f2e-6a7b8c9d0e1f`, { offsetTime: 900 });
        kafkaConnect.offsetsSupported = false;
        await reconcile(newManager(1));
        assert.deepStrictEqual(kafkaConnect.callsOf('pause'), []);
        assert.deepStrictEqual(kafkaConnect.callsOf('create'), []);
        assert.deepStrictEqual(kafkaConnect.callsOf('delete'), []);
        assert(metrics.onMigration.notCalled);
    });

    it('should delete leftover connectors once every hashed connector exists', async () => {
        const leftover = `${prefix}source-connector-3f1c7a52-9b1e-4c1d-8f2e-6a7b8c9d0e1f`;
        kafkaConnect.add(leftover, { state: 'PAUSED', offsetTime: 900 });
        addConnectors(2);
        await reconcile(newManager(2));
        assert.deepStrictEqual(kafkaConnect.callsOf('delete'), [leftover]);
        assert.deepStrictEqual(kafkaConnect.callsOf('offsets'), []);
        assert.strictEqual(Date.now(), 0);
    });

    it('should complete a migration interrupted between connector creations', async () => {
        const leftover = `${prefix}source-connector-3f1c7a52-9b1e-4c1d-8f2e-6a7b8c9d0e1f`;
        kafkaConnect.add(leftover, { state: 'PAUSED', offsetTime: 900 });
        const names = connectorNames(2);
        kafkaConnect.add(names[0], { config: { [startupTimeKey]: formatStartupTime({ t: 900, i: 0 }) } });
        await reconcile(newManager(2));
        assert.deepStrictEqual(kafkaConnect.callsOf('create'), [names[1]]);
        assert.deepStrictEqual(startOf(names[1]), { t: 900, i: 0 });
        assert.deepStrictEqual(kafkaConnect.callsOf('delete'), [leftover]);
    });

    it('should tolerate connectors left out of the listing', async () => {
        addConnectors(1);
        const [name] = connectorNames(1);
        const list = kafkaConnect.getConnectorsWithStatus.bind(kafkaConnect);
        sinon.stub(kafkaConnect, 'getConnectorsWithStatus').callsFake(async () => {
            const listed = await list();
            delete listed[name];
            return listed;
        });
        await reconcile(newManager(1));
        assert.deepStrictEqual(kafkaConnect.callsOf('create'), [name]);
        assert.deepStrictEqual(Object.keys(kafkaConnect.connectors), [name]);
    });

    it('should leave connectors of a newer generation alone', async () => {
        const newer = hashedConnectorName({ prefix, generation: 99, nbConnectors: 1, index: 0, pipeline: '[]' });
        kafkaConnect.add(newer);
        kafkaConnect.add(`${prefix}source-connector-3f1c7a52-9b1e-4c1d-8f2e-6a7b8c9d0e1f`, { offsetTime: 900 });
        await reconcile(newManager(1));
        assert.deepStrictEqual(kafkaConnect.calls, []);
        assert.strictEqual(metrics.onHashedConnectorsObserved.lastCall.args[0].newer, 1);
    });

    it('should not restart nor update its own connectors when a newer generation exists', async () => {
        const newer = hashedConnectorName({ prefix, generation: 99, nbConnectors: 2, index: 0, pipeline: '[]' });
        kafkaConnect.add(newer);
        addConnectors(2, { state: 'FAILED', config: { 'connection.uri': 'mongodb://old-host:27017' } });
        await reconcile(newManager(2));
        assert.deepStrictEqual(kafkaConnect.calls, []);
    });

    it('should keep the pre-pause offset of connectors deleted during the migration', async () => {
        const gone = `${prefix}source-connector-3f1c7a52-9b1e-4c1d-8f2e-6a7b8c9d0e1f`;
        const kept = `${prefix}source-connector-0d6c1a2b-3c4d-4e5f-8a9b-0c1d2e3f4a5b`;
        kafkaConnect.add(gone, { offsetTime: 100 });
        kafkaConnect.add(kept, { offsetTime: 900 });
        const pause = kafkaConnect.pauseConnector.bind(kafkaConnect);
        sinon.stub(kafkaConnect, 'pauseConnector').callsFake(async name => {
            if (name === gone) {
                delete kafkaConnect.connectors[gone];
            }
            return pause(name);
        });
        await reconcile(newManager(1));
        const [name] = connectorNames(1);
        assert.deepStrictEqual(startOf(name), { t: 100, i: 0 });
        assert.deepStrictEqual(kafkaConnect.callsOf('delete'), [kept]);
    });

    it('should keep the pre-pause offset of connectors deleted while waiting for the pause', async () => {
        const gone = `${prefix}source-connector-3f1c7a52-9b1e-4c1d-8f2e-6a7b8c9d0e1f`;
        const kept = `${prefix}source-connector-0d6c1a2b-3c4d-4e5f-8a9b-0c1d2e3f4a5b`;
        kafkaConnect.add(gone, { offsetTime: 100 });
        kafkaConnect.add(kept, { offsetTime: 900 });
        const pause = kafkaConnect.pauseConnector.bind(kafkaConnect);
        sinon.stub(kafkaConnect, 'pauseConnector').callsFake(async name => {
            await pause(name);
            if (name === gone) {
                delete kafkaConnect.connectors[gone];
            }
        });
        await reconcile(newManager(1));
        assert.strictEqual(kafkaConnect.listings, 2);
        assert.deepStrictEqual(startOf(connectorNames(1)[0]), { t: 100, i: 0 });
        assert.deepStrictEqual(kafkaConnect.callsOf('delete'), [kept]);
    });

    it('should leave out connectors deleted before their offsets were first read', async () => {
        const gone = `${prefix}source-connector-3f1c7a52-9b1e-4c1d-8f2e-6a7b8c9d0e1f`;
        const kept = `${prefix}source-connector-0d6c1a2b-3c4d-4e5f-8a9b-0c1d2e3f4a5b`;
        kafkaConnect.add(gone, { offsetTime: 100 });
        kafkaConnect.add(kept, { offsetTime: 900 });
        const readOffsets = kafkaConnect.getConnectorOffsets.bind(kafkaConnect);
        sinon.stub(kafkaConnect, 'getConnectorOffsets').callsFake(async name => {
            if (name === gone) {
                delete kafkaConnect.connectors[gone];
            }
            return readOffsets(name);
        });
        await reconcile(newManager(1));
        assert.deepStrictEqual(startOf(connectorNames(1)[0]), { t: 900, i: 0 });
        assert.deepStrictEqual(kafkaConnect.callsOf('pause'), [kept]);
    });

    it('should tolerate connectors deleted before their deletion', async () => {
        const name = `${prefix}source-connector-3f1c7a52-9b1e-4c1d-8f2e-6a7b8c9d0e1f`;
        kafkaConnect.add(name, { offsetTime: 900 });
        const create = kafkaConnect.createConnector.bind(kafkaConnect);
        sinon.stub(kafkaConnect, 'createConnector').callsFake(async params => {
            delete kafkaConnect.connectors[name];
            return create(params);
        });
        await reconcile(newManager(1));
        assert.deepStrictEqual(kafkaConnect.callsOf('delete'), [name]);
        assert.deepStrictEqual(Object.keys(kafkaConnect.connectors), connectorNames(1));
        assert(metrics.onMigration.calledOnceWith(true));
    });

    it('should consider failed tasks of paused connectors stopped', async () => {
        kafkaConnect.add(`${prefix}source-connector-3f1c7a52-9b1e-4c1d-8f2e-6a7b8c9d0e1f`, {
            taskState: 'FAILED',
            offsetTime: 900,
        });
        await reconcile(newManager(1));
        // stopped on the first status poll, after the reconciliation listing
        assert.strictEqual(kafkaConnect.listings, 2);
        assert.deepStrictEqual(startOf(connectorNames(1)[0]), { t: 900, i: 0 });
    });

    it('should consider unassigned tasks of paused connectors stopped', async () => {
        kafkaConnect.add(`${prefix}source-connector-3f1c7a52-9b1e-4c1d-8f2e-6a7b8c9d0e1f`, {
            taskState: 'UNASSIGNED',
            offsetTime: 900,
        });
        await reconcile(newManager(1));
        // stopped on the first status poll, after the reconciliation listing
        assert.strictEqual(kafkaConnect.listings, 2);
        assert.deepStrictEqual(startOf(connectorNames(1)[0]), { t: 900, i: 0 });
    });

    it('should retry later when paused connectors keep running', async () => {
        const name = `${prefix}source-connector-3f1c7a52-9b1e-4c1d-8f2e-6a7b8c9d0e1f`;
        kafkaConnect.add(name, { offsetTime: 900 });
        sinon.stub(kafkaConnect, 'pauseConnector').resolves();
        await reconcile(newManager(1));
        assert.deepStrictEqual(kafkaConnect.callsOf('create'), []);
        assert.deepStrictEqual(kafkaConnect.callsOf('delete'), []);
    });

    it('should only wait for the remaining flush delay when retrying a migration', async () => {
        const name = `${prefix}source-connector-3f1c7a52-9b1e-4c1d-8f2e-6a7b8c9d0e1f`;
        kafkaConnect.add(name, { offsetTime: 900 });
        const manager = newManager(1);
        sinon.stub(kafkaConnect, 'getConnectorOffsets')
            .onSecondCall().rejects(errors.InternalError)
            .callsFake(async n => kafkaConnect.connectors[n].offsets);
        await assert.rejects(reconcile(manager));
        assert(metrics.onMigration.calledOnceWith(false));
        clock.tick(40000);
        const retriedAt = Date.now();
        await reconcile(manager);
        assert.strictEqual(Date.now(), retriedAt);
        assert.deepStrictEqual(Object.keys(kafkaConnect.connectors), connectorNames(1));
    });

    it('should assume a connector without offset started shortly before its pause', async () => {
        kafkaConnect.add(`${prefix}source-connector-3f1c7a52-9b1e-4c1d-8f2e-6a7b8c9d0e1f`);
        await reconcile(newManager(1));
        // paused 60s before the cluster time (frozen at 1000), minus the 120s margin
        assert.deepStrictEqual(startOf(connectorNames(1)[0]), { t: 820, i: 0 });
    });

    it('should anchor the start of a connector without offset on its pause, not on a later retry', async () => {
        kafkaConnect.add(`${prefix}source-connector-3f1c7a52-9b1e-4c1d-8f2e-6a7b8c9d0e1f`);
        const manager = newManager(1);
        manager._getClusterTime = async () => ({ t: 1000 + Math.floor(Date.now() / 1000), i: 0 });
        sinon.stub(kafkaConnect, 'getConnectorOffsets')
            .onSecondCall().rejects(errors.InternalError)
            .callsFake(async n => kafkaConnect.connectors[n].offsets);
        // paused at cluster time 1000, then the migration fails
        await assert.rejects(reconcile(manager));
        clock.tick(600000);
        await reconcile(manager);
        assert.deepStrictEqual(startOf(connectorNames(1)[0]), { t: 880, i: 0 });
    });

    it('should use the start time of a connector that never committed', async () => {
        kafkaConnect.add(`${prefix}source-connector-3f1c7a52-9b1e-4c1d-8f2e-6a7b8c9d0e1f`, {
            config: { [startupTimeKey]: '1970-01-01T00:16:40.000Z' },
        });
        await reconcile(newManager(1));
        assert.deepStrictEqual(startOf(connectorNames(1)[0]), { t: 1000, i: 0 });
    });

    describe('hashed connectors checks', () => {
        it('should restart failed connectors', async () => {
            const [name] = connectorNames(1);
            addConnectors(1, { state: 'FAILED' });
            await reconcile(newManager(1));
            assert.deepStrictEqual(kafkaConnect.callsOf('restart'), [name]);
            assert(metrics.onConnectorRestart.calledOnceWith({ name }));
        });

        it('should keep restarting connectors when one fails to restart', async () => {
            addConnectors(2, { state: 'FAILED' });
            const [failing, other] = connectorNames(2);
            const restart = kafkaConnect.restartConnector.bind(kafkaConnect);
            sinon.stub(kafkaConnect, 'restartConnector').callsFake(async name => {
                if (name === failing) {
                    throw errors.InternalError;
                }
                return restart(name);
            });
            await reconcile(newManager(2));
            assert.strictEqual(kafkaConnect.connectors[other].state, 'RUNNING');
            assert.strictEqual(kafkaConnect.connectors[failing].state, 'FAILED');
        });

        it('should not resume paused connectors', async () => {
            addConnectors(1, { state: 'PAUSED' });
            await reconcile(newManager(1));
            assert.deepStrictEqual(kafkaConnect.calls, []);
            assert.strictEqual(metrics.onHashedConnectorsObserved.lastCall.args[0].states.paused, 1);
        });

        it('should update drifted settings, keeping the connector offset', async () => {
            const manager = newManager(1);
            const [desired] = manager._desired;
            const startTime = formatStartupTime({ t: 5, i: 0 });
            kafkaConnect.add(desired.name, {
                config: {
                    ...desired.config,
                    'connection.uri': 'mongodb://old-host:27017',
                    'offset.partition.name': 'partition-live',
                    [startupTimeKey]: startTime,
                },
            });
            await reconcile(manager);
            assert.deepStrictEqual(kafkaConnect.callsOf('update'), [desired.name]);
            const { config } = kafkaConnect.connectors[desired.name];
            assert.strictEqual(config['connection.uri'], 'mongodb://localhost:27017');
            assert.strictEqual(config['offset.partition.name'], 'partition-live');
            assert.strictEqual(config[startupTimeKey], startTime);
        });

        it('should report but keep an edited pipeline', async () => {
            const manager = newManager(1);
            const [desired] = manager._desired;
            kafkaConnect.add(desired.name, { config: { ...desired.config, pipeline: '[]' } });
            await reconcile(manager);
            assert.deepStrictEqual(kafkaConnect.calls, []);
            assert.strictEqual(kafkaConnect.connectors[desired.name].config.pipeline, '[]');
            assert.strictEqual(metrics.onHashedConnectorsObserved.lastCall.args[0].states.edited, 1);
        });
    });

    describe('decision helpers', () => {
        const running = { connector: { state: 'RUNNING' }, tasks: [{ id: 0, state: 'RUNNING' }] };
        const named = (generation, nbConnectors, index, pipeline = '[]') =>
            hashedConnectorName({ prefix, generation, nbConnectors, index, pipeline });
        const listed = names => Object.fromEntries(names.map(name =>
            [name, { info: { config: {} }, status: running }]));

        describe('_classify', () => {
            it('should report every desired connector as missing on a fresh install', () => {
                const manager = newManager(2);
                const plan = manager._classify({});
                assert.deepStrictEqual(plan.missing, manager._desired);
                assert.deepStrictEqual([plan.newer, plan.current, plan.outdated], [[], [], []]);
            });

            it('should sort current, outdated and newer connectors, and ignore unowned ones', () => {
                const manager = newManager(2);
                const [desired0, desired1] = manager._desired;
                const perBucket = `${prefix}source-connector-3f1c7a52-9b1e-4c1d-8f2e-6a7b8c9d0e1f`;
                const connectors = listed([
                    desired0.name,
                    perBucket,
                    named(0, 2, 1),
                    named(1, 4, 1),
                    named(1, 2, 1, '[edited]'),
                    named(2, 2, 0),
                    'other-connector',
                ]);
                const plan = manager._classify(connectors);
                assert.deepStrictEqual(plan.current.map(c => c.name), [desired0.name]);
                assert.strictEqual(plan.current[0].desired, desired0);
                assert.deepStrictEqual(plan.outdated.map(c => c.name),
                    [perBucket, named(0, 2, 1), named(1, 4, 1), named(1, 2, 1, '[edited]')]);
                assert.deepStrictEqual(plan.newer.map(c => c.name), [named(2, 2, 0)]);
                assert.deepStrictEqual(plan.missing, [desired1]);
            });
        });

        describe('_connectorState', () => {
            [
                ['running', running, 'running'],
                ['paused', { connector: { state: 'PAUSED' }, tasks: [{ state: 'PAUSED' }] }, 'paused'],
                ['failed connector', { connector: { state: 'FAILED' }, tasks: [] }, 'failed'],
                ['failed task', { connector: { state: 'RUNNING' }, tasks: [{ state: 'FAILED' }] }, 'failed'],
            ].forEach(([desc, status, expected]) => {
                it(`should report a ${desc} connector as ${expected}`, () => {
                    assert.strictEqual(newManager(1)._connectorState(status), expected);
                });
            });
        });

        describe('_isPipelineEdited', () => {
            it('should compare the live pipeline with the hash in the name', () => {
                const name = named(1, 1, 0, '[0]');
                const parsed = parseHashedConnectorName(prefix, name);
                const manager = newManager(1);
                assert.strictEqual(manager._isPipelineEdited({ name, config: { pipeline: '[0]' }, parsed }), false);
                assert.strictEqual(manager._isPipelineEdited({ name, config: { pipeline: '[]' }, parsed }), true);
            });
        });

        describe('_configDrift', () => {
            it('should report managed fields only, comparing as strings', () => {
                const desired = {
                    'name': 'n',
                    'pipeline': '[]',
                    'heartbeat.interval.ms': 10000,
                    'value.converter.schemas.enable': false,
                    'connection.uri': 'mongodb://new',
                    'topic.namespace.map': '{"*":"oplog"}',
                };
                const live = {
                    'name': 'n',
                    'pipeline': '[edited]',
                    'heartbeat.interval.ms': '10000',
                    'value.converter.schemas.enable': 'false',
                    'connection.uri': 'mongodb://old',
                    'offset.partition.name': 'partition-1',
                    'startup.mode.timestamp.start.at.operation.time': '{}',
                    'offset.topic.name': 'manual',
                };
                assert.deepStrictEqual(newManager(1)._configDrift(live, desired),
                    ['connection.uri', 'topic.namespace.map']);
            });
        });

        describe('_connectorOplogTime', () => {
            const config = { 'offset.partition.name': 'partition-new' };
            const oplogTime = (offsets, connectorConfig) => newManager(1)._connectorOplogTime(offsets, connectorConfig);

            it('should use the committed offset of the current partition', () => {
                const offsets = [
                    { partition: { ns: 'partition-old' }, offset: { _id: resumeTokenAt(10) } },
                    { partition: { ns: 'partition-new' }, offset: { _id: resumeTokenAt(50, 2) } },
                ];
                assert.deepStrictEqual(oplogTime(offsets, config), { t: 50, i: 2 });
            });

            it('should use every partition when the connector has no partition name', () => {
                const offsets = [
                    { partition: { ns: 'mongodb://h/db' }, offset: { _id: resumeTokenAt(50) } },
                    { partition: { ns: 'mongodb://h2/db' }, offset: { _id: resumeTokenAt(40) } },
                ];
                assert.deepStrictEqual(oplogTime(offsets, {}), { t: 40, i: 0 });
            });

            it('should fall back to the configured start time', () => {
                const offsets = [{ partition: { ns: 'partition-old' }, offset: { _id: resumeTokenAt(10) } }];
                assert.deepStrictEqual(oplogTime(offsets, {
                    ...config,
                    [startupTimeKey]: '1970-01-01T00:01:00.500Z',
                }), { t: 60, i: 0 });
                assert.deepStrictEqual(oplogTime([], {
                    [startupTimeKey]: formatStartupTime({ t: 70, i: 1 }),
                }), { t: 70, i: 1 });
            });

            it('should return null when the oplog time is unknown', () => {
                assert.strictEqual(oplogTime([], config), null);
            });
        });

        describe('_startTimes', () => {
            const at = t => ({ t, i: 0 });
            const outdatedConnector = (nbConnectors, k, t, generation = 1) => ({
                parsed: { generation, nbConnectors, index: k, hash: '00000000' },
                time: at(t),
            });
            const missing = [{ index: 0 }, { index: 1 }];

            it('should start each connector from the one it replaces', () => {
                const startTimes = newManager(2)._startTimes(missing,
                    [outdatedConnector(2, 0, 10), outdatedConnector(2, 1, 20)]);
                assert.deepStrictEqual([...startTimes], [[0, at(10)], [1, at(20)]]);
            });

            it('should start every connector from the earliest time when the number of connectors changes', () => {
                const startTimes = newManager(2)._startTimes(missing,
                    [outdatedConnector(4, 0, 30), outdatedConnector(4, 3, 15)]);
                assert.deepStrictEqual([...startTimes], [[0, at(15)], [1, at(15)]]);
            });

            it('should start every connector from the earliest time when the generation changes', () => {
                const startTimes = newManager(2)._startTimes(missing,
                    [outdatedConnector(2, 0, 10, 0), outdatedConnector(2, 1, 20, 0)]);
                assert.deepStrictEqual([...startTimes], [[0, at(10)], [1, at(10)]]);
            });

            it('should account for per-bucket connectors in every hashed connector', () => {
                const startTimes = newManager(2)._startTimes(missing,
                    [outdatedConnector(2, 0, 10), outdatedConnector(2, 1, 20), { parsed: null, time: at(15) }]);
                assert.deepStrictEqual([...startTimes], [[0, at(10)], [1, at(15)]]);
            });

            it('should have no start point for a connector no outdated connector covers', () => {
                const startTimes = newManager(2)._startTimes([{ index: 1 }], [outdatedConnector(2, 0, 10)]);
                assert.deepStrictEqual([...startTimes], [[1, null]]);
            });
        });
    });
});
