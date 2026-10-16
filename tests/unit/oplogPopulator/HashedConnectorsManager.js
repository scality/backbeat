const assert = require('assert');
const sinon = require('sinon');
const schedule = require('node-schedule');
const werelogs = require('werelogs');
const { errors } = require('@scality/arsenal');

const HashedConnectorsManager = require('../../../extensions/oplogPopulator/modules/HashedConnectorsManager');
const HashedPipelineFactory = require('../../../extensions/oplogPopulator/pipeline/HashedPipelineFactory');
const OplogPopulatorMetrics = require('../../../extensions/oplogPopulator/OplogPopulatorMetrics');
const { hashedConnectorName } =
    require('../../../extensions/oplogPopulator/modules/connectorNaming');
const { connectorStartupTime, timestampFromConnectorStartupTime } =
    require('../../../extensions/oplogPopulator/modules/resumeToken');

/**
 * Builds a change stream resume token, serialized as mongo-kafka commits it
 * in its source offsets, whose cluster time is (t, i)
 * @param {number} t cluster time seconds
 * @param {number} [i] cluster time increment
 * @returns {string} resume token as JSON
 */
function resumeTokenAt(t, i = 0) {
    return JSON.stringify({ _data: `82${t.toString(16).padStart(8, '0')}${i.toString(16).padStart(8, '0')}04` });
}

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

    _status(c) {
        // stopped connectors have no tasks
        const tasks = c.state === 'STOPPED' ? [] : [{ id: 0, state: c.taskState || c.state }];
        return { connector: { state: c.state }, tasks };
    }

    async getConnectorsWithStatus() {
        return Object.fromEntries(Object.entries(this.connectors).map(([name, c]) => [name, {
            info: { name, config: { ...c.config } },
            status: this._status(c),
        }]));
    }

    async getConnectorStatus(name) {
        return this._status(this._get(name));
    }

    async getConnectorOffsets(name) {
        this.calls.push(['offsets', name]);
        if (!this.offsetsSupported) {
            throw httpError(404);
        }
        return this._get(name).offsets;
    }

    async stopConnector(name) {
        this.calls.push(['stop', name]);
        this._get(name).state = 'STOPPED';
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
        newManager(nbConnectors)._connectorSpecs.forEach(d => kafkaConnect.add(d.name, {
            ...opts,
            config: { ...d.config, ...opts.config },
        }));
    }

    function connectorNames(nbConnectors, locationStrippingBytesThreshold = 0) {
        return newManager(nbConnectors, locationStrippingBytesThreshold)._connectorSpecs.map(d => d.name);
    }

    function startOf(name) {
        return timestampFromConnectorStartupTime(kafkaConnect.connectors[name].config[startupTimeKey]);
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

    it('should create its reconfiguration metrics when started', () => {
        const scheduleStub = sinon.stub(schedule, 'scheduleJob');
        newManager(1).start();
        assert(metrics.onHashedConnectorsStarted.calledOnce);
        assert(scheduleStub.calledOnce);
    });

    it('should refuse a number of connectors that is not a positive integer', () => {
        [0, 1.5].forEach(nbConnectors => assert.throws(() => newManager(nbConnectors)));
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
        assert.deepStrictEqual(kafkaConnect.callsOf('stop').sort(), [...perBucket].sort());
        // stopped connectors have committed their final offsets: no wait
        assert.strictEqual(Date.now(), 0);
        assert.deepStrictEqual(Object.keys(kafkaConnect.connectors), connectorNames(2));
        connectorNames(2).forEach(name => assert.deepStrictEqual(startOf(name), { t: 900, i: 0 }));
        assert(metrics.onMigration.calledOnceWith(true));
        // 2 creations sending their configuration, then 2 deletions
        const reconfigurations = metrics.onHashedConnectorReconfiguration.getCalls()
            .map(({ args: [success, , config] }) => [success, !!config]);
        assert.deepStrictEqual(reconfigurations, [[true, true], [true, true], [true, false], [true, false]]);
    });

    it('should record failed connector creations', async () => {
        sinon.stub(kafkaConnect, 'createConnector').rejects(httpError(500));
        await assert.rejects(reconcile(newManager(1)));
        assert(metrics.onHashedConnectorReconfiguration.calledOnceWith(false));
    });

    it('should report the pipeline size of its connectors', async () => {
        addConnectors(1);
        await reconcile(newManager(1));
        const [name] = connectorNames(1);
        assert(metrics.onHashedConnectorPipeline.calledOnceWith(name, kafkaConnect.connectors[name].config.pipeline));
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
        assert.deepStrictEqual(kafkaConnect.callsOf('stop'), []);
        assert.deepStrictEqual(kafkaConnect.callsOf('create'), []);
        assert.deepStrictEqual(kafkaConnect.callsOf('delete'), []);
        assert(metrics.onMigration.notCalled);
        assert.deepStrictEqual(metrics.onHashedConnectorsObserved.lastCall.args[0],
            { running: 0, paused: 0, failed: 0, outdated: 1 });
    });

    it('should delete leftover connectors once every hashed connector exists', async () => {
        const leftover = `${prefix}source-connector-3f1c7a52-9b1e-4c1d-8f2e-6a7b8c9d0e1f`;
        kafkaConnect.add(leftover, { state: 'STOPPED', offsetTime: 900 });
        addConnectors(2);
        await reconcile(newManager(2));
        assert.deepStrictEqual(kafkaConnect.callsOf('delete'), [leftover]);
        assert.deepStrictEqual(kafkaConnect.callsOf('offsets'), []);
        assert.strictEqual(Date.now(), 0);
    });

    it('should complete a migration interrupted between connector creations', async () => {
        const leftover = `${prefix}source-connector-3f1c7a52-9b1e-4c1d-8f2e-6a7b8c9d0e1f`;
        kafkaConnect.add(leftover, { state: 'STOPPED', offsetTime: 900 });
        const names = connectorNames(2);
        kafkaConnect.add(names[0], { config: { [startupTimeKey]: connectorStartupTime({ t: 900, i: 0 }) } });
        await reconcile(newManager(2));
        assert.deepStrictEqual(kafkaConnect.callsOf('create'), [names[1]]);
        assert.deepStrictEqual(startOf(names[1]), { t: 900, i: 0 });
        assert.deepStrictEqual(kafkaConnect.callsOf('delete'), [leftover]);
    });

    it('should retry later when a connector left out of the listing exists', async () => {
        addConnectors(1);
        const [name] = connectorNames(1);
        const list = kafkaConnect.getConnectorsWithStatus.bind(kafkaConnect);
        const listStub = sinon.stub(kafkaConnect, 'getConnectorsWithStatus').callsFake(async () => {
            const listed = await list();
            delete listed[name];
            return listed;
        });
        await assert.rejects(reconcile(newManager(1)), err => err.statusCode === 409);
        listStub.restore();
        await reconcile(newManager(1));
        assert.deepStrictEqual(kafkaConnect.callsOf('create'), [name]);
        assert.deepStrictEqual(Object.keys(kafkaConnect.connectors), [name]);
    });

    it('should keep outdated connectors when a creation conflicts', async () => {
        const name = `${prefix}source-connector-3f1c7a52-9b1e-4c1d-8f2e-6a7b8c9d0e1f`;
        kafkaConnect.add(name, { offsetTime: 900 });
        const manager = newManager(1);
        // kafka connect answers 409 during worker rebalances, without creating anything
        sinon.stub(kafkaConnect, 'createConnector').onFirstCall().rejects(httpError(409)).callThrough();
        await assert.rejects(reconcile(manager), err => err.statusCode === 409);
        assert.deepStrictEqual(kafkaConnect.callsOf('delete'), []);
        assert(metrics.onMigration.calledOnceWith(false));
        await reconcile(manager);
        assert.deepStrictEqual(Object.keys(kafkaConnect.connectors), connectorNames(1));
        assert.deepStrictEqual(startOf(connectorNames(1)[0]), { t: 900, i: 0 });
    });

    it('should leave alone connectors listed without their configuration', async () => {
        addConnectors(1, { config: { 'connection.uri': 'mongodb://old-host:27017' } });
        const list = kafkaConnect.getConnectorsWithStatus.bind(kafkaConnect);
        sinon.stub(kafkaConnect, 'getConnectorsWithStatus').callsFake(async () =>
            Object.fromEntries(Object.entries(await list()).map(([name, { status }]) => [name, { status }])));
        await reconcile(newManager(1));
        assert.deepStrictEqual(kafkaConnect.calls, []);
        assert.deepStrictEqual(metrics.onHashedConnectorsObserved.lastCall.args[0],
            { running: 0, paused: 0, failed: 0, outdated: 0 });
    });

    it('should keep the offset read before stopping of connectors deleted meanwhile', async () => {
        const gone = `${prefix}source-connector-3f1c7a52-9b1e-4c1d-8f2e-6a7b8c9d0e1f`;
        const kept = `${prefix}source-connector-0d6c1a2b-3c4d-4e5f-8a9b-0c1d2e3f4a5b`;
        kafkaConnect.add(gone, { offsetTime: 100 });
        kafkaConnect.add(kept, { offsetTime: 900 });
        const stop = kafkaConnect.stopConnector.bind(kafkaConnect);
        sinon.stub(kafkaConnect, 'stopConnector').callsFake(async name => {
            if (name === gone) {
                delete kafkaConnect.connectors[gone];
            }
            return stop(name);
        });
        await reconcile(newManager(1));
        assert.deepStrictEqual(startOf(connectorNames(1)[0]), { t: 100, i: 0 });
        assert.deepStrictEqual(Object.keys(kafkaConnect.connectors), connectorNames(1));
    });

    it('should keep the offset read before stopping of connectors deleted while stopping', async () => {
        const gone = `${prefix}source-connector-3f1c7a52-9b1e-4c1d-8f2e-6a7b8c9d0e1f`;
        const kept = `${prefix}source-connector-0d6c1a2b-3c4d-4e5f-8a9b-0c1d2e3f4a5b`;
        kafkaConnect.add(gone, { offsetTime: 100 });
        kafkaConnect.add(kept, { offsetTime: 900 });
        const stop = kafkaConnect.stopConnector.bind(kafkaConnect);
        sinon.stub(kafkaConnect, 'stopConnector').callsFake(async name => {
            await stop(name);
            if (name === gone) {
                delete kafkaConnect.connectors[gone];
            }
        });
        await reconcile(newManager(1));
        assert.deepStrictEqual(startOf(connectorNames(1)[0]), { t: 100, i: 0 });
        assert.deepStrictEqual(Object.keys(kafkaConnect.connectors), connectorNames(1));
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
        assert.deepStrictEqual(kafkaConnect.callsOf('stop'), [kept]);
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

    it('should stop failed connectors too', async () => {
        kafkaConnect.add(`${prefix}source-connector-3f1c7a52-9b1e-4c1d-8f2e-6a7b8c9d0e1f`, {
            state: 'FAILED',
            offsetTime: 900,
        });
        await reconcile(newManager(1));
        assert.deepStrictEqual(startOf(connectorNames(1)[0]), { t: 900, i: 0 });
    });

    it('should retry later when connectors do not stop', async () => {
        const name = `${prefix}source-connector-3f1c7a52-9b1e-4c1d-8f2e-6a7b8c9d0e1f`;
        kafkaConnect.add(name, { offsetTime: 900 });
        sinon.stub(kafkaConnect, 'stopConnector').resolves();
        await reconcile(newManager(1));
        assert.deepStrictEqual(kafkaConnect.callsOf('create'), []);
        assert.deepStrictEqual(kafkaConnect.callsOf('delete'), []);
        assert(metrics.onMigration.notCalled);
    });

    it('should complete a migration on a later reconciliation, from stopped connectors', async () => {
        const name = `${prefix}source-connector-3f1c7a52-9b1e-4c1d-8f2e-6a7b8c9d0e1f`;
        kafkaConnect.add(name, { offsetTime: 900 });
        const manager = newManager(1);
        sinon.stub(kafkaConnect, 'getConnectorOffsets')
            .onSecondCall().rejects(errors.InternalError)
            .callsFake(async n => kafkaConnect.connectors[n].offsets);
        await assert.rejects(reconcile(manager));
        assert(metrics.onMigration.calledOnceWith(false));
        assert.strictEqual(kafkaConnect.connectors[name].state, 'STOPPED');
        await reconcile(manager);
        assert.deepStrictEqual(Object.keys(kafkaConnect.connectors), connectorNames(1));
        assert.deepStrictEqual(startOf(connectorNames(1)[0]), { t: 900, i: 0 });
    });

    it('should assume a connector without offset started shortly before it stopped', async () => {
        kafkaConnect.add(`${prefix}source-connector-3f1c7a52-9b1e-4c1d-8f2e-6a7b8c9d0e1f`);
        await reconcile(newManager(1));
        // stopped at the cluster time (frozen at 1000), minus the 120s margin
        assert.deepStrictEqual(startOf(connectorNames(1)[0]), { t: 880, i: 0 });
    });

    it('should use the start time of a connector that never committed', async () => {
        kafkaConnect.add(hashedConnectorName({ prefix, nbConnectors: 2, index: 0, pipeline: '[]' }), {
            config: { [startupTimeKey]: connectorStartupTime({ t: 1000, i: 0 }) },
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
            assert.strictEqual(metrics.onHashedConnectorsObserved.lastCall.args[0].paused, 1);
        });

        it('should update drifted settings, keeping the connector offset', async () => {
            const manager = newManager(1);
            const [spec] = manager._connectorSpecs;
            const startTime = connectorStartupTime({ t: 5, i: 0 });
            kafkaConnect.add(spec.name, {
                config: {
                    ...spec.config,
                    'connection.uri': 'mongodb://old-host:27017',
                    'offset.partition.name': 'partition-live',
                    [startupTimeKey]: startTime,
                },
            });
            await reconcile(manager);
            assert.deepStrictEqual(kafkaConnect.callsOf('update'), [spec.name]);
            const { config } = kafkaConnect.connectors[spec.name];
            assert.strictEqual(config['connection.uri'], 'mongodb://localhost:27017');
            assert.strictEqual(config['offset.partition.name'], 'partition-live');
            assert.strictEqual(config[startupTimeKey], startTime);
        });

        it('should update a failed connector whose settings drifted, rather than restart it', async () => {
            const manager = newManager(1);
            const [spec] = manager._connectorSpecs;
            kafkaConnect.add(spec.name, {
                state: 'FAILED',
                config: { ...spec.config, 'connection.uri': 'mongodb://old-host:27017' },
            });
            await reconcile(manager);
            assert.deepStrictEqual(kafkaConnect.callsOf('update'), [spec.name]);
            assert.deepStrictEqual(kafkaConnect.callsOf('restart'), []);
        });

        it('should leave an edited pipeline alone', async () => {
            const manager = newManager(1);
            const [spec] = manager._connectorSpecs;
            kafkaConnect.add(spec.name, { config: { ...spec.config, pipeline: '[]' } });
            await reconcile(manager);
            assert.deepStrictEqual(kafkaConnect.calls, []);
            assert.strictEqual(kafkaConnect.connectors[spec.name].config.pipeline, '[]');
        });
    });

    describe('decision helpers', () => {
        const running = { connector: { state: 'RUNNING' }, tasks: [{ id: 0, state: 'RUNNING' }] };
        const named = (nbConnectors, index, pipeline = '[]') =>
            hashedConnectorName({ prefix, nbConnectors, index, pipeline });
        const listed = names => Object.fromEntries(names.map(name =>
            [name, { info: { config: {} }, status: running }]));

        describe('_classify', () => {
            it('should report every desired connector as missing on a fresh install', () => {
                const manager = newManager(2);
                const plan = manager._classify({});
                assert.deepStrictEqual(plan.missing, manager._connectorSpecs);
                assert.deepStrictEqual([plan.current, plan.outdated], [[], []]);
            });

            it('should sort current and outdated connectors, and ignore unowned ones', () => {
                const manager = newManager(2);
                const [spec0, spec1] = manager._connectorSpecs;
                const perBucket = `${prefix}source-connector-3f1c7a52-9b1e-4c1d-8f2e-6a7b8c9d0e1f`;
                const connectors = listed([
                    spec0.name,
                    perBucket,
                    named(4, 1),
                    named(2, 1, '[edited]'),
                    'other-connector',
                ]);
                const plan = manager._classify(connectors);
                assert.deepStrictEqual(plan.current.map(c => c.name), [spec0.name]);
                assert.strictEqual(plan.current[0].spec, spec0);
                assert.deepStrictEqual(plan.outdated.map(c => c.name),
                    [perBucket, named(4, 1), named(2, 1, '[edited]')]);
                assert.strictEqual(plan.outdated[0].parsed, null);
                assert.deepStrictEqual(plan.missing, [spec1]);
            });
        });

        describe('_connectorState', () => {
            [
                ['running', running, 'running'],
                ['paused', { connector: { state: 'PAUSED' }, tasks: [{ state: 'PAUSED' }] }, 'paused'],
                ['stopped', { connector: { state: 'STOPPED' }, tasks: [] }, 'paused'],
                ['failed connector', { connector: { state: 'FAILED' }, tasks: [] }, 'failed'],
                ['failed task', { connector: { state: 'RUNNING' }, tasks: [{ state: 'FAILED' }] }, 'failed'],
            ].forEach(([desc, status, expected]) => {
                it(`should report a ${desc} connector as ${expected}`, () => {
                    assert.strictEqual(newManager(1)._connectorState(status), expected);
                });
            });
        });

        describe('_configDrift', () => {
            it('should report managed fields only, comparing as strings', () => {
                const spec = {
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
                assert.deepStrictEqual(newManager(1)._configDrift(live, spec),
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
                    [startupTimeKey]: connectorStartupTime({ t: 60, i: 0 }),
                }), { t: 60, i: 0 });
                assert.deepStrictEqual(oplogTime([], {
                    [startupTimeKey]: connectorStartupTime({ t: 70, i: 1 }),
                }), { t: 70, i: 1 });
            });

            it('should return null when the oplog time is unknown', () => {
                assert.strictEqual(oplogTime([], config), null);
            });
        });

        describe('_startTimes', () => {
            const at = t => ({ t, i: 0 });
            const outdatedConnector = (nbConnectors, k, t) => ({
                parsed: { nbConnectors, index: k, hash: '00000000' },
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
