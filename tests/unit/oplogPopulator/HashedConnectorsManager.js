const assert = require('assert');
const sinon = require('sinon');
const schedule = require('node-schedule');
const werelogs = require('werelogs');
const { errors } = require('@scality/arsenal');

const HashedConnectorsManager = require('../../../extensions/oplogPopulator/modules/HashedConnectorsManager');
const OplogPopulatorMetrics = require('../../../extensions/oplogPopulator/OplogPopulatorMetrics');
const ConnectorNameGenerator = require('../../../extensions/oplogPopulator/modules/ConnectorNameGenerator');
const ListedConnector = require('../../../extensions/oplogPopulator/modules/ListedConnector');
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
const nameGenerator = new ConnectorNameGenerator(prefix);
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
    let getClusterTime;

    function newManager(nbConnectors, locationStrippingBytesThreshold = 0, { fakeKafkaConnect = true } = {}) {
        const manager = new HashedConnectorsManager({
            config: {
                numberOfConnectors: nbConnectors,
                topic: 'oplog',
                connectorsUpdateCronRule: '* * * * * *',
                prefix,
                heartbeatIntervalMs: 10000,
                kafkaConnectHost: 'localhost',
                kafkaConnectPort: 8083,
                locationStrippingBytesThreshold,
            },
            database: 'metadata',
            mongoUrl: 'mongodb://localhost:27017',
            metricsHandler: metrics,
            getClusterTime,
            logger,
        });
        if (fakeKafkaConnect) {
            manager._kafkaConnect = kafkaConnect;
        }
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

    // last count reported for each state
    function counted() {
        return Object.fromEntries(metrics.onConnectorsCounted.getCalls().map(({ args }) => args));
    }

    beforeEach(() => {
        kafkaConnect = new FakeKafkaConnect();
        clock = sinon.useFakeTimers();
        metrics = new OplogPopulatorMetrics(logger);
        sinon.stub(metrics);
        getClusterTime = sinon.stub().resolves(now);
    });

    afterEach(() => {
        sinon.restore();
    });

    it('should report its connectors when started', () => {
        const scheduleStub = sinon.stub(schedule, 'scheduleJob');
        const manager = newManager(2);
        manager.start();
        assert(metrics.onHashedConnectorsStarted.calledOnceWithExactly(manager._connectorSpecs));
        assert(scheduleStub.calledOnce);
    });

    it('should record its connector creations and updates through its kafka connect wrapper', () => {
        const manager = newManager(1, 0, { fakeKafkaConnect: false });
        assert.strictEqual(manager._kafkaConnect._metricsHandler, metrics);
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
        assert(getClusterTime.notCalled);
        assert.deepStrictEqual(counted(), { running: 2, paused: 0, failed: 0, outdated: 0 });
    });

    it('should ignore connectors it does not own', async () => {
        kafkaConnect.add('other-connector');
        addConnectors(1);
        await reconcile(newManager(1));
        assert.deepStrictEqual(kafkaConnect.calls, []);
    });

    it('should replace per-bucket connectors from their earliest offset', async () => {
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
        assert.deepStrictEqual(kafkaConnect.callsOf('delete').sort(), [...perBucket].sort());
        assert.deepStrictEqual(counted(), { running: 0, paused: 0, failed: 0, outdated: 2 });
        // once to stop, once to create
        assert.strictEqual(getClusterTime.callCount, 2);
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
        assert.deepStrictEqual(counted(), { running: 0, paused: 0, failed: 0, outdated: 1 });
    });

    it('should log every reason outdated connectors are not ready to be replaced', async () => {
        const noOffsets = `${prefix}source-connector-3f1c7a52-9b1e-4c1d-8f2e-6a7b8c9d0e1f`;
        const notStopping = `${prefix}source-connector-0d6c1a2b-3c4d-4e5f-8a9b-0c1d2e3f4a5b`;
        kafkaConnect.add(noOffsets, { offsetTime: 900 });
        kafkaConnect.add(notStopping, { offsetTime: 900 });
        const readOffsets = kafkaConnect.getConnectorOffsets.bind(kafkaConnect);
        sinon.stub(kafkaConnect, 'getConnectorOffsets').callsFake(async name => {
            if (name === noOffsets) {
                throw httpError(404);
            }
            return readOffsets(name);
        });
        const stop = kafkaConnect.stopConnector.bind(kafkaConnect);
        sinon.stub(kafkaConnect, 'stopConnector').callsFake(async name => {
            if (name !== notStopping) {
                await stop(name);
            }
        });
        const warn = sinon.spy(logger, 'warn');
        await reconcile(newManager(1));
        assert.deepStrictEqual(kafkaConnect.callsOf('create'), []);
        assert.deepStrictEqual(kafkaConnect.callsOf('delete'), []);
        const [, { connectors }] = warn.getCalls()
            .find(({ args: [msg] }) => msg.startsWith('outdated connectors')).args;
        const byName = (x, y) => x.connector.localeCompare(y.connector);
        assert.deepStrictEqual(connectors.sort(byName), [
            { connector: notStopping, reason: 'connector tasks still running' },
            { connector: noOffsets, reason: 'kafka connect offsets API unavailable' },
        ].sort(byName));
    });

    it('should stop leftover connectors, then delete them once every hashed connector exists', async () => {
        const leftover = `${prefix}source-connector-3f1c7a52-9b1e-4c1d-8f2e-6a7b8c9d0e1f`;
        kafkaConnect.add(leftover, { state: 'STOPPED', offsetTime: 900 });
        addConnectors(2);
        await reconcile(newManager(2));
        assert.deepStrictEqual(kafkaConnect.calls,
            [['offsets', leftover], ['stop', leftover], ['offsets', leftover], ['delete', leftover]]);
        assert.strictEqual(getClusterTime.callCount, 1);
        assert.strictEqual(Date.now(), 0);
    });

    it('should keep leftover connectors not ready to be replaced, and delete them later', async () => {
        const leftover = `${prefix}source-connector-3f1c7a52-9b1e-4c1d-8f2e-6a7b8c9d0e1f`;
        kafkaConnect.add(leftover, { offsetTime: 900 });
        addConnectors(1);
        const manager = newManager(1);
        kafkaConnect.offsetsSupported = false;
        await reconcile(manager);
        assert.deepStrictEqual(kafkaConnect.callsOf('delete'), []);
        kafkaConnect.offsetsSupported = true;
        await reconcile(manager);
        assert.deepStrictEqual(kafkaConnect.callsOf('delete'), [leftover]);
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
        assert.deepStrictEqual(counted(), { running: 0, paused: 0, failed: 0, outdated: 0 });
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
    });

    it('should not replace connectors that fail to stop', async () => {
        kafkaConnect.add(`${prefix}source-connector-3f1c7a52-9b1e-4c1d-8f2e-6a7b8c9d0e1f`, { offsetTime: 900 });
        sinon.stub(kafkaConnect, 'stopConnector').rejects(httpError(500));
        await assert.rejects(reconcile(newManager(1)), err => err.statusCode === 500);
        assert.deepStrictEqual(kafkaConnect.callsOf('create'), []);
        assert.deepStrictEqual(kafkaConnect.callsOf('delete'), []);
    });

    it('should use the offsets read before stopping when they cannot be read after', async () => {
        const name = `${prefix}source-connector-3f1c7a52-9b1e-4c1d-8f2e-6a7b8c9d0e1f`;
        kafkaConnect.add(name, { offsetTime: 900 });
        const before = [{ partition: { ns: `partition-${name}` }, offset: { _id: resumeTokenAt(800) } }];
        sinon.stub(kafkaConnect, 'getConnectorOffsets')
            .onFirstCall().resolves(before)
            .onSecondCall().rejects(httpError(404));
        await reconcile(newManager(1));
        assert.deepStrictEqual(startOf(connectorNames(1)[0]), { t: 800, i: 0 });
        assert.deepStrictEqual(kafkaConnect.callsOf('delete'), [name]);
    });

    it('should complete a replacement on a later reconciliation, from stopped connectors', async () => {
        const name = `${prefix}source-connector-3f1c7a52-9b1e-4c1d-8f2e-6a7b8c9d0e1f`;
        kafkaConnect.add(name, { offsetTime: 900 });
        const manager = newManager(1);
        sinon.stub(kafkaConnect, 'getConnectorOffsets')
            .onSecondCall().rejects(errors.InternalError)
            .callsFake(async n => kafkaConnect.connectors[n].offsets);
        await assert.rejects(reconcile(manager));
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
        kafkaConnect.add(nameGenerator.hashedName({ nbConnectors: 2, index: 0, pipeline: '[]' }), {
            config: { [startupTimeKey]: connectorStartupTime({ t: 1000, i: 0 }) },
        });
        await reconcile(newManager(1));
        assert.deepStrictEqual(startOf(connectorNames(1)[0]), { t: 1000, i: 0 });
    });

    describe('crash safety', () => {
        const perBucket = `${prefix}source-connector-3f1c7a52-9b1e-4c1d-8f2e-6a7b8c9d0e1f`;
        const hashed = nameGenerator.hashedName({ nbConnectors: 1, index: 0, pipeline: '[]' });

        function addOutdated() {
            kafkaConnect.add(perBucket, { offsetTime: 900 });
            kafkaConnect.add(hashed, { config: { [startupTimeKey]: connectorStartupTime({ t: 950, i: 3 }) } });
        }

        [
            ['after stopping', 'createConnector', 0],
            ['after the first creation', 'createConnector', 1],
            ['before deleting', 'deleteConnector', 0],
        ].forEach(([desc, method, failingCall]) => {
            it(`should start from the same times when interrupted ${desc}`, async () => {
                addOutdated();
                await reconcile(newManager(2));
                const expected = connectorNames(2).map(startOf);
                assert.deepStrictEqual(expected, [{ t: 900, i: 0 }, { t: 900, i: 0 }]);

                kafkaConnect = new FakeKafkaConnect();
                addOutdated();
                const manager = newManager(2);
                sinon.stub(kafkaConnect, method).onCall(failingCall).rejects(httpError(500)).callThrough();
                await assert.rejects(reconcile(manager));
                await reconcile(manager);
                assert.deepStrictEqual(Object.keys(kafkaConnect.connectors).sort(), connectorNames(2).sort());
                assert.deepStrictEqual(connectorNames(2).map(startOf), expected);
            });
        });
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
            assert.strictEqual(counted().paused, 1);
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
            nameGenerator.hashedName({ nbConnectors, index, pipeline });
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
                assert.deepStrictEqual(plan.current.map(c => c.connector.name), [spec0.name]);
                assert(plan.current[0].connector instanceof ListedConnector);
                assert.strictEqual(plan.current[0].spec, spec0);
                assert.deepStrictEqual(plan.outdated.map(o => o.connector.name),
                    [perBucket, named(4, 1), named(2, 1, '[edited]')]);
                assert(plan.outdated.every(o => o.connector instanceof ListedConnector));
                assert.deepStrictEqual(plan.outdated.map(o => o.parsedName && o.parsedName.nbConnectors),
                    [null, 4, 2]);
                assert.deepStrictEqual(plan.missing, [spec1]);
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

        describe('_startTime', () => {
            const at = t => ({ t, i: 0 });
            const outdatedConnector = (parsedName, t) => {
                const connector = new ListedConnector({ name: 'c', config: {} });
                connector.oplogTime = t === null ? null : at(t);
                return { connector, parsedName };
            };
            const hashedAt = (nbConnectors, index, t) =>
                outdatedConnector({ nbConnectors, index, hash: '00000000' }, t);

            it('should start each connector from the one it replaces', () => {
                const manager = newManager(2);
                const outdated = [hashedAt(2, 0, 10), hashedAt(2, 1, 20)];
                assert.deepStrictEqual([0, 1].map(index => manager._startTime(index, outdated)), [at(10), at(20)]);
            });

            it('should start every connector from the earliest time when the number of connectors changes', () => {
                const manager = newManager(2);
                const outdated = [hashedAt(4, 0, 30), hashedAt(4, 3, 15)];
                assert.deepStrictEqual([0, 1].map(index => manager._startTime(index, outdated)), [at(15), at(15)]);
            });

            it('should account for per-bucket connectors in every hashed connector', () => {
                const manager = newManager(2);
                const outdated = [hashedAt(2, 0, 10), hashedAt(2, 1, 20), outdatedConnector(null, 15)];
                assert.deepStrictEqual([0, 1].map(index => manager._startTime(index, outdated)), [at(10), at(15)]);
            });

            it('should leave out connectors whose oplog time is unknown', () => {
                const outdated = [outdatedConnector(null, null), hashedAt(2, 1, 20)];
                assert.deepStrictEqual(newManager(2)._startTime(1, outdated), at(20));
            });

            it('should have no start point for a connector no outdated connector covers', () => {
                assert.strictEqual(newManager(2)._startTime(1, [hashedAt(2, 0, 10)]), null);
            });
        });
    });
});
