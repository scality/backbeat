const assert = require('assert');
const fs = require('fs');
const os = require('os');
const path = require('path');
const sinon = require('sinon');
const { ZenkoMetrics } = require('arsenal').metrics;

const WorkgroupConfigLoader = require(
    '../../../extensions/notification/deliveryWorker/WorkgroupConfigLoader');

const CHANGES_METRIC =
    's3_notification_delivery_worker_workgroup_config_changes_total';
const GENERATION_METRIC =
    's3_notification_delivery_worker_workgroup_generation';

const TOPIC = 'bucket-notification-delivery';
const ZK_PATH = '/notification/delivery-workgroups';

// node-zookeeper-client watch event types
const NODE_DELETED = 2;
const NODE_DATA_CHANGED = 3;

function makeDoc(overrides) {
    return Object.assign({
        configVersion: 1,
        generation: 3,
        topic: TOPIC,
        updatedAt: '2026-08-27T11:04:18.221Z',
        workgroups: [
            { id: 'wg-a', rule: { type: 'hashmod', modulo: 2, remainders: [0] } },
            { id: 'wg-b', rule: { type: 'hashmod', modulo: 2, remainders: [1] } },
        ],
        barriers: { 0: 154023, 1: 154990 },
    }, overrides);
}

/**
 * Logger that records what it was told, so a test can assert on a warning
 * without printing it
 *
 * @return {Object} recording logger
 */
function makeLogger() {
    const logger = {
        infos: [],
        warns: [],
        errors: [],
        info: (msg, data) => logger.infos.push({ msg, data }),
        warn: (msg, data) => logger.warns.push({ msg, data }),
        error: (msg, data) => logger.errors.push({ msg, data }),
        debug: () => {},
        trace: () => {},
    };
    return logger;
}

/**
 * Stubs the zookeeper client methods the loader uses. getData answers with
 * the responses in order, repeating the last one, and records the watcher it
 * was given so the re-arm discipline can be asserted.
 *
 * @param {Object[]} responses - { err, data } in call order
 * @return {Object} zookeeper client stub
 */
function makeZkClient(responses) {
    const client = {
        getDataCalls: [],
        existsCalls: [],
        closed: 0,
        getData(zkPath, watcher, cb) {
            const index = client.getDataCalls.length;
            client.getDataCalls.push({ zkPath, watcher });
            const response = responses[Math.min(index, responses.length - 1)];
            return process.nextTick(() =>
                cb(response.err || null, response.data));
        },
        exists(zkPath, watcher, cb) {
            client.existsCalls.push({ zkPath, watcher });
            return process.nextTick(() => cb(null, { version: 1 }));
        },
        setData: sinon.spy(),
        mkdirp: sinon.spy(),
        close: () => { client.closed += 1; },
    };
    return client;
}

async function counterValue(name, labels) {
    const data = await ZenkoMetrics.getMetric(name).get();
    const entry = data.values.find(value => Object.entries(labels)
        .every(([label, expected]) => value.labels[label] === expected));
    return entry ? entry.value : 0;
}

describe('WorkgroupConfigLoader', () => {
    let tmpDir;
    let cachePath;
    let logger;

    beforeEach(() => {
        tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'wg-loader-'));
        cachePath = path.join(tmpDir, 'delivery-workgroups.json');
        logger = makeLogger();
    });

    afterEach(() => {
        sinon.restore();
        fs.rmSync(tmpDir, { recursive: true, force: true });
    });

    function makeLoader(params) {
        return new WorkgroupConfigLoader(Object.assign({
            zkConfig: { connectionString: '127.0.0.1:2181' },
            workgroupsConfig: { zookeeperPath: ZK_PATH, cachePath },
            topic: TOPIC,
            workgroupId: 'wg-a',
            logger,
            validate: doc => ({ error: null, value: doc }),
        }, params));
    }

    it('should read the document from zookeeper and cache it', done => {
        const doc = makeDoc();
        const loader = makeLoader({
            zkClient: makeZkClient([
                { data: Buffer.from(JSON.stringify(doc)) },
            ]),
        });
        loader.load((err, loaded) => {
            assert.ifError(err);
            assert.strictEqual(loaded.source, 'zookeeper');
            assert.deepStrictEqual(loaded.doc, doc);
            assert.deepStrictEqual(loader.getConfig(), doc);
            const cached = JSON.parse(fs.readFileSync(cachePath, 'utf8'));
            assert.deepStrictEqual(cached, doc);
            return counterValue(GENERATION_METRIC, {
                workgroup: 'wg-a',
                source: 'zookeeper',
            }).then(value => {
                assert.strictEqual(value, 3);
                done();
            }).catch(done);
        });
    });

    it('should fall back to the cache when the node does not exist', done => {
        const doc = makeDoc();
        fs.writeFileSync(cachePath, JSON.stringify(doc));
        const loader = makeLoader({
            zkClient: makeZkClient([{ err: { name: 'NO_NODE' } }]),
        });
        loader.load((err, loaded) => {
            assert.ifError(err);
            assert.strictEqual(loaded.source, 'cache');
            assert.deepStrictEqual(loaded.doc, doc);
            assert(logger.warns.length > 0);
            done();
        });
    });

    it('should fail when zookeeper has no node and there is no cache', done => {
        const loader = makeLoader({
            zkClient: makeZkClient([{ err: { name: 'NO_NODE' } }]),
        });
        loader.load(err => {
            assert(err);
            assert(err.description.includes(cachePath));
            done();
        });
    });

    it('should refuse a cache at another generation than the pinned one',
    done => {
        fs.writeFileSync(cachePath, JSON.stringify(makeDoc({ generation: 2 })));
        const loader = makeLoader({
            workgroupsConfig: {
                zookeeperPath: ZK_PATH,
                cachePath,
                generation: 3,
            },
            zkClient: makeZkClient([{ err: { name: 'CONNECTION_LOSS' } }]),
        });
        loader.load(err => {
            assert(err);
            assert(err.description.includes('generation'));
            assert.strictEqual(loader.getConfig(), null);
            done();
        });
    });

    it('should refuse a zookeeper document at another generation than the ' +
    'pinned one', done => {
        const loader = makeLoader({
            workgroupsConfig: {
                zookeeperPath: ZK_PATH,
                cachePath,
                generation: 7,
            },
            zkClient: makeZkClient([
                { data: Buffer.from(JSON.stringify(makeDoc())) },
            ]),
        });
        loader.load(err => {
            assert(err);
            assert(err.description.includes('generation 7'));
            done();
        });
    });

    it('should refuse a document written for another topic', done => {
        const loader = makeLoader({
            zkClient: makeZkClient([{
                data: Buffer.from(JSON.stringify(makeDoc({
                    topic: 'another-delivery-topic',
                }))),
            }]),
        });
        loader.load(err => {
            assert(err);
            assert(err.description.includes('another-delivery-topic'));
            done();
        });
    });

    it('should refuse a document that does not list this workgroup', done => {
        const loader = makeLoader({
            workgroupId: 'wg-missing',
            zkClient: makeZkClient([
                { data: Buffer.from(JSON.stringify(makeDoc())) },
            ]),
        });
        loader.load(err => {
            assert(err);
            assert(err.description.includes('wg-missing'));
            done();
        });
    });

    it('should not consult the cache when the document is not valid JSON',
    done => {
        fs.writeFileSync(cachePath, JSON.stringify(makeDoc()));
        const readFile = sinon.spy(fs, 'readFile');
        const loader = makeLoader({
            zkClient: makeZkClient([{ data: Buffer.from('{ not json') }]),
        });
        loader.load(err => {
            assert(err);
            assert(err.description.includes('not valid JSON'));
            assert(readFile.notCalled);
            done();
        });
    });

    it('should fail when the document does not pass the validator', done => {
        const loader = makeLoader({
            validate: () => ({ error: new Error('remainders do not cover 0') }),
            zkClient: makeZkClient([
                { data: Buffer.from(JSON.stringify(makeDoc())) },
            ]),
        });
        loader.load(err => {
            assert(err);
            assert(err.description.includes('remainders do not cover 0'));
            done();
        });
    });

    it('should count a change and re-arm the watch without reconfiguring',
    done => {
        const workgroups = [
            { id: 'wg-watch', rule: { type: 'hashmod', modulo: 2, remainders: [0] } },
            { id: 'wg-b', rule: { type: 'hashmod', modulo: 2, remainders: [1] } },
        ];
        const doc = makeDoc({ workgroups });
        const zkClient = makeZkClient([
            { data: Buffer.from(JSON.stringify(doc)) },
            { data: Buffer.from(JSON.stringify(
                makeDoc({ workgroups, generation: 4 }))) },
        ]);
        const loader = makeLoader({ workgroupId: 'wg-watch', zkClient });
        loader.load(err => {
            assert.ifError(err);
            const { watcher } = zkClient.getDataCalls[0];
            assert.strictEqual(typeof watcher, 'function');
            watcher({ type: NODE_DATA_CHANGED, path: ZK_PATH });
            setTimeout(() => {
                assert.strictEqual(zkClient.getDataCalls.length, 2);
                assert.strictEqual(
                    typeof zkClient.getDataCalls[1].watcher, 'function');
                assert.deepStrictEqual(loader.getConfig(), doc);
                counterValue(CHANGES_METRIC, { workgroup: 'wg-watch' })
                    .then(value => {
                        assert.strictEqual(value, 1);
                        done();
                    }).catch(done);
            }, 20);
        });
    });

    it('should re-arm through exists when the node was deleted', done => {
        const zkClient = makeZkClient([
            { data: Buffer.from(JSON.stringify(makeDoc())) },
            { err: { name: 'NO_NODE' } },
        ]);
        const loader = makeLoader({ zkClient });
        loader.load(err => {
            assert.ifError(err);
            zkClient.getDataCalls[0].watcher({
                type: NODE_DELETED,
                path: ZK_PATH,
            });
            setTimeout(() => {
                assert.strictEqual(zkClient.existsCalls.length, 1);
                assert.strictEqual(
                    typeof zkClient.existsCalls[0].watcher, 'function');
                done();
            }, 20);
        });
    });

    it('should load when the cache cannot be written', done => {
        const doc = makeDoc();
        const loader = makeLoader({
            workgroupsConfig: {
                zookeeperPath: ZK_PATH,
                cachePath: path.join(tmpDir, 'no', 'such', 'dir', 'wg.json'),
            },
            zkClient: makeZkClient([
                { data: Buffer.from(JSON.stringify(doc)) },
            ]),
        });
        loader.load((err, loaded) => {
            assert.ifError(err);
            assert.strictEqual(loaded.source, 'zookeeper');
            assert(logger.warns.some(entry =>
                entry.msg.includes('cache')));
            done();
        });
    });

    it('should close a client it did not open only through stop', done => {
        const zkClient = makeZkClient([
            { data: Buffer.from(JSON.stringify(makeDoc())) },
        ]);
        const loader = makeLoader({ zkClient });
        loader.load(err => {
            assert.ifError(err);
            loader.stop(() => {
                assert.strictEqual(zkClient.closed, 0);
                done();
            });
        });
    });
});
