const assert = require('assert');
const { promisify } = require('util');
const kafka = require('node-rdkafka');
const sinon = require('sinon');

const BackbeatProducer = require('../../../lib/BackbeatProducer');
const BackbeatConsumer = require('../../../lib/BackbeatConsumer');
const { withTopicPrefix } = require('../../../lib/util/topic');

const zookeeperConf = { connectionString: 'localhost:2181' };
const kafkaConf = { hosts: 'localhost:9092' };

const { ERR__ASSIGN_PARTITIONS, ERR__REVOKE_PARTITIONS } = kafka.CODES.ERRORS;

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

function deferred() {
    let resolve;
    const promise = new Promise(res => {
        resolve = res;
    });
    return { promise, resolve };
}

class InstrumentedConsumer extends BackbeatConsumer {
    _onRebalance(err, assignment) {
        if (!this.rebalanceLog) {
            this.rebalanceLog = [];
        }
        this.rebalanceLog.push(err.code);
        super._onRebalance(err, assignment);
    }
}

// When a revoke arrives with work in flight, the un-assign is deferred
// until the drain completes. librdkafka however answers a subscribed-topic
// metadata change with an immediate rejoin even while that revoke is
// still unanswered, so a fresh assignment can be granted and applied
// before the drain completes -- and the deferred un-assign then wipes it,
// leaving a live group member that owns partitions at the broker but
// consumes nothing, with no rebalance ever coming to fix it.
//
// Both tests manufacture that interleaving with partition-count bumps,
// differing only in which metadata request observes the change: an
// application getMetadata() call, or librdkafka's own periodic refresh,
// which runs regardless of what the application does. If librdkafka ever
// stops rejoining while a revoke is unanswered, the setup fails on 'no
// assign delivered' rather than on the final consumption assertion.
describe('BackbeatConsumer superseded deferred un-assign', function testSuite() {
    this.timeout(120000);

    let admin;
    let producer;
    let consumer;
    let rawTopic;
    let fullTopic;
    let taskStarted;
    let taskGate;
    let unassigns;
    let consumedAfterRelease;

    function queueProcessor(message, cb) {
        const value = message.value.toString();
        if (value === 'hold') {
            taskStarted.resolve();
            taskGate.promise.then(() => cb());
            return;
        }
        consumedAfterRelease.push(value);
        process.nextTick(cb);
    }

    beforeEach(() => {
        rawTopic = `backbeat-supersede-spec-${Date.now()}`;
        fullTopic = withTopicPrefix(rawTopic);
        taskStarted = deferred();
        taskGate = deferred();
        unassigns = [];
        consumedAfterRelease = [];
        admin = kafka.AdminClient.create({
            'client.id': 'supersede-spec-admin',
            'metadata.broker.list': kafkaConf.hosts,
        });
    });

    afterEach(async function teardown() {
        this.timeout(40000);
        sinon.restore();
        taskGate.resolve();
        if (consumer) {
            // a consumer stalled by the bug may misbehave on close();
            // never let teardown mask the test outcome
            await Promise.race([
                promisify(consumer.close.bind(consumer))(),
                sleep(15000),
            ]);
        }
        if (producer) {
            await promisify(producer.close.bind(producer))();
        }
        try {
            admin.disconnect();
        } catch {
            // already disconnected
        }
        consumer = null;
        producer = null;
    });

    async function start({ refreshIntervalMs } = {}) {
        await promisify(admin.createTopic.bind(admin))({
            topic: fullTopic,
            /* eslint-disable camelcase */
            num_partitions: 1,
            replication_factor: 1,
            /* eslint-enable camelcase */
        }, 15000);

        if (refreshIntervalMs) {
            // BackbeatConsumer has no rdkafka config passthrough, so
            // shrink the periodic refresh by wrapping the constructor
            const RealKafkaConsumer = kafka.KafkaConsumer;
            sinon.replace(kafka, 'KafkaConsumer',
                // an arrow function cannot be invoked with `new`
                // eslint-disable-next-line prefer-arrow-callback
                function kafkaConsumerWithFastRefresh(conf, topicConf) {
                    return new RealKafkaConsumer({
                        ...conf,
                        'topic.metadata.refresh.interval.ms': refreshIntervalMs,
                    }, topicConf);
                });
        }
        consumer = new InstrumentedConsumer({
            clientId: 'BackbeatConsumer-supersede',
            zookeeper: zookeeperConf,
            kafka: kafkaConf,
            groupId: `supersede-group-${Math.random()}`,
            topic: rawTopic,
            queueProcessor,
            fromOffset: 'earliest',
            // rebalance callbacks are delivered by the consume poll,
            // which stops while the pipeline is full: a free slot must
            // remain next to the held task for the revoke to reach us
            concurrency: 2,
        });
        sinon.restore();
        consumer.on('unassign', status => unassigns.push(status));

        producer = new BackbeatProducer({
            kafka: kafkaConf,
            topic: rawTopic,
            pollIntervalMs: 100,
        });
        await Promise.all([
            new Promise(resolve => consumer.on('ready', resolve)),
            new Promise(resolve => producer.on('ready', resolve)),
        ]);
        consumer.subscribe();
    }

    const send = messages =>
        promisify(producer.send.bind(producer))(messages);

    const sawRevoke = () =>
        (consumer.rebalanceLog || []).includes(ERR__REVOKE_PARTITIONS);

    const sawAssignAfterRevoke = () => {
        const log = consumer.rebalanceLog || [];
        const revokeIdx = log.indexOf(ERR__REVOKE_PARTITIONS);
        return revokeIdx !== -1 &&
            log.slice(revokeIdx + 1).includes(ERR__ASSIGN_PARTITIONS);
    };

    // wait for cond; when poking, issue the same full-cluster metadata
    // request BackbeatConsumer itself makes, which librdkafka answers
    // with a consumer-group subscription re-check
    async function driveUntil(cond, poke, what) {
        for (let i = 0; i < 30; i++) {
            if (cond()) {
                return;
            }
            if (poke) {
                await promisify(consumer.getMetadata.bind(consumer))(
                    { allTopics: true, timeout: 10000 }).catch(() => {});
            }
            await sleep(500);
        }
        assert.fail(what);
    }

    async function waitUntil(cond, timeoutMs, what) {
        const deadline = Date.now() + timeoutMs;
        while (!cond()) {
            if (Date.now() > deadline) {
                assert.fail(typeof what === 'function' ? what() : what);
            }
            await sleep(100);
        }
    }

    const bump = (partitions =>
        promisify(admin.createPartitions.bind(admin))(
            fullTopic, partitions, 15000));

    async function runScenario({ poke }) {
        // hold one task in flight so the next revoke defers its un-assign
        await send([{ key: 'k-hold', message: 'hold' }]);
        await taskStarted.promise;

        // first partition bump: the next group-updating metadata
        // response delivers a normal revoke
        await bump(2);
        await driveUntil(sawRevoke, poke,
            'no revoke delivered after the first partition bump');

        await sleep(300);
        assert.strictEqual(unassigns.length, 0,
            'the un-assign should be deferred while a task is in flight');
        assert(consumer._drainProcessQueueTimeout,
            'the drain watchdog should be armed while the drain is pending');

        // second bump while the revoke is unanswered: librdkafka rejoins
        // and grants a fresh assignment before the drain completes
        await bump(3);
        await driveUntil(sawAssignAfterRevoke, poke,
            'no assign delivered while the revoke was parked: the ' +
            'trigger is not reproducible on this librdkafka version');

        assert.strictEqual(unassigns.length, 0,
            'the drain must still be pending when the fresh assignment lands');

        // release the held task: the drain completes and the deferred
        // un-assign fires -- it must not touch the superseding assignment
        taskGate.resolve();
        await waitUntil(() => consumer._processingQueue.idle(), 10000,
            'processing queue drain');
        await sleep(1500);

        await send([
            { key: 'a', message: 'after-1' },
            { key: 'b', message: 'after-2' },
            { key: 'c', message: 'after-3' },
        ]);
        await waitUntil(() => consumedAfterRelease.length >= 3, 20000,
            () => 'consumer stalled after the deferred un-assign: ' +
                `local assignment=[${consumer._consumer.assignments()
                    .map(a => a.partition)}], ` +
                `isReady=${consumer.isReady()}, ` +
                `consumed=${JSON.stringify(consumedAfterRelease)}`);
    }

    it('keeps the assignment granted while draining, when an application ' +
    'metadata call observes the topic change', async () => {
        await start();
        await runScenario({ poke: true });
    });

    it('keeps the assignment granted while draining, when the periodic ' +
    'metadata refresh observes the topic change', async () => {
        await start({ refreshIntervalMs: 1000 });
        await runScenario({ poke: false });
    });
});
