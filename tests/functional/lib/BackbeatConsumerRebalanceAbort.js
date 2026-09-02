const assert = require('assert');
const { promisify } = require('util');
const kafka = require('node-rdkafka');
const sinon = require('sinon');

const BackbeatProducer = require('../../../lib/BackbeatProducer');
const BackbeatConsumer = require('../../../lib/BackbeatConsumer');
const { withTopicPrefix } = require('../../../lib/util/topic');
const { unassignStatus } = require('../../../lib/constants');

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
// still unanswered, so a fresh assignment can be granted for a generation
// that has already moved past the work still draining.
//
// The consumer must then stop consuming, let the drain commit what it has
// and leave the group, rather than carry on and reprocess those entries
// alongside their own redelivery.
//
// The trigger is manufactured with partition-count bumps, observed by an
// application getMetadata() call. If librdkafka ever stops rejoining while
// a revoke is unanswered, the setup fails on 'no assign delivered' rather
// than on the assertions that follow.
describe('BackbeatConsumer aborts a superseded rebalance', function testSuite() {
    this.timeout(120000);

    let admin;
    let producer;
    let consumer;
    let rawTopic;
    let fullTopic;
    let groupId;
    let taskStarted;
    let taskGate;
    let unassigns;
    let processed;

    function queueProcessor(message, cb) {
        const value = message.value.toString();
        processed.push(value);
        if (value === 'hold') {
            taskStarted.resolve();
            taskGate.promise.then(() => cb());
            return;
        }
        process.nextTick(cb);
    }

    beforeEach(() => {
        rawTopic = `backbeat-abort-spec-${Date.now()}`;
        fullTopic = withTopicPrefix(rawTopic);
        groupId = `abort-group-${Math.random()}`;
        taskStarted = deferred();
        taskGate = deferred();
        unassigns = [];
        processed = [];
        admin = kafka.AdminClient.create({
            'client.id': 'abort-spec-admin',
            'metadata.broker.list': kafkaConf.hosts,
        });
    });

    afterEach(async function teardown() {
        this.timeout(40000);
        sinon.restore();
        taskGate.resolve();
        if (consumer) {
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

    async function start() {
        await promisify(admin.createTopic.bind(admin))({
            topic: fullTopic,
            /* eslint-disable camelcase */
            num_partitions: 1,
            replication_factor: 1,
            /* eslint-enable camelcase */
        }, 15000);

        consumer = new InstrumentedConsumer({
            clientId: 'BackbeatConsumer-abort',
            zookeeper: zookeeperConf,
            kafka: kafkaConf,
            groupId,
            topic: rawTopic,
            queueProcessor,
            fromOffset: 'earliest',
            // rebalance callbacks are delivered by the consume poll,
            // which stops while the pipeline is full: a free slot must
            // remain next to the held task for the revoke to reach us
            concurrency: 2,
        });
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

    const bump = partitions =>
        promisify(admin.createPartitions.bind(admin))(
            fullTopic, partitions, 15000);

    const sawRevoke = () =>
        (consumer.rebalanceLog || []).includes(ERR__REVOKE_PARTITIONS);

    const sawAssignAfterRevoke = () => {
        const log = consumer.rebalanceLog || [];
        const revokeIdx = log.indexOf(ERR__REVOKE_PARTITIONS);
        return revokeIdx !== -1 &&
            log.slice(revokeIdx + 1).includes(ERR__ASSIGN_PARTITIONS);
    };

    // wait for cond, issuing the same full-cluster metadata request
    // BackbeatConsumer itself makes, which librdkafka answers with a
    // consumer-group subscription re-check
    async function driveUntil(cond, what) {
        for (let i = 0; i < 30; i++) {
            if (cond()) {
                return;
            }
            await promisify(consumer.getMetadata.bind(consumer))(
                { allTopics: true, timeout: 10000 }).catch(() => {});
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

    // read the group's committed offset without joining the group:
    // assign() alone does not make this client a member
    async function committedOffset(partition) {
        const probe = new kafka.KafkaConsumer({
            'metadata.broker.list': kafkaConf.hosts,
            'group.id': groupId,
            'enable.auto.commit': false,
        }, {});
        try {
            await new Promise((resolve, reject) => {
                probe.connect({ timeout: 10000 }, err =>
                    (err ? reject(err) : resolve()));
            });
            probe.assign([{ topic: fullTopic, partition }]);
            const toppars = await promisify(
                probe.committed.bind(probe))([{ topic: fullTopic, partition }],
                10000);
            return toppars[0].offset;
        } finally {
            probe.disconnect();
        }
    }

    it('leaves the group instead of consuming a generation it cannot '
    + 'commit for', async () => {
        await start();

        // hold one task in flight so the next revoke defers its un-assign
        await send([{ key: 'k-hold', message: 'hold' }]);
        await taskStarted.promise;

        // first bump: the next group-updating metadata response delivers
        // a normal revoke, which parks behind the held task
        await bump(2);
        await driveUntil(sawRevoke,
            'no revoke delivered after the first partition bump');
        await sleep(300);
        assert.deepStrictEqual(unassigns, [],
            'the un-assign should be deferred while a task is in flight');

        // second bump while the revoke is unanswered: librdkafka rejoins
        // and grants a fresh assignment before the drain completes
        await bump(3);
        await driveUntil(sawAssignAfterRevoke,
            'no assign delivered while the revoke was parked: the '
            + 'trigger is not reproducible on this librdkafka version');
        assert.strictEqual(consumer._abortingRebalance, true,
            'the superseded grant should have started an abort');

        // the held task completes, so the drain commits and un-assigns
        taskGate.resolve();
        await waitUntil(() => unassigns.length > 0, 20000,
            'the drain did not complete after the held task finished');
        // the drain ran to completion first; the departure that follows
        // emits its own shutdown un-assign
        assert.strictEqual(unassigns[0], unassignStatus.DRAINED,
            `expected the drain to complete, got ${unassigns.join()}`);

        // ... and the consumer then leaves and reports unhealthy, so the
        // liveness probe restarts it
        await waitUntil(() => !consumer.isReady(), 20000,
            'the consumer stayed ready after aborting the rebalance');

        // the drained work was committed, so whoever takes the partition
        // over does not reprocess it
        const committed = await committedOffset(0);
        assert.strictEqual(committed, 1,
            `expected the held entry to be committed, got ${committed}`);

        // and nothing was consumed twice, nor picked up after leaving
        await send([{ key: 'after', message: 'after-leaving' }]);
        await sleep(3000);
        assert.deepStrictEqual(processed, ['hold'],
            `expected only the held entry, got ${JSON.stringify(processed)}`);
    });
});
