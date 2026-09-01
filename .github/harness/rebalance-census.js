/* eslint-disable no-console */
/**
 * BB-835 census: how often does a deferred un-assign get superseded by a
 * later rebalance?
 *
 * The race needs two things at once: a revoke arriving while work is still
 * in flight (so the un-assign is deferred), and a new generation completing
 * before that drain does. This harness manufactures both -- slow tasks that
 * also hold the offset ledger open, and members joining and leaving on a
 * tight cycle -- then counts the outcome from the rebalance metric.
 *
 * Both arms are measured by this same file; only lib/ differs. The counter
 * label is identical on each side: the fixed tree reports 'superseded' when
 * it skips the stale un-assign, the detector tree reports it and then goes
 * ahead anyway, reproducing the stall.
 */
const Kafka = require('node-rdkafka');
const { promisify } = require('util');
const { metrics } = require('arsenal');

const BackbeatProducer = require('../../lib/BackbeatProducer');
const BackbeatConsumer = require('../../lib/BackbeatConsumer');
const { withTopicPrefix } = require('../../lib/util/topic');

const HOSTS = process.env.HOSTS || 'localhost:9092';
const ARM = process.env.ARM || 'unknown';
const SCENARIO = process.env.SCENARIO || 'default';
const JOB_INDEX = process.env.JOB_INDEX || '0';
const ITERATIONS = parseInt(process.env.ITERATIONS || '4', 10);
const PARTITIONS = parseInt(process.env.PARTITIONS || '5', 10);
const MEMBERS = parseInt(process.env.MEMBERS || '1', 10);
// how long the newcomer stays: short, so the partitions come straight back
const JOINER_LIFE_MS = parseInt(process.env.JOINER_LIFE_MS || '1500', 10);
// how long the group is left alone afterwards: long enough for the base
// member to rebuild a full queue and ledger before the next disturbance
const SETTLE_MS = parseInt(process.env.SETTLE_MS || '6000', 10);
// 'shutdown' only: how long after the revoke lands before close() runs.
// Short, so the deferred un-assign is still pending when it does.
const CLOSE_DELAY_MS = parseInt(process.env.CLOSE_DELAY_MS || '400', 10);
const TASK_MS = parseInt(process.env.TASK_MS || '400', 10);
const LEDGER_MS = parseInt(process.env.LEDGER_MS || '600', 10);
const DURATION_MS = parseInt(process.env.DURATION_MS || '90000', 10);
const PRODUCE_EVERY_MS = parseInt(process.env.PRODUCE_EVERY_MS || '100', 10);
const PRODUCE_BATCH = parseInt(process.env.PRODUCE_BATCH || '5', 10);
const ZK = process.env.ZK || 'localhost:2181';
// the untreated arm is the tree where close() can hang outright (BB-833),
// so an unbounded close would make the two arms incomparable
const CLOSE_TIMEOUT_MS = parseInt(process.env.CLOSE_TIMEOUT_MS || '5000', 10);
const ADMIN_TIMEOUT_MS = 20000;

const REBALANCE_METRIC = 's3_backbeat_queue_rebalance_total';

const kafkaConf = {
    hosts: HOSTS,
    // the asynchronous offset publish is what widens the window between
    // deciding to un-assign and actually calling it, so keep it on
    backlogMetrics: { zkPath: '/census/kafka-backlog-metrics', intervalS: 1 },
};
// backlogMetrics is gated on zookeeper: without this the consumer never
// reaches 'ready', because _checkIfReady waits on the metrics client too
const zookeeperConf = { connectionString: ZK };

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

/**
 * Read the rebalance counter, summed per status across every label set.
 *
 * @returns {Promise<object>} status -> cumulative count
 */
async function readRebalanceCounts() {
    const text = await metrics.ZenkoMetrics.asPrometheus();
    const counts = {};
    for (const line of text.split('\n')) {
        if (!line.startsWith(REBALANCE_METRIC)) {
            continue;
        }
        const status = /status="([^"]+)"/.exec(line);
        const value = /\s([0-9.]+)$/.exec(line);
        if (status && value) {
            counts[status[1]] = (counts[status[1]] || 0) + Number(value[1]);
        }
    }
    return counts;
}

/**
 * @param {object} after - counts read at the end
 * @param {object} before - counts read at the start
 * @returns {object} per-status delta, zero entries dropped
 */
function diffCounts(after, before) {
    const out = {};
    const keys = new Set([...Object.keys(after), ...Object.keys(before)]);
    for (const key of keys) {
        const delta = (after[key] || 0) - (before[key] || 0);
        if (delta) {
            out[key] = delta;
        }
    }
    return out;
}

/**
 * @param {object} admin - node-rdkafka AdminClient
 * @param {string} topic - prefixed topic name
 * @returns {Promise<undefined>} resolves once the topic exists
 */
async function createTopic(admin, topic) {
    const { ERR_TOPIC_ALREADY_EXISTS } = Kafka.CODES.ERRORS;
    try {
        await promisify(admin.createTopic).bind(admin)({
            topic,
            num_partitions: PARTITIONS, // eslint-disable-line camelcase
            replication_factor: 1, // eslint-disable-line camelcase
        }, ADMIN_TIMEOUT_MS);
    } catch (err) {
        if (err.code !== ERR_TOPIC_ALREADY_EXISTS) {
            throw err;
        }
    }
}

/**
 * A member of the group. Its worker holds a slot in the processing queue for
 * TASK_MS, then defers the commit for a further LEDGER_MS, so a revoke landing
 * at any point finds both the queue and the ledger non-empty -- the
 * running=5 ledger=5 shape from the BB-835 trace.
 */
class Member {
    constructor(name, topic, groupId) {
        this.name = name;
        this.consumed = 0;
        this.consumedAtLastCheck = 0;
        this.closed = false;
        this.closeTimedOut = false;
        this.consumer = new BackbeatConsumer({
            clientId: `census-${name}`,
            zookeeper: zookeeperConf,
            kafka: kafkaConf,
            groupId,
            topic,
            concurrency: 10,
            queueProcessor: (entry, cb) => {
                this.consumed++;
                setTimeout(() => {
                    cb(null, { committable: false });
                    setTimeout(() => {
                        if (!this.closed) {
                            this.consumer.onEntryCommittable(entry);
                        }
                    }, LEDGER_MS);
                }, TASK_MS);
            },
        });
        this.consumer.on('error', () => {});
    }

    async start() {
        await new Promise(resolve => this.consumer.on('ready', resolve));
        this.consumer.subscribe();
    }

    /**
     * Leave the group without going through close(), which is the code under
     * test: on the untreated tree it wedges, so the newcomer would linger for
     * the whole close bound and depart later on one arm than the other. Churn
     * has to be identical across arms or the comparison is rigged.
     *
     * @returns {undefined}
     */
    leaveFast() {
        this.closed = true;
        for (const op of ['unsubscribe', 'unassign', 'disconnect']) {
            try {
                this.consumer._consumer[op]();
            } catch (e) { // eslint-disable-line no-unused-vars
                // already gone, or never got that far
            }
        }
    }

    async close() {
        this.closed = true;
        let settled = false;
        await Promise.race([
            new Promise(resolve => this.consumer.close(() => {
                settled = true;
                resolve();
            })),
            sleep(CLOSE_TIMEOUT_MS),
        ]);
        if (!settled) {
            this.closeTimedOut = true;
            // close() is wedged, so the member would linger in the group until
            // session.timeout.ms. Force it out so churn keeps its cadence.
            try {
                this.consumer._consumer.disconnect();
            } catch (e) { // eslint-disable-line no-unused-vars
                // already gone
            }
        }
    }
}

/**
 * @param {number} index - iteration number, for the record
 * @returns {Promise<object>} one RESULT row
 */
async function iteration(index) {
    const stamp = `${JOB_INDEX}-${index}-${Date.now()}`;
    const topic = `bb835-census-${stamp}`;
    const groupId = `bb835-census-group-${stamp}`;

    const admin = Kafka.AdminClient.create({
        'client.id': 'census-admin',
        'metadata.broker.list': HOSTS,
    });
    await createTopic(admin, withTopicPrefix(topic));

    const producer = new BackbeatProducer({
        kafka: { hosts: HOSTS }, topic, pollIntervalMs: 100,
    });
    await new Promise(resolve => producer.on('ready', resolve));

    let produced = 0;
    const feed = setInterval(() => {
        const batch = [];
        for (let i = 0; i < PRODUCE_BATCH; i++) {
            batch.push({ key: `k${produced}`, message: `m${produced}` });
            produced++;
        }
        producer.send(batch, () => {});
    }, PRODUCE_EVERY_MS);

    const before = await readRebalanceCounts();

    const members = [];
    for (let i = 0; i < MEMBERS; i++) {
        const member = new Member(`base${i}`, topic, groupId);
        await member.start();
        members.push(member);
    }

    // churn: a joiner arrives, then leaves, over and over. Each transition
    // forces a rebalance, which is what a rolling update does to the group.
    let churnCycles = 0;
    let stalledCycles = 0;
    const deadline = Date.now() + DURATION_MS;

    // In 'shutdown' the member being revoked is also the one going away -- the
    // rollout shape. close() calls unsubscribe(), which answers the revoke the
    // deferred un-assign has not answered yet, letting the rebalance proceed
    // while that un-assign is still outstanding. That is the ordering the
    // reported trace shows: an ASSIGN 132ms after an unanswered revoke, with
    // the stale un-assign landing 13ms after that.
    const closes = [];
    while (SCENARIO === 'shutdown' && Date.now() < deadline) {
        const base = new Member(`base${churnCycles}`, topic, groupId);
        await base.start();
        members.push(base);
        await sleep(SETTLE_MS);

        const joiner = new Member(`churn${churnCycles}`, topic, groupId);
        await joiner.start();
        // the revoke lands here, with the queue and ledger still full
        await sleep(CLOSE_DELAY_MS);

        // not awaited: close() is the thing being raced against the drain
        closes.push(base.close());
        joiner.leaveFast();
        churnCycles++;
        await sleep(CLOSE_TIMEOUT_MS + 1000);
    }
    await Promise.all(closes);

    while (SCENARIO !== 'shutdown' && Date.now() < deadline) {
        // a newcomer revokes the base member's partitions while its queue and
        // ledger are still full, which is what defers the un-assign
        const joiner = new Member(`churn${churnCycles}`, topic, groupId);
        await joiner.start();
        await sleep(JOINER_LIFE_MS);

        // ...and leaves again, so the same partitions are granted straight
        // back: the revoke -> assign pair from the BB-835 trace. This has to be
        // prompt and arm-independent, hence leaveFast rather than close().
        joiner.leaveFast();
        churnCycles++;
        await sleep(SETTLE_MS);

        // checked after the settle: a base member consuming nothing once the
        // group has restabilised is the BB-835 end state -- still a member,
        // owning partitions at the broker, with no local assignment
        for (const member of members) {
            const delta = member.consumed - member.consumedAtLastCheck;
            member.consumedAtLastCheck = member.consumed;
            if (delta === 0) {
                stalledCycles++;
            }
        }
    }


    clearInterval(feed);
    for (const member of members) {
        if (!member.closed) {
            await member.close();
        }
    }
    await new Promise(resolve => producer.close(resolve));
    admin.disconnect();

    const delta = diffCounts(await readRebalanceCounts(), before);
    const deferred = (delta.drained || 0) + (delta.superseded || 0);

    return {
        arm: ARM,
        scenario: SCENARIO,
        job: JOB_INDEX,
        iteration: index,
        churnCycles,
        produced,
        consumed: members.reduce((sum, m) => sum + m.consumed, 0),
        // the denominator: a zero superseded count only means something if
        // deferred un-assigns actually happened
        deferredUnassigns: deferred,
        // base members only: the joiners bypass close() by design
        closeTimeouts: members.filter(m => m.closeTimedOut).length,
        superseded: delta.superseded || 0,
        drained: delta.drained || 0,
        idle: delta.idle || 0,
        timeout: delta.timeout || 0,
        stalledCycles,
    };
}

/**
 * @returns {Promise<undefined>} resolves when every iteration has reported
 */
async function main() {
    console.log(`census arm=${ARM} scenario=${SCENARIO} job=${JOB_INDEX} iterations=${ITERATIONS} ` +
                `partitions=${PARTITIONS} members=${MEMBERS} ` +
                `joinerLife=${JOINER_LIFE_MS}ms settle=${SETTLE_MS}ms ` +
                `task=${TASK_MS}ms ledger=${LEDGER_MS}ms duration=${DURATION_MS}ms`);
    for (let i = 1; i <= ITERATIONS; i++) {
        try {
            const result = await iteration(i);
            console.log(`RESULT ${JSON.stringify(result)}`);
        } catch (err) {
            console.log(`RESULT ${JSON.stringify({
                arm: ARM, scenario: SCENARIO, job: JOB_INDEX,
                iteration: i, error: err.message,
            })}`);
        }
    }
    // process.exit() runs librdkafka's destructors, which block on the clients
    // a wedged close() left behind -- the untreated arm would never exit. Flush
    // stdout (a pipe in CI, so the write is async) then kill outright.
    process.stdout.write('', () => process.kill(process.pid, 'SIGKILL'));
}

main().catch(err => {
    console.error('census failed', err);
    process.exit(1);
});
