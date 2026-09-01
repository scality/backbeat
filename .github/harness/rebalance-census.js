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
const JOB_INDEX = process.env.JOB_INDEX || '0';
const ITERATIONS = parseInt(process.env.ITERATIONS || '4', 10);
const PARTITIONS = parseInt(process.env.PARTITIONS || '5', 10);
const MEMBERS = parseInt(process.env.MEMBERS || '2', 10);
const CHURN_MS = parseInt(process.env.CHURN_MS || '3000', 10);
const TASK_MS = parseInt(process.env.TASK_MS || '400', 10);
const LEDGER_MS = parseInt(process.env.LEDGER_MS || '600', 10);
const DURATION_MS = parseInt(process.env.DURATION_MS || '90000', 10);
const PRODUCE_EVERY_MS = parseInt(process.env.PRODUCE_EVERY_MS || '100', 10);
const PRODUCE_BATCH = parseInt(process.env.PRODUCE_BATCH || '5', 10);
const ADMIN_TIMEOUT_MS = 20000;

const REBALANCE_METRIC = 's3_backbeat_queue_rebalance_total';

const kafkaConf = {
    hosts: HOSTS,
    // the asynchronous offset publish is what widens the window between
    // deciding to un-assign and actually calling it, so keep it on
    backlogMetrics: { zkPath: '/census/kafka-backlog-metrics', intervalS: 1 },
};

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
        this.consumer = new BackbeatConsumer({
            clientId: `census-${name}`,
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

    async close() {
        this.closed = true;
        await new Promise(resolve => this.consumer.close(resolve));
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
    while (Date.now() < deadline) {
        const joiner = new Member(`churn${churnCycles}`, topic, groupId);
        await joiner.start();
        await sleep(CHURN_MS);

        // a base member that consumed nothing over a whole churn cycle is the
        // BB-835 end state: still a group member, owning partitions at the
        // broker, with no local assignment and nothing to trigger recovery
        for (const member of members) {
            const delta = member.consumed - member.consumedAtLastCheck;
            member.consumedAtLastCheck = member.consumed;
            if (delta === 0) {
                stalledCycles++;
            }
        }

        await joiner.close();
        churnCycles++;
        await sleep(CHURN_MS);
    }

    clearInterval(feed);
    for (const member of members) {
        await member.close();
    }
    await new Promise(resolve => producer.close(resolve));
    admin.disconnect();

    const delta = diffCounts(await readRebalanceCounts(), before);
    const deferred = (delta.drained || 0) + (delta.superseded || 0);

    return {
        arm: ARM,
        job: JOB_INDEX,
        iteration: index,
        churnCycles,
        produced,
        consumed: members.reduce((sum, m) => sum + m.consumed, 0),
        // the denominator: a zero superseded count only means something if
        // deferred un-assigns actually happened
        deferredUnassigns: deferred,
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
    console.log(`census arm=${ARM} job=${JOB_INDEX} iterations=${ITERATIONS} ` +
                `partitions=${PARTITIONS} members=${MEMBERS} churn=${CHURN_MS}ms ` +
                `task=${TASK_MS}ms ledger=${LEDGER_MS}ms duration=${DURATION_MS}ms`);
    for (let i = 1; i <= ITERATIONS; i++) {
        try {
            const result = await iteration(i);
            console.log(`RESULT ${JSON.stringify(result)}`);
        } catch (err) {
            console.log(`RESULT ${JSON.stringify({
                arm: ARM, job: JOB_INDEX, iteration: i, error: err.message,
            })}`);
        }
    }
    process.exit(0);
}

main().catch(err => {
    console.error('census failed', err);
    process.exit(1);
});
