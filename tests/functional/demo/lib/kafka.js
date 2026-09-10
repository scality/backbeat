'use strict';

/**
 * Everything the suite asks kafka, through the CLI inside the broker
 * container. No new client dependency, and it is the same view an operator
 * gets from a terminal during the demo.
 *
 * Container resolution never matches another rig on the machine: the compose
 * project's own name first, then a demo-named container, and an explicit
 * deny list for the older notification rig, the kerberos spike and the
 * unrelated mongo.
 */

const fs = require('fs');
const env = require('./env');
const { run } = require('./sh');

const DENY = /^(ft-|bnaaskrb-|f9-mongo$|bnaas-mongo$|wg-mongo$)/;
let KAFKA_CONTAINER = null;
let KAFKA_BIN = null;
let ZK_CONTAINER = null;

function running() {
    return run('docker', ['ps', '--format', '{{.Names}}']).out.split('\n')
        .filter(Boolean);
}

function resolve(service, explicit, port, extraDeny) {
    if (explicit) {
        return explicit;
    }
    const names = running();
    const own = `${env.PROJECT}-${service}-1`;
    if (names.includes(own)) {
        return own;
    }
    const byName = names.filter(n => !DENY.test(n)
        && /demo/i.test(n) && n.toLowerCase().includes(service)
        && !(extraDeny && extraDeny.test(n)));
    if (byName.length) {
        return byName[0];
    }
    const byPort = run('docker', ['ps', '--format', '{{.Names}}\t{{.Ports}}']).out
        .split('\n').filter(l => l.includes(`:${port}->`))
        .map(l => l.split('\t')[0]).filter(n => !DENY.test(n));
    return byPort.length ? byPort[0] : null;
}

function kafkaContainer() {
    if (!KAFKA_CONTAINER) {
        KAFKA_CONTAINER = resolve('kafka', env.knob('KAFKA_CONTAINER'),
            env.KAFKA_PORT, /kafka-ui|exporter|krb/);
        if (!KAFKA_CONTAINER) {
            throw new Error('no demo kafka container is running. Bring the '
                + 'stack up (demo/bin/stack-up.sh) or set KAFKA_CONTAINER.');
        }
    }
    return KAFKA_CONTAINER;
}

function zkContainer() {
    if (!ZK_CONTAINER) {
        ZK_CONTAINER = resolve('zookeeper', env.knob('ZK_CONTAINER'),
            env.ZK_PORT, /krb/);
        if (!ZK_CONTAINER) {
            throw new Error('no demo zookeeper container is running.');
        }
    }
    return ZK_CONTAINER;
}

function kafkaBin() {
    if (!KAFKA_BIN) {
        const dirs = ['/opt/kafka/bin', '/opt/bitnami/kafka/bin'];
        KAFKA_BIN = dirs.find(d => run('docker',
            ['exec', kafkaContainer(), 'test', '-x', `${d}/kafka-topics.sh`]).ok);
        if (!KAFKA_BIN) {
            throw new Error(`cannot find the kafka CLI inside ${kafkaContainer()}`);
        }
    }
    return KAFKA_BIN;
}

// the broker's host-facing listener, as seen from inside the container
function bootstrap() {
    return `localhost:${env.KAFKA_PORT}`;
}

function cli(tool, args, opts) {
    return run('docker', ['exec', kafkaContainer(), `${kafkaBin()}/${tool}`]
        .concat(args), opts);
}

function tool(tool_, args, opts) {
    return cli(tool_, ['--bootstrap-server', bootstrap()].concat(args), opts);
}

function topics() {
    return tool('kafka-topics.sh', ['--list']).out.split('\n')
        .map(s => s.trim()).filter(Boolean);
}

function topicExists(name) {
    return topics().includes(name);
}

/**
 * @param {String} name - topic
 * @return {Object} partition number to end offset
 */
function heads(name) {
    const out = {};
    tool('kafka-get-offsets.sh', ['--topic', name]).out.split('\n')
        .forEach(l => {
            const m = /^(.+):(\d+):(-?\d+)$/.exec(l.trim());
            if (m && m[1] === name) {
                out[Number(m[2])] = Number(m[3]);
            }
        });
    return out;
}

function headTotal(name) {
    return Object.values(heads(name)).reduce((a, b) => a + b, 0);
}

function head(name, partition) {
    const h = heads(name);
    return h[partition === undefined ? 0 : partition];
}

/**
 * @param {String} name - topic
 * @return {Number} partition count, 0 when the topic does not exist
 */
function partitionCount(name) {
    const out = tool('kafka-topics.sh', ['--describe', '--topic', name]).out;
    const m = /PartitionCount:\s*(\d+)/.exec(out);
    return m ? Number(m[1]) : 0;
}

/**
 * Add partitions to an existing topic. Kafka can grow a topic, never shrink
 * one, and growing it moves keys to different partitions, so this is only
 * for fixing a topic that was auto-created with the broker default.
 *
 * @param {String} name - topic
 * @param {Number} partitions - wanted count
 * @return {Object} the CLI result
 */
function alterPartitions(name, partitions) {
    return tool('kafka-topics.sh',
        ['--alter', '--topic', name, '--partitions', String(partitions)]);
}

function groups() {
    return tool('kafka-consumer-groups.sh', ['--list']).out.split('\n')
        .map(s => s.trim()).filter(Boolean);
}

function describe(group) {
    return tool('kafka-consumer-groups.sh', ['--describe', '--group', group]).out;
}

/**
 * The topic a group of this demo consumes, from its id.
 *
 * @param {String} group - consumer group
 * @return {String|null} topic name
 */
function topicOfGroup(group) {
    if (group.startsWith(env.DELIVERY_GROUP)) {
        return env.DELIVERY_TOPIC;
    }
    if (group.startsWith(env.LEGACY_GROUP_PREFIX)) {
        return env.INTERNAL_TOPIC;
    }
    return null;
}

/**
 * A consumer group holds committed offsets for every topic it ever consumed
 * and --describe prints them all, so a group id reused across topics has a
 * total lag that never reaches zero and reads exactly like a wedge. Every
 * figure here is per topic.
 *
 * @param {String} group - consumer group
 * @param {String} [topic] - restrict to this topic, else infer from the id
 * @return {Object} { lag, committed, unknown, partitions, members, rows }
 */
function groupState(group, topic) {
    const want = topic || topicOfGroup(group);
    const state = { lag: 0, committed: 0, unknown: 0, partitions: 0,
        members: 0, rows: [] };
    describe(group).split('\n').forEach(l => {
        const f = l.trim().split(/\s+/);
        if (f.length < 6 || f[0] === 'GROUP') {
            return;
        }
        if (want && f[1] !== want) {
            return;
        }
        const committed = f[3];
        const end = f[4];
        const lag = f[5];
        state.partitions += 1;
        state.rows.push({ topic: f[1], partition: Number(f[2]),
            committed, end, lag, member: f[6] });
        if (/^\d+$/.test(committed)) {
            state.committed += Number(committed);
        } else if (/^\d+$/.test(end) && Number(end) > 0) {
            // A partition with records but no committed offset is the wedge
            // signature. A partition that is simply empty is not: one
            // destination is one delivery key is one partition, so the
            // partitions no destination hashes to stay empty forever and
            // never get a committed offset.
            state.unknown += 1;
        }
        if (/^\d+$/.test(lag)) {
            state.lag += Number(lag);
        }
        if (f[6] && f[6] !== '-') {
            state.members += 1;
        }
    });
    return state;
}

function groupLag(group, topic) {
    return groupState(group, topic).lag;
}

/**
 * Wait until no group of this demo has a live member.
 *
 * A killed consumer keeps its group membership until its session expires,
 * which is 45 seconds by default, and the cutover tool refuses to pre-seed a
 * group that still has members: "already has members, stop the workers of
 * that generation before pre-seeding it". That refusal is correct, so a run
 * that has just killed workers has to wait it out.
 *
 * @param {Number} [timeoutMs] - how long to wait
 * @return {Array} the groups that still had members when it gave up
 */
function waitForNoMembers(timeoutMs) {
    const deadline = Date.now() + (timeoutMs || 90000);
    const mine = () => groups().filter(g => g === env.DELIVERY_GROUP
        || g.startsWith(`${env.DELIVERY_GROUP}-`)
        || g.startsWith(`${env.LEGACY_GROUP_PREFIX}-`));
    let held = [];
    while (Date.now() < deadline) {
        held = mine().filter(g => groupState(g, null).members > 0);
        if (!held.length) {
            return [];
        }
        require('./sh').sleepSync(5000);
    }
    return held;
}

function createTopic(name, partitions, extra) {
    const args = ['--create', '--if-not-exists', '--topic', name,
        '--partitions', String(partitions), '--replication-factor', '1'];
    return tool('kafka-topics.sh', args.concat(extra || []));
}

function deleteTopic(name) {
    return tool('kafka-topics.sh', ['--delete', '--topic', name]);
}

function deleteGroup(group) {
    return tool('kafka-consumer-groups.sh', ['--delete', '--group', group]);
}

function deleteOffsets(group, topic) {
    return tool('kafka-consumer-groups.sh',
        ['--delete-offsets', '--group', group, '--topic', topic]);
}

/**
 * Give a consumer group a committed offset, at the partition's current end,
 * on every partition of a topic it has none on, so that a fresh group never
 * falls back to auto.offset.reset.
 *
 * The legacy queue processor builds its consumer with no fromOffset, so
 * librdkafka's default `latest` applies to any partition the group has never
 * committed on, and its first-join revoke leaves a window of about forty
 * seconds with no assignment: whatever is published in that window is skipped
 * when the assignment comes back, and a partition that then receives nothing
 * more never gets a committed offset at all, which reads exactly like the
 * wedge. A group that has run in production for years has a committed offset
 * everywhere; a group this suite just deleted has none. This puts it in the
 * production state before the process starts. It touches only partitions
 * with no committed offset, and it needs the group to have no members, which
 * is true before the processor is started. A group that does not exist yet
 * is created by the reset.
 *
 * @param {String} group - consumer group
 * @param {String} topic - topic
 * @return {Array} the partitions that were seeded, empty if none needed it
 */
function seedGroupAtHead(group, topic) {
    const ends = heads(topic);
    const committed = new Set(groupState(group, topic).rows
        .filter(r => /^\d+$/.test(r.committed)).map(r => r.partition));
    const missing = Object.keys(ends).map(Number).filter(p => !committed.has(p));
    if (!missing.length) {
        return [];
    }
    tool('kafka-consumer-groups.sh', ['--reset-offsets', '--group', group,
        '--topic', `${topic}:${missing.join(',')}`, '--to-latest', '--execute']);
    return missing;
}

/**
 * Wait until every partition of a topic reports a leader, three consecutive
 * looks. A consumer that joins before that can wedge, which is the design/06
 * trigger, so this is not optional politeness.
 *
 * @param {String} name - topic
 * @param {Number} partitions - expected partition count
 * @return {Boolean} whether leadership was confirmed
 */
function waitForLeaders(name, partitions) {
    let ok = 0;
    for (let i = 0; i < 60 && ok < 3; i++) {
        const out = tool('kafka-topics.sh', ['--describe', '--topic', name]).out;
        const leaders = (out.match(/Leader: \d+/g) || []).length;
        ok = leaders >= partitions ? ok + 1 : 0;
        if (ok < 3) {
            require('./sh').sleepSync(500);
        }
    }
    return ok >= 3;
}

/**
 * Recreate a topic so a run starts from offset 0 with nobody's leftovers.
 * The stack's own validation records, and any earlier act's, are exactly
 * what this removes. Falls back to keeping the topic if the broker refuses
 * to delete it, and says so.
 *
 * @param {String} name - topic
 * @param {Number} partitions - partition count
 * @param {Array} [extra] - extra kafka-topics arguments
 * @return {String} 'recreated', 'created' or 'kept'
 */
function recreateTopic(name, partitions, extra) {
    let outcome = 'created';
    if (topicExists(name)) {
        // An empty topic needs no recreating, and every kafka CLI call is a
        // JVM start: skipping the ones that would change nothing takes the
        // setup of a run from minutes to seconds.
        if (headTotal(name) === 0) {
            waitForLeaders(name, partitions);
            return 'kept, already empty';
        }
        deleteTopic(name);
        let gone = false;
        for (let i = 0; i < 60 && !gone; i++) {
            gone = !topicExists(name);
            if (!gone) {
                require('./sh').sleepSync(500);
            }
        }
        outcome = gone ? 'recreated' : 'kept';
    }
    createTopic(name, partitions, extra);
    waitForLeaders(name, partitions);
    return outcome;
}

/**
 * Dump one partition slice of a topic to a file, deterministically, from a
 * recorded start offset. No consumer group is involved, so this cannot
 * disturb anybody's offsets.
 *
 * @param {String} topic - topic
 * @param {Number} start - start offset
 * @param {String} file - output path
 * @param {Number} [partition] - partition, default 0
 * @param {Number} [timeoutMs] - console consumer timeout
 * @return {Number} lines written
 */
function dump(topic, start, file, partition, timeoutMs) {
    const p = partition === undefined ? 0 : partition;
    const h = head(topic, p);
    if (h === undefined) {
        fs.writeFileSync(file, '');
        return 0;
    }
    const n = h - start;
    if (n <= 0) {
        fs.writeFileSync(file, '');
        return 0;
    }
    const r = cli('kafka-console-consumer.sh', [
        '--bootstrap-server', bootstrap(),
        '--topic', topic,
        '--partition', String(p),
        '--offset', String(start),
        '--max-messages', String(n),
        '--timeout-ms', String(timeoutMs || 30000),
        '--property', 'print.key=true',
        '--property', 'print.timestamp=true',
        '--property', 'print.partition=true',
        '--property', 'print.offset=true',
    ], { timeout: (timeoutMs || 30000) + 60000 });
    fs.writeFileSync(file, `${r.out}\n`);
    return r.out ? r.out.split('\n').length : 0;
}


/**
 * Recreate several topics at once, which matters because every kafka CLI
 * call is a JVM start: deleting nine topics one at a time, each with its own
 * wait loop, is minutes, and the demo starts with this.
 *
 * A topic that exists and is empty is kept: recreating it would change
 * nothing.
 *
 * @param {Array} wanted - [name, partitions] pairs
 * @return {Object} name to outcome
 */
function recreateTopics(wanted) {
    const report = {};
    const existing = new Set(topics());
    const toDelete = [];
    wanted.forEach(([name]) => {
        if (!existing.has(name)) {
            report[name] = 'created';
        } else if (headTotal(name) === 0) {
            report[name] = 'kept, already empty';
        } else {
            report[name] = 'recreated';
            toDelete.push(name);
        }
    });
    toDelete.forEach(name => deleteTopic(name));
    if (toDelete.length) {
        // one list per poll for every topic at once, rather than one call per
        // topic per poll
        for (let i = 0; i < 40; i++) {
            const now = new Set(topics());
            if (!toDelete.some(n => now.has(n))) {
                break;
            }
            require('./sh').sleepSync(1000);
        }
        const left = new Set(topics());
        toDelete.filter(n => left.has(n)).forEach(n => {
            report[n] = 'kept, the broker would not delete it';
        });
    }
    wanted.forEach(([name, partitions]) => {
        if (report[name] === 'created' || report[name] === 'recreated') {
            createTopic(name, partitions);
        }
    });
    // A topic can come back with the broker default of one partition:
    // auto.create.topics.enable is on, so any producer that touches the name
    // between the delete and the create wins the race. Grow it back rather
    // than waiting forever for leaders that will never appear.
    wanted.forEach(([name, partitions]) => {
        const have = partitionCount(name);
        if (have > 0 && have < partitions) {
            alterPartitions(name, partitions);
            report[name] = `${report[name]}, grown from ${have} to ${partitions} `
                + 'partitions (it had been auto-created)';
        }
    });
    // One --describe for every topic at once, three times, rather than three
    // per topic: leadership has to be confirmed before anything consumes,
    // and on this broker each CLI call is a JVM start.
    let ok = 0;
    for (let i = 0; i < 60 && ok < 3; i++) {
        const out = tool('kafka-topics.sh', ['--describe']).out;
        const perTopic = {};
        out.split('\n').forEach(l => {
            const m = /^\s*Topic:\s*(\S+)\s+Partition:\s*\d+.*Leader:\s*(\d+)/.exec(l);
            if (m) {
                perTopic[m[1]] = (perTopic[m[1]] || 0) + 1;
            }
        });
        const missing = wanted.filter(([name, partitions]) =>
            (perTopic[name] || 0) < partitions);
        ok = missing.length ? 0 : ok + 1;
        if (ok < 3) {
            require('./sh').sleepSync(1000);
        }
    }
    if (ok < 3) {
        const short = wanted.map(([name, partitions]) =>
            `${name} wants ${partitions}, has ${partitionCount(name)}`);
        report._leaders = `not confirmed three times: ${short.join('; ')}`;
    }
    return report;
}

module.exports = {
    waitForNoMembers,
    partitionCount,
    alterPartitions,
    recreateTopics,
    topicOfGroup,
    kafkaContainer,
    zkContainer,
    kafkaBin,
    bootstrap,
    cli,
    tool,
    topics,
    topicExists,
    heads,
    headTotal,
    head,
    groups,
    describe,
    groupState,
    groupLag,
    createTopic,
    deleteTopic,
    deleteGroup,
    deleteOffsets,
    seedGroupAtHead,
    waitForLeaders,
    recreateTopic,
    dump,
};
