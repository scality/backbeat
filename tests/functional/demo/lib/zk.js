'use strict';

/**
 * What ZooKeeper holds, through zkCli inside the container, so the terminal
 * shows the same thing as the ZooKeeper browser next to it.
 *
 * Three nodes matter to the demo: the populator's log offset, which is its
 * checkpoint, the delivery pool's workgroups document, which carries the
 * generation and the slices, and the per-destination watermarks the seed
 * tool writes for a generation under it.
 */

const env = require('./env');
const kafka = require('./kafka');
const { run } = require('./sh');

const LOG_LINE = /^(\d{4}-\d{2}-\d{2} |WATCHER|WatchedEvent|Connecting to |JLine support|\[zk:|$)/;

function cli(args) {
    const container = kafka.zkContainer();
    const path = run('docker', ['exec', container, 'sh', '-c',
        'ls -d /apache-zookeeper-*/bin/zkCli.sh /opt/zookeeper/bin/zkCli.sh '
        + '2>/dev/null | head -1']).out.trim();
    const bin = path || 'zkCli.sh';
    return run('docker', ['exec', container, bin, '-server', 'localhost:2181']
        .concat(args), { timeout: 60000 });
}

/**
 * @param {String} node - znode path
 * @return {String|null} the data, or null when the node has none
 */
function get(node) {
    const r = cli(['get', node]);
    const lines = r.out.split('\n').map(l => l.trim())
        .filter(l => l && !LOG_LINE.test(l));
    if (!lines.length) {
        return null;
    }
    return lines[lines.length - 1];
}

/**
 * @param {String} node - znode path
 * @return {Object|null} the parsed document, or null
 */
function getJson(node) {
    const raw = get(node);
    if (!raw) {
        return null;
    }
    try {
        return JSON.parse(raw);
    } catch {
        return null;
    }
}

function ls(node) {
    const r = cli(['ls', '-R', node]);
    return r.out.split('\n').map(l => l.trim())
        .filter(l => l.startsWith(node));
}

function deleteAll(node) {
    return cli(['deleteall', node]).ok;
}

function workgroupsDoc() {
    return getJson(env.ZK_WORKGROUPS_PATH);
}

/**
 * Build a workgroups document, the way an Ansible run would render it from
 * its configuration groups: a generation, a hashmod rule per auto workgroup
 * and a static rule per pin. No barriers and no previous groups: a
 * generation change is a container swap with the new groups seeded from the
 * old ones, so the document carries only the layout.
 *
 * @param {Object} spec - { generation, modulo, workgroups: { id: [remainders] },
 *   statics: { id: [destinationIds] }, topic }
 * @return {Object} the document, validated by the repository's own schema
 */
function buildWorkgroupsDoc(spec) {
     
    const wg = require(`${env.BACKBEAT_DIR}/extensions/notification/utils/workgroups`);
    const doc = {
        configVersion: 1,
        generation: spec.generation,
        topic: spec.topic || env.POOL_TOPIC,
        updatedAt: new Date().toISOString(),
        workgroups: [],
    };
    Object.entries(spec.workgroups || {}).forEach(([id, remainders]) => {
        doc.workgroups.push({ id, rule: { type: 'hashmod',
            modulo: spec.modulo, remainders } });
    });
    Object.entries(spec.statics || {}).forEach(([id, destinationIds]) => {
        doc.workgroups.push({ id, rule: { type: 'static', destinationIds } });
    });
    const checked = wg.validateWorkgroupsDoc(doc);
    if (checked.error) {
        throw checked.error;
    }
    return doc;
}

/**
 * Write the workgroups document, creating the node if needed, and keep a
 * copy under history/gen<N> for the seed tool. zkCli takes
 * the data as one argument, and docker exec passes argv through untouched,
 * so a JSON string with spaces is fine as long as it has no newline.
 *
 * @param {Object} doc - the document
 * @return {Object} the document as read back
 */
function writeWorkgroupsDoc(doc) {
    const data = JSON.stringify(doc);
    const parent = env.ZK_WORKGROUPS_PATH.split('/').slice(0, -1).join('/');
    if (parent) {
        cli(['create', parent, '']);
    }
    const r = cli(['set', env.ZK_WORKGROUPS_PATH, data]);
    if (!r.ok || /Node does not exist/.test(`${r.out}${r.err}`)) {
        cli(['create', env.ZK_WORKGROUPS_PATH, data]);
    }
    // the seed tool reads a previous generation's layout from the history
    // node when it works out which group owned which destination, so every
    // generation written here is kept there too, as an Ansible run would
    cli(['create', `${env.ZK_WORKGROUPS_PATH}/history`, '']);
    const hist = `${env.ZK_WORKGROUPS_PATH}/history/gen${doc.generation}`;
    if (!cli(['set', hist, data]).ok
        || !getJson(hist)) {
        cli(['create', hist, data]);
    }
    const back = workgroupsDoc();
    if (!back || back.generation !== doc.generation) {
        throw new Error('the workgroups document did not take: read back '
            + `${JSON.stringify(back)}`);
    }
    return back;
}

/**
 * The per-destination watermarks the seed tool wrote for a generation.
 *
 * @param {Number} generation - generation number
 * @return {Object|null} { destinationId: { partition: offset } }
 */
function watermarks(generation) {
    return getJson(`${env.ZK_WORKGROUPS_PATH}/watermarks/gen${generation}`);
}

function deleteWatermarks(generation) {
    return deleteAll(`${env.ZK_WORKGROUPS_PATH}/watermarks/gen${generation}`);
}

function populatorOffset() {
    return get(`${env.ZK_POPULATOR_PATH}/logState/mongo_s3-recordlog/logOffset`);
}

/**
 * A one-screen view of the workgroups document, for the narration.
 *
 * @param {Object} doc - the document
 * @return {Array} lines to print
 */
function describeDoc(doc) {
    if (!doc) {
        return ['no workgroups document in zookeeper'];
    }
    const out = [];
    out.push(`generation ${doc.generation}, config version `
        + `${doc.configVersion}, topic ${doc.topic}`);
    (doc.workgroups || []).forEach(w => {
        const rule = w.rule || {};
        const how = rule.type === 'static'
            ? `static, destinations ${(rule.destinationIds || []).join(',')}`
            : `hashmod modulo ${rule.modulo}, remainders `
              + `${(rule.remainders || []).join(',')}`;
        out.push(`  workgroup ${w.id}: ${how}`);
    });
    if (doc.barriers) {
        out.push(`  barriers, partition to offset: ${JSON.stringify(doc.barriers)}`);
    }
    if (doc.previousGroups && doc.previousGroups.length) {
        out.push(`  replaces groups: ${doc.previousGroups.join(', ')}`);
    }
    return out;
}

/**
 * Which workgroup a destination belongs to, answered by the repository's own
 * function rather than by a copy of the rule.
 *
 * @param {Object} doc - the workgroups document
 * @param {Array} destinations - destination ids
 * @return {Object} destination id to workgroup id
 */
function mapping(doc, destinations) {
     
    const wg = require(`${env.BACKBEAT_DIR}/extensions/notification/utils/workgroups`);
    const out = {};
    destinations.forEach(d => {
        // the repository's own operator-facing function, so the demo and the
        // workers cannot disagree about who owns a destination
        out[d] = wg.workgroupIdForDestination(doc, d);
    });
    return out;
}

/**
 * The consumer group a workgroup joins, from the repository's own builder.
 *
 * @param {String} base - the configured base group id
 * @param {String} workgroupId - workgroup id
 * @param {Number} generation - generation
 * @return {String} the group id
 */
function groupIdFor(base, workgroupId, generation) {
     
    const wg = require(`${env.BACKBEAT_DIR}/extensions/notification/utils/workgroups`);
    return wg.buildGroupId(base, workgroupId, generation);
}

module.exports = {
    cli,
    get,
    getJson,
    ls,
    deleteAll,
    workgroupsDoc,
    buildWorkgroupsDoc,
    writeWorkgroupsDoc,
    watermarks,
    deleteWatermarks,
    populatorOffset,
    describeDoc,
    mapping,
    groupIdFor,
};
