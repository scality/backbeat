/*
 * Demo-only preload shim, loaded with `node --require`. It patches a module
 * in memory for the life of one process, so nothing on disk, in the
 * repository or in node_modules, is modified.
 *
 * WHY
 * ---
 * arsenal's ListRecordStream decodes oplog updates as:
 *     const value = (itemObj.o.$set ? itemObj.o.$set : itemObj.o).value;
 * i.e. it expects the `$v: 1` oplog update format, `{ $set: { value: {...} } }`.
 *
 * MongoDB 5.0 emits the `$v: 2` delta format instead:
 *     { "$v": 2, "diff": { "u": { "value": { ...full object metadata... } } } }
 *     { "$v": 2, "diff": { "svalue": { "i": { "deleted": true } } } }
 * There is no `$set`, so `value` comes out undefined and EVERY update is
 * dropped. On the rig this meant 20 PUTs (oplog op 'i', which arsenal reads
 * straight from `o`) produced 20 events, while 10 update ops and 5 delete ops
 * produced none. No ObjectRemoved event can ever be emitted.
 *
 * Deletes matter here because of how the delete path is shaped. CloudServer
 * writes TWO updates per delete before the actual remove:
 *   1. {"$v":2,"diff":{"svalue":{"i":{"deleted":true}}}}          (flag only)
 *   2. {"$v":2,"diff":{"u":{"value":{...,"originOp":"s3:ObjectRemoved:Delete",
 *                                   "deleted":true,...}}}}        (full metadata)
 * The second one is what backbeat needs: ListRecordStream turns
 * `value.deleted` into `type: 'delete'`, and NotificationQueuePopulator reads
 * the event name from `value.originOp` (extensions/notification/constants.js:18
 * maps eventType -> 'originOp'). Note the raw oplog 'd' op is deliberately
 * ignored by arsenal because it carries no metadata, and the populator's
 * `type === 'del'` fallback never fires on the mongo log source, which yields
 * `type: 'delete'`. So the delete event depends entirely on decoding update #2.
 *
 * WHAT THIS DOES
 * --------------
 * Rewrites `$v: 2` diff documents coming out of the oplog collection into the
 * equivalent `{ $set: {...}, $unset: {...} }` shape, using dotted paths for
 * nested sub-diffs. Field values are passed through untouched, so the event
 * payload is exactly the metadata CloudServer wrote. Update #1 above becomes
 * `$set: {'value.deleted': true}`, which has no `$set.value`, so arsenal keeps
 * ignoring it, which is the correct outcome. Update #2 becomes
 * `$set: {value: {...}}` and produces the real ObjectRemoved event.
 *
 * Array diffs (`{a: true, ...}`) are left as dotted paths without attempting to
 * reconstruct element order. Nothing in the notification path reads them.
 */
'use strict';

const path = require('path');

/**
 * Resolve a module the way the process this shim is preloaded into resolves
 * it, so the copy that gets patched is the copy that gets loaded.
 *
 * The suite spawns every backbeat process with the repository root as its
 * working directory, so that is the first place to look. BACKBEAT_DIR is the
 * explicit override, and this file's own repository is the fallback for a
 * process started from somewhere else. There is no hardcoded path: a shim
 * that patches another checkout's node_modules reports success and changes
 * nothing.
 *
 * @param {String} spec - a module path under node_modules
 * @return {String} an absolute, resolvable module path
 */
function resolveInRepo(spec) {
    const bases = [
        process.cwd(),
        process.env.BACKBEAT_DIR,
        // poc-demo/conf/shims -> the repository root
        path.resolve(__dirname, '..', '..', '..'),
    ].filter(Boolean);
    for (const base of bases) {
        try {
            return require.resolve(path.join(base, 'node_modules', spec));
        } catch {
            // try the next base
        }
    }
    return require.resolve(spec);
}

const LOG_CONSUMER = resolveInRepo(
    'arsenal/build/lib/storage/metadata/mongoclient/LogConsumer.js');

function join(prefix, field) {
    return prefix ? `${prefix}.${field}` : field;
}

/**
 * Walk a $v:2 diff, accumulating dotted-path assignments and removals.
 */
function walkDiff(diff, prefix, set, unset) {
    Object.keys(diff).forEach(op => {
        if (op === '$v' || op === 'a' || op === 'l') {
            return;
        }
        const payload = diff[op];
        if (op === 'i' || op === 'u') {
            // fields inserted / updated at this level
            Object.keys(payload).forEach(field => {
                set[join(prefix, field)] = payload[field];
            });
            return;
        }
        if (op === 'd') {
            // fields removed at this level
            Object.keys(payload).forEach(field => {
                unset[join(prefix, field)] = '';
            });
            return;
        }
        if (op.startsWith('s')) {
            // sub-diff for the field named by the rest of the key
            const field = op.slice(1);
            if (payload && typeof payload === 'object') {
                walkDiff(payload, join(prefix, field), set, unset);
            }
            return;
        }
        if (op.startsWith('u') && op.length > 1) {
            // array element assignment, e.g. u0 -> index 0
            set[join(prefix, op.slice(1))] = payload;
        }
    });
}

function translate(doc) {
    if (!doc || doc.op !== 'u' || !doc.o || doc.o.$v !== 2 || !doc.o.diff) {
        return doc;
    }
    const set = {};
    const unset = {};
    walkDiff(doc.o.diff, '', set, unset);

    const translated = {};
    if (Object.keys(set).length > 0) {
        translated.$set = set;
    }
    if (Object.keys(unset).length > 0) {
        translated.$unset = unset;
    }
    // keep the original around for debugging; arsenal never reads it
    translated.__v2diff = doc.o.diff;
    // eslint-disable-next-line no-param-reassign
    doc.o = translated;
    return doc;
}

function wrapCursor(cursor) {
    const nextFn = cursor.next.bind(cursor);
    cursor.next = async (...args) => translate(await nextFn(...args));

    const toArrayFn = cursor.toArray.bind(cursor);
    cursor.toArray = async (...args) => {
        const docs = await toArrayFn(...args);
        if (Array.isArray(docs)) {
            docs.forEach(translate);
        }
        return docs;
    };
    return cursor;
}

const LogConsumer = require(LOG_CONSUMER);
const connectMongo = LogConsumer.prototype.connectMongo;

LogConsumer.prototype.connectMongo = function patchedConnectMongo(done) {
    return connectMongo.call(this, err => {
        if (err) {
            return done(err);
        }
        const coll = this._coll;
        this._coll = {
            find: (...args) => wrapCursor(coll.find(...args)),
        };
        return done();
    });
};

// eslint-disable-next-line no-console
console.error('[rig] oplog $v:2 diff -> $set shim installed (MongoDB >=5.0 compat)');
