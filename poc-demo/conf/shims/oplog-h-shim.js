/*
 * Demo-only preload shim, loaded with `node --require`. It patches a module
 * in memory for the life of one process, so nothing on disk, in the
 * repository or in node_modules, is modified.
 *
 * WHY
 * ---
 * arsenal's mongo oplog reader identifies oplog entries by their `h` field:
 *   lib/storage/metadata/mongoclient/LogConsumer.js  -> data[0].h.toString()
 *   lib/storage/metadata/mongoclient/ListRecordStream.js -> itemObj.h.toString()
 * MongoDB removed the oplog `h` field in 4.2. The rig runs the ci-mongodb image
 * (mongo:5.0.21), so every oplog entry lacks `h` and the populator dies with
 *   TypeError: Cannot read properties of undefined (reading 'toString')
 * on its first batch, before reading a single record.
 *
 * WHAT THIS DOES
 * --------------
 * Wraps LogConsumer.prototype.connectMongo so the oplog collection handed to
 * the reader returns documents with `h` aliased to `ts`, the BSON Timestamp.
 * `h` is only ever used as an opaque unique id for resume matching
 * (lastSavedID === lastConsumedID), and `ts` is unique and monotonic per oplog
 * entry, so it serves that role at least as well as the old random hash.
 *
 * Nothing else is touched: record decoding, the notification extension, the
 * internal topic and the delivery path all run the unmodified worktree code.
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

function aliasH(doc) {
    if (doc && doc.h === undefined && doc.ts !== undefined) {
        // eslint-disable-next-line no-param-reassign
        doc.h = doc.ts;
    }
    return doc;
}

function wrapCursor(cursor) {
    const nextFn = cursor.next.bind(cursor);
    cursor.next = async (...args) => aliasH(await nextFn(...args));

    const toArrayFn = cursor.toArray.bind(cursor);
    cursor.toArray = async (...args) => {
        const docs = await toArrayFn(...args);
        if (Array.isArray(docs)) {
            docs.forEach(aliasH);
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
        // `find` is the only entry point the reader uses; sort()/limit() return
        // the same cursor object, so the wrapped toArray survives the chain.
        this._coll = {
            find: (...args) => wrapCursor(coll.find(...args)),
        };
        return done();
    });
};

// eslint-disable-next-line no-console
console.error('[rig] oplog `h`->`ts` shim installed (MongoDB >=4.2 compat)');
