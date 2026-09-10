'use strict';

const { execFileSync, spawnSync } = require('child_process');

/**
 * Run a command and return its stdout, trimmed. Never throws: a failure
 * comes back as { ok: false, out, err, code }, because most of what the
 * suite asks the world is a question, not an instruction.
 *
 * Synchronous on purpose. Everything long-lived (the populator, the workers,
 * the workload driver) is a child process, so blocking here blocks nothing
 * that matters.
 *
 * @param {String} cmd - command
 * @param {Array} args - arguments
 * @param {Object} [opts] - { cwd, env, input, timeout }
 * @return {Object} { ok, out, err, code }
 */
function run(cmd, args, opts) {
    const o = Object.assign({ encoding: 'utf8', timeout: 120000,
        maxBuffer: 64 * 1024 * 1024 }, opts || {});
    const r = spawnSync(cmd, args, o);
    return {
        ok: r.status === 0,
        code: r.status,
        out: (r.stdout || '').toString().trim(),
        err: (r.stderr || '').toString().trim(),
    };
}

/**
 * Same, but throw on failure, for the few places where carrying on makes no
 * sense.
 *
 * @param {String} cmd - command
 * @param {Array} args - arguments
 * @param {Object} [opts] - options
 * @return {String} stdout
 */
function must(cmd, args, opts) {
    const r = run(cmd, args, opts);
    if (!r.ok) {
        throw new Error(`${cmd} ${args.join(' ')} failed (${r.code}): `
            + `${r.err || r.out}`);
    }
    return r.out;
}

/**
 * Block for a while. Used for the deliberate pauses the recording needs.
 *
 * @param {Number} ms - milliseconds
 * @return {undefined}
 */
function sleepSync(ms) {
    if (ms <= 0) {
        return;
    }
    execFileSync(process.execPath, ['-e', `setTimeout(()=>{}, ${Math.round(ms)})`],
        { timeout: ms + 30000 });
}

module.exports = { run, must, sleepSync };
