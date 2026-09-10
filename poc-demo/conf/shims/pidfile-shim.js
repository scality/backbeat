/*
 * Rig preload shim, chaos round only. Writes the process pid to the path in
 * RIG_PIDFILE so the chaos driver can signal exactly this process.
 *
 * Needed because every backbeat process on this rig is started as
 * `node ... | tee -a log` inside a tmux window, and the shell's $! after a
 * background pipeline is tee's pid, not node's.
 *
 * Changes nothing about notification semantics: no offset policy, no group
 * id, no event shape, no delivery behaviour. Pure observation.
 */
'use strict';
const fs = require('fs');
const p = process.env.RIG_PIDFILE;
if (p) {
    try {
        fs.writeFileSync(p, `${process.pid}\n`);
        process.stderr.write(`[rig] pidfile shim installed, pid ${process.pid} -> ${p}\n`);
    } catch (err) {
        process.stderr.write(`[rig] pidfile shim FAILED: ${err.message}\n`);
    }
}
