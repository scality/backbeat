'use strict';

/**
 * The backbeat processes, as real child processes. The audience has to see
 * separate processes, and an act has to be able to kill -9 one of them, so
 * nothing here runs in the mocha process.
 *
 * Each child's stdout and stderr go straight to a file descriptor rather
 * than through this process, so the log keeps filling even while the suite
 * is blocked in a synchronous kafka call.
 */

const fs = require('fs');
const path = require('path');
const { spawn } = require('child_process');
const env = require('./env');
const conf = require('./conf');
const { say, note } = require('./narrate');

const ALL = [];

class Proc {
    /**
     * @param {Object} p - name, argv (after the shims), configFile, logFile,
     *   readyRe, probePort, workgroupId, autoRestart, cwd, extraEnv
     */
    constructor(p) {
        this.name = p.name;
        this.argv = p.argv;
        this.configFile = p.configFile;
        this.logFile = p.logFile;
        this.readyRe = p.readyRe;
        this.probePort = p.probePort;
        this.workgroupId = p.workgroupId;
        this.autoRestart = !!p.autoRestart;
        this.cwd = p.cwd || env.BACKBEAT_DIR;
        this.extraEnv = p.extraEnv || {};
        // a keeper survives stopAll(), which acts call to clean up after
        // themselves. CloudServer is the only one.
        this.keep = !!p.keep;
        this.exits = [];
        this.restarts = 0;
        this.held = false;
        this.child = null;
        this.pidFile = path.join(env.RUN, `${this.name}.pid`);
        fs.mkdirSync(env.RUN, { recursive: true });
        ALL.push(this);
    }

    spawnOnce() {
        const fd = fs.openSync(this.logFile, 'a');
        const childEnv = Object.assign({}, process.env, {
            PATH: `${env.NODE_BIN}:${process.env.PATH}`,
            BACKBEAT_CONFIG_FILE: this.configFile,
            RIG_PIDFILE: this.pidFile,
        }, this.extraEnv);
        if (this.probePort) {
            childEnv.DELIVERY_POOL_PROBE_PORT = String(this.probePort);
        }
        if (this.workgroupId) {
            childEnv.DELIVERY_POOL_WORKGROUP_ID = this.workgroupId;
        }
        fs.appendFileSync(this.logFile,
            `\n=== ${new Date().toISOString()} starting ${this.name} `
            + `(config ${path.basename(this.configFile)}`
            + `${this.workgroupId ? `, workgroup ${this.workgroupId}` : ''})\n`);
        this.child = spawn(env.NODE, conf.shimArgs().concat(this.argv), {
            cwd: this.cwd,
            env: childEnv,
            stdio: ['ignore', fd, fd],
            detached: false,
        });
        this.pid = this.child.pid;
        this.startedAt = Date.now();
        this.child.on('exit', (code, signal) => {
            this.exits.push({ code, signal, at: Date.now() });
            fs.appendFileSync(this.logFile,
                `=== ${new Date().toISOString()} ${this.name} EXIT `
                + `code=${code} signal=${signal}\n`);
            this.child = null;
            if (this.autoRestart && !this.held) {
                // this is what systemd does on a deployment: an ordinary
                // rebalance can kill a worker through the uncaught commit
                // path throw, and the restart is what keeps the pool serving
                setTimeout(() => {
                    if (!this.held && !this.child) {
                        this.restarts += 1;
                        say(`${this.name} exited by itself `
                            + `(code ${code}, signal ${signal}); restarting it, `
                            + 'the way systemd would');
                        this.spawnOnce();
                    }
                }, 2000);
            }
        });
        return this;
    }

    alive() {
        return !!(this.child && this.child.pid && !this.child.killed
            && this.isRunning());
    }

    isRunning() {
        try {
            process.kill(this.pid, 0);
            return true;
        } catch {
            return false;
        }
    }

    logText() {
        try {
            return fs.readFileSync(this.logFile, 'utf8');
        } catch {
            return '';
        }
    }

    /**
     * @param {RegExp} re - pattern to count
     * @return {Number} matches in the log
     */
    count(re) {
        return (this.logText().match(re) || []).length;
    }

    rebalances() {
        return {
            assign: this.count(/rdkafka\.assign/g),
            revoke: this.count(/rdkafka\.revoke/g),
        };
    }

    kill(signal) {
        if (!this.pid) {
            return false;
        }
        try {
            process.kill(this.pid, signal || 'SIGTERM');
            return true;
        } catch {
            return false;
        }
    }

    /** hold it deliberately dead, so an auto-restart does not undo the point */
    hold() {
        this.held = true;
        return this;
    }

    release() {
        this.held = false;
        if (!this.isRunning()) {
            this.restarts += 1;
            this.spawnOnce();
        }
        return this;
    }

    /**
     * Stop it, and mean it.
     *
     * A delivery worker does not reliably stop on SIGTERM: its consumer
     * close waits for a revoke callback with no deadline of its own, and on
     * the rig one stayed alive four minutes after logging "received SIGTERM,
     * exiting". A process left behind keeps its group membership and its
     * probe port, so a replacement cannot bind and the group keeps churning.
     * So escalate.
     *
     * @param {Object} [opts] - { graceMs }
     * @return {Proc} this
     */
    stop(opts) {
        this.held = true;
        this.autoRestart = false;
        if (!this.isRunning()) {
            return this;
        }
        this.kill('SIGTERM');
        const deadline = Date.now() + ((opts && opts.graceMs) || 6000);
        while (Date.now() < deadline && this.isRunning()) {
            require('./sh').sleepSync(250);
        }
        if (this.isRunning()) {
            note(`${this.name} ignored SIGTERM, sending KILL`);
            this.kill('SIGKILL');
            const hard = Date.now() + 4000;
            while (Date.now() < hard && this.isRunning()) {
                require('./sh').sleepSync(250);
            }
        }
        return this;
    }
}

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, Math.max(0, ms)));
}

/**
 * Wait for a process to say it is up: a log line, and where there is one, a
 * liveness probe.
 *
 * @param {Proc} proc - the process
 * @param {Number} timeoutMs - how long to wait
 * @return {Promise} resolves with the seconds it took, rejects on timeout
 */
async function waitReady(proc, timeoutMs) {
    const deadline = Date.now() + (timeoutMs || 60000);
    while (Date.now() < deadline) {
        if (proc.readyRe && proc.readyRe.test(proc.logText())) {
            return Math.round((Date.now() - proc.startedAt) / 1000);
        }
        if (!proc.readyRe && proc.isRunning()) {
            return Math.round((Date.now() - proc.startedAt) / 1000);
        }
        if (!proc.isRunning() && proc.exits.length && !proc.autoRestart) {
            throw new Error(`${proc.name} exited before it was ready `
                + `(see ${proc.logFile})`);
        }
        await sleep(500);
    }
    throw new Error(`${proc.name} was not ready within `
        + `${Math.round((timeoutMs || 60000) / 1000)}s (see ${proc.logFile})`);
}

// ------------------------------------------------------------ factories ---

/**
 * The queue populator. Which path it takes is the config, nothing else.
 *
 * @param {Object} act - the act, for the log location
 * @param {String} configFile - generated config
 * @param {String} [label] - log file suffix, e.g. 'legacy' or 'pool'
 * @return {Proc} started process
 */
function populator(act, configFile, label) {
    const p = new Proc({
        name: `populator-${label || 'x'}`,
        argv: ['bin/queuePopulator.js'],
        configFile,
        logFile: act.file(`populator-${label || 'x'}.log`),
        readyRe: /notification extension is active|Server is listening/,
        extraEnv: { BACKBEAT_QUEUEPOPULATOR_EXTENSIONS: 'notification' },
    });
    return p.spawnOnce();
}

/**
 * One legacy per-destination queue processor: today's shipped path, and the
 * design's single-destination worker of generation v0.
 *
 * @param {Object} act - the act
 * @param {String} configFile - generated config
 * @param {String} dest - destination id
 * @return {Proc} started process
 */
function legacyProcessor(act, configFile, dest) {
    const p = new Proc({
        name: `processor-${dest}`,
        argv: ['extensions/notification/queueProcessor/task.js', dest],
        configFile,
        logFile: act.file(`processor-${dest}.log`),
        readyRe: /queue processor is ready to consume/,
    });
    return p.spawnOnce();
}

/**
 * A delivery-pool worker. Its probe port is 8920 plus its index at offset 0
 * and 9920 plus its index at offset 1000, which is what prometheus already
 * scrapes.
 *
 * @param {Object} act - the act
 * @param {String} configFile - generated config
 * @param {Number} n - worker index
 * @param {Object} [opts] - { workgroupId, autoRestart }
 * @return {Proc} started process
 */
function worker(act, configFile, n, opts) {
    const o = opts || {};
    const name = o.workgroupId ? `worker${n}-${o.workgroupId}` : `worker${n}`;
    const p = new Proc({
        name,
        argv: ['extensions/notification/deliveryWorker/task.js'],
        configFile,
        logFile: act.file(`${name}.log`),
        readyRe: /rdkafka\.assign|delivery worker is ready|Server is listening/,
        probePort: env.probePort(n),
        workgroupId: o.workgroupId,
        autoRestart: o.autoRestart,
    });
    return p.spawnOnce();
}

/**
 * The workload driver, as a child so a synchronous kafka call in the suite
 * cannot starve it. Sequence in the object size, straddle keys optional.
 *
 * @param {Object} act - the act
 * @param {Object} opts - driver-cli options
 * @return {Proc} started process
 */
function driver(act, opts) {
    const args = [path.join(__dirname, 'driver-cli.js')];
    Object.keys(opts).forEach(k => {
        if (opts[k] === true) {
            args.push(`--${k}`);
        } else if (opts[k] !== undefined && opts[k] !== null) {
            args.push(`--${k}`, String(opts[k]));
        }
    });
    const p = new Proc({
        name: `driver-${opts.prefix || 'x'}`,
        argv: args,
        configFile: 'none',
        logFile: act.file(`driver-${opts.prefix || 'x'}.out`),
        readyRe: /driver start/,
        cwd: env.DEMO,
    });
    return p.spawnOnce();
}

/**
 * Run one of the repository's own tools to completion and return its output,
 * e.g. bin/notificationWorkgroupCutover.js.
 *
 * @param {Object} p - { act, name, argv, configFile, timeoutMs }
 * @return {Object} { code, out }
 */
function runTool(p) {
    const { run } = require('./sh');
    const r = run(env.NODE, conf.shimArgs().concat(p.argv), {
        cwd: env.BACKBEAT_DIR,
        timeout: p.timeoutMs || 180000,
        env: Object.assign({}, process.env, {
            PATH: `${env.NODE_BIN}:${process.env.PATH}`,
            BACKBEAT_CONFIG_FILE: p.configFile,
        }, p.extraEnv || {}),
    });
    if (p.act && p.name) {
        fs.appendFileSync(p.act.file(`${p.name}.log`),
            `\n=== ${new Date().toISOString()} ${p.argv.join(' ')}\n`
            + `${r.out}\n${r.err ? `stderr:\n${r.err}\n` : ''}`
            + `exit=${r.code}\n`);
    }
    return { code: r.code, out: r.out, err: r.err };
}

/**
 * Stop what this suite started, in reverse order. Keepers (CloudServer) stay
 * up unless force is set.
 *
 * @param {Boolean} [force] - include the keepers
 * @return {undefined}
 */
function stopAll(force) {
    ALL.slice().reverse().filter(p => force || !p.keep).forEach(p => {
        p.held = true;
        p.autoRestart = false;
        if (p.isRunning()) {
            p.kill('SIGTERM');
        }
    });
    // a delivery worker does not reliably stop on SIGTERM: its consumer close
    // waits for a revoke callback with no deadline of its own
    const mine = ALL.filter(p => force || !p.keep);
    const deadline = Date.now() + 8000;
    while (Date.now() < deadline && mine.some(p => p.isRunning())) {
        require('./sh').sleepSync(500);
    }
    mine.forEach(p => {
        if (p.isRunning()) {
            note(`${p.name} ignored SIGTERM, sending KILL`);
            p.kill('SIGKILL');
        }
    });
}

module.exports = {
    Proc,
    sleep,
    waitReady,
    populator,
    legacyProcessor,
    worker,
    driver,
    runTool,
    stopAll,
    ALL,
};
