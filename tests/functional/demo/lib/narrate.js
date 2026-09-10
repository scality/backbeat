'use strict';

/**
 * The suite is watched, not just run, so every step says what it is doing in
 * plain language, with a timestamp, and every act ends with the outcome the
 * rig measured next to the one this run measured.
 */

const fs = require('fs');
const path = require('path');
const env = require('./env');

const W = 78;

function stamp() {
    return new Date().toISOString().replace('T', ' ').slice(11, 23);
}

function line(text) {
    process.stdout.write(`${text}\n`);
}

function rule(ch) {
    line(ch.repeat(W));
}

/** say <text...> : one narration line, timestamped */
function say(...parts) {
    line(`  ${stamp()}  ${parts.join(' ')}`);
}

/** note <text...> : an aside, no timestamp, for the explanation of a step */
function note(...parts) {
    line(`              ${parts.join(' ')}`);
}

function watch(where, what) {
    line(`              watch: ${where} -> ${what}`);
}

function step(n, text) {
    line('');
    line(`  --- step ${n}: ${text}`);
}

class Act {
    /**
     * @param {String} id - act number, e.g. '04'
     * @param {String} name - short name used for the evidence directory
     * @param {String} rigId - the scenario id in RESULTS.md, e.g. 'M2b'
     * @param {String} claim - one line: what this act proves
     */
    constructor(id, name, rigId, claim) {
        this.id = id;
        this.name = name;
        this.rigId = rigId;
        this.claim = claim;
        this.rows = [];
        this.dir = path.join(env.EVIDENCE, `${id}-${name}`);
        this.started = Date.now();
    }

    /** open the evidence directory, moving a previous run's aside */
    open() {
        if (fs.existsSync(this.dir) && fs.readdirSync(this.dir).length) {
            const moved = `${this.dir}.${new Date().toISOString()
                .replace(/[-:]/g, '').replace(/\.\d+Z$/, 'Z')}`;
            fs.renameSync(this.dir, moved);
            line(`  previous evidence moved to ${moved}`);
        }
        fs.mkdirSync(this.dir, { recursive: true });
        rule('=');
        line(` ACT ${this.id}  ${this.name}   (rig scenario ${this.rigId})`);
        line(` ${this.claim}`);
        line(` evidence: ${this.dir}`);
        line(` pace: ${env.PACE}   run id: ${env.RUN_ID}`);
        rule('=');
        this.timeline('ACT START');
        return this;
    }

    file(name) {
        return path.join(this.dir, name);
    }

    timeline(...parts) {
        fs.appendFileSync(this.file('timeline.txt'),
            `${new Date().toISOString()} ${Date.now()} ${parts.join(' ')}\n`);
    }

    /** expect <key> <value> : the rig's measured outcome */
    expect(key, value) {
        this.rows.push({ key, expected: String(value), measured: 'not measured' });
        return this;
    }

    /** measured <key> <value> : what this run got */
    measured(key, value) {
        const row = this.rows.find(r => r.key === key);
        if (row) {
            row.measured = String(value);
        } else {
            this.rows.push({ key, expected: '(not predicted)', measured: String(value) });
        }
        return this;
    }

    /**
     * Many expected values carry their explanation ("0 with the worker
     * started first"), so a measured value equal to the leading value counts
     * as close rather than as a difference.
     *
     * @param {Object} row - one row of the table
     * @return {String} the verdict word
     */
    static compare(row) {
        const e = row.expected;
        const m = row.measured;
        if (e === m) {
            return 'same';
        }
        if (m === 'not measured') {
            return '-';
        }
        if (e === '(not predicted)') {
            return 'context';
        }
        const head = e.split(' ')[0].replace(/,$/, '');
        if (m === head) {
            return 'close';
        }
        const or = / or (\S+)/.exec(e);
        if (or && m === or[1].replace(/,$/, '')) {
            return 'close';
        }
        return 'DIFFERS';
    }

    /** print the expected versus measured table and save it as evidence */
    close() {
        const secs = Math.round((Date.now() - this.started) / 1000);
        const out = [];
        out.push('');
        out.push('='.repeat(W));
        out.push(` ACT ${this.id} ${this.name}: rig measurement vs this run`);
        out.push('='.repeat(W));
        this.rows.forEach(r => {
            out.push(` ${r.key.padEnd(30)} rig ${r.expected.padEnd(34)}`
                + ` now ${r.measured.padEnd(16)} ${Act.compare(r)}`);
        });
        out.push('');
        out.push(` evidence ${this.dir}`);
        out.push(` took ${secs}s at pace ${env.PACE}`);
        out.push('');
        out.push(' A DIFFERS is not automatically a failure: the rig numbers are one');
        out.push(' run at one rate. Loss is the row that must match. Duplicates and');
        out.push(' inversions scale with backlog and rebalance timing.');
        out.push('');
        const text = out.join('\n');
        line(text);
        fs.writeFileSync(this.file('verdict.txt'), `${text}\n`);
        fs.writeFileSync(this.file('verdict.json'),
            `${JSON.stringify({ act: this.id, name: this.name, rig: this.rigId,
                seconds: secs, rows: this.rows }, null, 1)}\n`);
        this.timeline('ACT END');
    }
}

module.exports = { say, note, watch, step, line, rule, stamp, Act };
