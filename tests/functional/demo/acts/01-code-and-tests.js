'use strict';

/**
 * Act 01: here is the code, and here are its tests.
 *
 * The demo shows behaviour; this act shows what produced it. The commit
 * list, the files that carry the delivery pool, where the tests live, and
 * then the unit suite and the linter running.
 *
 * By default it runs the unit suite and the linter, which together take
 * about three minutes. The functional suites for the pool, today's topic,
 * the workgroups and Kerberos run in CI on every push and take fifteen to
 * thirty minutes between them, so here they are behind DEMO_ACT01_FULL=1
 * and the act says so on screen rather than skipping them silently. They
 * honour KAFKA_HOSTS and ZOOKEEPER_HOSTS, so they run against this stack at
 * any PORT_OFFSET. One unit file binds port 8080, so two unit runs cannot
 * overlap on one machine.
 */

const fs = require('fs');
const path = require('path');
const env = require('../lib/env');
const { run } = require('../lib/sh');
const { Act, say, note, step, line } = require('../lib/narrate');

// The base this work sits on, and the two segments of it, described by what
// they carry rather than by name: the branch names are the repository's, and
// this act reads them off it.
const BASE = 'development/9.3';
const SEGMENTS = [
    ['workgroups',
        'the delivery pool and workgroups: publish-time addressing, the '
        + 'worker, the producer pool, the slice filter, the ZooKeeper '
        + 'document, the barrier cutover CLI and the drainer'],
    ['kerberos-producer',
        'per-destination producer stacks, so one process can hold several '
        + 'Kerberos identities'],
];

const FILE_MAP = [
    ['extensions/notification/NotificationQueuePopulator.js',
        'publishes one addressed record per matching destination'],
    ['extensions/notification/utils/deliveryKey.js',
        'the delivery key: the destination name, optionally spread'],
    ['extensions/notification/utils/workgroups.js',
        'the workgroups document, the ownership function and the barrier'],
    ['extensions/notification/deliveryWorker/task.js',
        'the worker entry point, its probe server and its workgroup id'],
    ['extensions/notification/deliveryWorker/DeliveryWorker.js',
        'consume, resolve, deliver, commit on terminal resolution'],
    ['extensions/notification/deliveryWorker/DeliveryProducerPool.js',
        'one producer per endpoint and credential, idle-reaped'],
    ['extensions/notification/deliveryWorker/WorkgroupConfigLoader.js',
        'reads the document from ZooKeeper and caches it'],
    ['extensions/notification/deliveryWorker/WorkgroupCutover.js',
        'barriers, the document write, the pre-seed and the drain report'],
    ['extensions/notification/deliveryWorker/DeliveryTopicDrainer.js',
        'the drainer, for a legacy processor that cannot drain'],
    ['extensions/notification/destination/KafkaNotificationDestination.js',
        'the destination seam, where the producer stack is chosen'],
    ['bin/notificationWorkgroupCutover.js',
        'the operator CLI: plan, cutover, preseed, show, verify'],
    ['bin/notificationDeliveryReplay.js', 'the drainer CLI'],
    ['tests/functional/deliverypool/deliveryPool.js',
        'the pool functional suite, out of CI'],
    ['tests/functional/deliverypool/workgroups.js',
        'the workgroups functional suite: slices, generations, reshard'],
    ['tests/functional/deliverypool/kerberos.js',
        'the two-principal suite'],
    ['tests/functional/demo/demo.js', 'this demo suite'],
];

// name, command, what to expect, how long, extra environment, gated
const SUITES = [
    ['unit suite', ['yarn', '--silent', 'test'],
        'about 1720 passing, 1 pending, 0 failing', '1 to 4 minutes',
        { CI: 'true' }, false],
    ['lint', ['yarn', 'lint'], 'no errors', 'about a minute', {}, false],
    ['delivery pool functional suite',
        ['yarn', 'ft_test:notification:deliverypool'],
        '15 cases green', '2 to 3 minutes', {}, true],
    ['workgroups functional suite',
        ['yarn', 'ft_test:notification:workgroups'],
        'the slice, generation, observability and reshard gates',
        '8 to 15 minutes', {}, true],
    ['kerberos functional suite',
        ['yarn', 'ft_test:notification:kerberos'],
        'two principals, 9 of 9 each, in one process', '5 to 10 minutes',
        {}, true],
];

/**
 * The first of these refs that the repository actually has. A fresh clone
 * has origin/<name> and no local branch, so both spellings are tried.
 *
 * @param {String} name - a ref name
 * @return {String|null} a resolvable ref, or null
 */
function ref(name) {
    for (const candidate of [name, `origin/${name}`, `refs/remotes/origin/${name}`]) {
        if (run('git', ['-C', env.BACKBEAT_DIR, 'rev-parse', '--verify',
            '--quiet', candidate]).ok) {
            return candidate;
        }
    }
    return null;
}

/**
 * The branch whose name ends in this suffix, local or remote, by what it
 * carries rather than by its full name.
 *
 * @param {String} suffix - e.g. 'workgroups'
 * @return {String|null} the ref, or null
 */
function branchEndingIn(suffix) {
    const out = run('git', ['-C', env.BACKBEAT_DIR, 'for-each-ref',
        '--format=%(refname:short)', 'refs/heads/poc', 'refs/remotes/origin/poc']).out;
    const names = out.split('\n').map(s => s.trim()).filter(Boolean);
    return names.find(n => n.endsWith(suffix)) || null;
}

function register() {
    describe('Act 01: the code and its tests', () => {
        const act = new Act('01', 'code-and-tests', 'the branches',
            'what produced the behaviour the other acts show');
        const full = env.knob('DEMO_ACT01_FULL', '') === '1';

        before(() => {
            act.open();
            act.expect('unit suite', 'about 1800 passing, 0 failing (5 specs '
                + 'outside the POC need a MongoDB on the default port)');
            act.expect('lint', 'no errors');
            if (full) {
                act.expect('delivery pool functional suite', 'green');
                act.expect('workgroups functional suite', 'green');
                act.expect('kerberos functional suite', 'green');
            }
        });

        after(() => act.close());

        it('shows the commits, the files and where the tests live', () => {
            const base = ref(BASE);
            step(1, `the work, as commits on ${BASE}`);
            if (!base) {
                note(`this clone has no ${BASE} ref, so there is nothing to`);
                note('measure against. Fetch it: git fetch origin '
                    + `${BASE}:${BASE}`);
            } else {
                const all = run('git', ['-C', env.BACKBEAT_DIR, 'log',
                    '--oneline', `${base}..HEAD`]).out.split('\n')
                    .filter(Boolean);
                const shortstat = run('git', ['-C', env.BACKBEAT_DIR, 'diff',
                    '--shortstat', `${base}..HEAD`]).out;
                say(`${all.length} commits, ${shortstat}`);
                fs.writeFileSync(act.file('commits.txt'),
                    `${all.join('\n')}\n`);
                note(`the whole list is in ${act.file('commits.txt')}`);
                line('');
                SEGMENTS.forEach(([suffix, what]) => {
                    const b = branchEndingIn(suffix);
                    line(`  ${b || `(no branch ending in ${suffix} here)`}`);
                    line(`  ${what}`);
                    if (!b) {
                        return;
                    }
                    const commits = run('git', ['-C', env.BACKBEAT_DIR, 'log',
                        '--oneline', `${base}..${b}`]).out.split('\n')
                        .filter(Boolean);
                    line(`  ${commits.length} commits on ${BASE}:`);
                    commits.slice(0, 10).forEach(c => line(`    ${c}`));
                    if (commits.length > 10) {
                        line(`    ... and ${commits.length - 10} more`);
                    }
                    fs.writeFileSync(act.file(`commits-${suffix}.txt`),
                        `${commits.join('\n')}\n`);
                    line('');
                });
                note('this branch sits on top of both segments, so the tree');
                note('every act runs carries all of it. A third branch holds');
                note('an assume-destination experiment that was explored and');
                note('set aside: reference only, not part of the demo.');
            }

            step(2, 'the files that carry the delivery pool');
            FILE_MAP.forEach(([file, what]) => {
                const p = path.join(env.BACKBEAT_DIR, file);
                const lines = fs.existsSync(p)
                    ? fs.readFileSync(p, 'utf8').split('\n').length : 0;
                line(`  ${String(lines).padStart(5)} lines  ${file}`);
                line(`                ${what}`);
            });

            step(3, 'where the tests live');
            note('the pool, workgroups and kerberos functional suites are out');
            note('of CI on purpose: they need a broker, ZooKeeper and, for');
            note('kerberos, a KDC. The unit suite runs in CI. The demo suite');
            note('is excluded from every CI glob.');
            line('  tests/unit/                      the unit suite, in CI');
            line('  tests/functional/deliverypool/   pool, workgroups, kerberos');
            line('  tests/functional/demo/           this demo suite');
            line('  poc-demo/                        the stack it runs against');
        });

        it('runs the suites, or says why it will not', async function suites() {
            this.timeout(45 * 60 * 1000);
            const only = (env.knob('DEMO_SUITES', '') || '').split(',')
                .map(s => s.trim()).filter(Boolean);

            step(4, full
                ? 'every suite, including the three long functional ones'
                : 'the unit suite and the linter');
            if (!full) {
                note('the three functional suites for the pool, the workgroups');
                note('and kerberos are not run here. They take fifteen to');
                note('thirty minutes between them, they need a stack at');
                note('PORT_OFFSET=0 because they hardcode localhost:9092, and');
                note('acts 03 to 07 show the same mechanics live against real');
                note('processes. To run them here: DEMO_ACT01_FULL=1.');
                line('');
                SUITES.filter(s => s[5]).forEach(([name, cmd, expected, howLong]) => {
                    line(`  ${name.padEnd(34)} ${cmd.join(' ')}`);
                    line(`  ${''.padEnd(34)} expect ${expected}, ${howLong}`);
                });
                line('');
            }
            note('one constraint on the unit suite: tests/unit/RoleCredentials.js');
            note('binds port 8080, so two full unit runs cannot overlap on one');
            note('machine. Run them one at a time.');

            for (const [name, cmd, expected, howLong, extra, gated] of SUITES) {
                if (gated && !full) {
                    act.measured(name, 'not run, DEMO_ACT01_FULL=1 runs it');
                    continue;
                }
                if (only.length && !only.some(o => name.includes(o))) {
                    act.measured(name, 'skipped by DEMO_SUITES');
                    continue;
                }
                if (gated && env.PORT_OFFSET !== 0) {
                    note(`${name}: the stack is at PORT_OFFSET=`
                        + `${env.PORT_OFFSET}, so the broker is on `
                        + `${env.KAFKA_PORT} and this suite's hardcoded `
                        + 'localhost:9092 will not find it. Running it anyway, '
                        + 'because DEMO_ACT01_FULL=1 asked for it.');
                }
                say(`${cmd.join(' ')} in ${env.BACKBEAT_DIR}`);
                say(`expect ${expected}, takes ${howLong}`);
                const started = Date.now();
                const r = run(cmd[0], cmd.slice(1), {
                    cwd: env.BACKBEAT_DIR,
                    timeout: 40 * 60 * 1000,
                    env: Object.assign({}, process.env, {
                        PATH: `${env.NODE_BIN}:${process.env.PATH}`,
                    }, extra),
                });
                const secs = Math.round((Date.now() - started) / 1000);
                fs.writeFileSync(act.file(`${name.replace(/\s+/g, '-')}.log`),
                    `${r.out}\n${r.err}\n`);
                const summary = (r.out.match(/\d+ (passing|failing|pending)/g)
                    || []).join(', ');
                line(r.out.split('\n').slice(-15)
                    .map(l => `      | ${l}`).join('\n'));
                say(`${name}: exit ${r.code} in ${secs}s ${summary}`);
                act.measured(name, summary || (r.ok ? 'no errors'
                    : `exit ${r.code}`));
            }
        });
    });
}

module.exports = { register };
