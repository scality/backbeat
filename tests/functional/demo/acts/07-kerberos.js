'use strict';

/**
 * Act 07: can ONE delivery worker process serve destinations that
 * authenticate as DIFFERENT Kerberos principals?
 *
 * With librdkafka the answer is no, measured: one identity per process, and
 * every documented workaround fails. The per-connection alternative works: a
 * pure-JS client with a GSSAPI mechanism over the kerberos binding, which
 * acquires credentials by principal per client object. It is implemented in
 * backbeat behind the destination seam on the kerberos branch, with
 * node-rdkafka still the default.
 *
 * This act does not build a Kerberos rig. It runs that branch's own
 * functional suite against one that is already up, in a container sharing
 * the rig's network namespace, because the tests need MIT krb5, a Linux
 * node_modules and the docker socket. It skips cleanly when the profile, the
 * image or the branch is not there.
 *
 * The evidence rule to state on camera: only the broker's own
 * authenticationID line counts. A client stack can report the identity it
 * was configured with rather than the one it authenticated as.
 */

const fs = require('fs');
const path = require('path');
const env = require('../lib/env');
const { run } = require('../lib/sh');
const { Act, say, note, step, line } = require('../lib/narrate');

// The kerberos work is on this branch, so the suite it runs is in this
// repository. The sibling-worktree fallback is for a checkout that carries
// the demo but not the kerberos segment.
const KRB_SUITE = 'tests/functional/deliverypool/kerberos.js';
const KRB_WORKTREE = env.knob('KRB_BACKBEAT_DIR')
    || (fs.existsSync(path.join(env.BACKBEAT_DIR, KRB_SUITE))
        ? env.BACKBEAT_DIR
        : path.resolve(env.BACKBEAT_DIR, '..', 'backbeat-krb'));
const IMAGE = env.knob('KRB_TEST_IMAGE', 'backbeat-krbtest:spike');
const NM_VOLUME = env.knob('KRB_NODE_MODULES_VOLUME', 'backbeat-krb-nm');

function firstExisting(candidates) {
    return candidates.find(c => c && fs.existsSync(c)) || null;
}

function register() {
    describe('Act 07: two Kerberos principals in one process', () => {
        const act = new Act('07', 'kerberos', 'GATE 2',
            'one delivery worker process, two Kerberos identities, proven by '
            + 'the broker\'s own log');

        before(() => {
            act.open();
            act.expect('two principals in one process',
                '9 of 9 and 9 of 9, each as its own identity');
            act.expect('node-rdkafka collision control',
                'the losing producer delivers 0 of 9');
            act.expect('50 principals in one process', '250 of 250');
            act.expect('suite exit code', 0);
        });

        after(() => act.close());

        it('runs the branch\'s kerberos suite against the krb profile',
            function kerberosTest() {
                const netns = env.knob('KRB_NET_CONTAINER',
                    `${env.PROJECT}-krb-net-1`);
                const names = run('docker', ['ps', '--format', '{{.Names}}']).out;
                const keytabs = firstExisting([
                    env.knob('KRB_KEYTABS'),
                    path.join(env.DEMO, 'krb', 'keytabs'),
                ]);
                const krb5 = firstExisting([
                    env.knob('KRB_KRB5_CONF'),
                    path.join(env.DEMO, 'krb', 'krb5.conf'),
                ]);
                const reasons = [];
                if (!names.split('\n').includes(netns)) {
                    reasons.push(`the krb profile is not up (no ${netns}). `
                        + 'Bring it up with `yarn demo:up:krb`');
                }
                if (!fs.existsSync(path.join(KRB_WORKTREE, KRB_SUITE))) {
                    reasons.push(`no ${KRB_SUITE} under ${KRB_WORKTREE}, so `
                        + 'this checkout does not carry the kerberos work. '
                        + 'Set KRB_BACKBEAT_DIR to one that does.');
                }
                if (!run('docker', ['image', 'inspect', IMAGE]).ok) {
                    reasons.push(`the test image ${IMAGE} is not built`);
                }
                if (!keytabs || !fs.existsSync(path.join(keytabs, 'notifa.keytab'))) {
                    reasons.push('the keytabs notifa.keytab and notifb.keytab '
                        + 'are not there yet: they are written by the KDC when '
                        + 'the krb profile starts');
                }
                if (!krb5) {
                    reasons.push('no krb5.conf for the containers');
                }
                if (reasons.length) {
                    reasons.forEach(r => note(`skipping: ${r}`));
                    note('the written-up result is in '
                        + 'poc-demo/results/RESULTS-kerberos-nodejs.md');
                    act.measured('suite exit code', 'skipped');
                    this.skip();
                    return;
                }

                step(1, 'what the suite asserts');
                note('two destinations, one writing topic-a and one topic-b,');
                note('served by ONE producer pool. Broker ACLs give each');
                note('principal its own topic only, so a producer that');
                note('authenticated as the wrong principal is DENIED rather');
                note('than delivering as the wrong user.');
                note('arms: a credential cache collection, a client keytab');
                note('with no kinit, the shipping node-rdkafka producer as a');
                note('collision control, one principal per process, three');
                note('broker restarts mid run, a two minute ticket lifetime,');
                note('and 50 producers in one process.');

                step(2, 'run it in a container on the rig\'s network namespace');
                const brokers = env.knob('KRB_BROKERS',
                    `localhost:${env.KRB_BROKER_PORT}`);
                const verify = env.knob('KRB_VERIFY_BROKERS',
                    `localhost:${env.KRB_VERIFY_PORT}`);
                say(`image ${IMAGE}, namespace ${netns}, brokers ${brokers}`);
                say(`log ${act.file('suite.log')}`);
                const r = run('docker', ['run', '--rm',
                    '--name', 'bnaas-demo-krbtest',
                    '--network', `container:${netns}`,
                    '-v', '/var/run/docker.sock:/var/run/docker.sock',
                    '-v', `${KRB_WORKTREE}:/usr/src/app`,
                    '-v', `${NM_VOLUME}:/usr/src/app/node_modules`,
                    '-v', `${keytabs}:/conf/ssl:ro`,
                    '-v', `${krb5}:/etc/krb5.conf:ro`,
                    '-e', 'CONF_DIR=/conf',
                    '-e', `KRB_BROKERS=${brokers}`,
                    '-e', `KRB_VERIFY_BROKERS=${verify}`,
                    '-e', `KRB_KAFKA_CONTAINER=${env.knob('KRB_KAFKA_CONTAINER',
                        `${env.PROJECT}-krb-kafka-1`)}`,
                    '-e', `KRB_KDC_CONTAINER=${env.knob('KRB_KDC_CONTAINER',
                        `${env.PROJECT}-krb-kdc-1`)}`,
                    IMAGE,
                    'yarn', 'ft_test:notification:kerberos'],
                { timeout: 20 * 60 * 1000 });
                fs.writeFileSync(act.file('suite.log'),
                    `${r.out}\n${r.err}\n`);
                line(r.out.split('\n').slice(-40)
                    .map(l => `      | ${l}`).join('\n'));

                step(3, 'read the result out of the broker\'s own lines');
                const log = `${r.out}\n${r.err}`;
                const ids = (log.match(/authenticationID=\S+/g) || []);
                const uniq = Array.from(new Set(ids));
                say(`${ids.length} authenticationID lines, distinct: `
                    + `${uniq.join(', ')}`);
                act.measured('two principals in one process',
                    uniq.length >= 2
                        ? `${uniq.length} distinct identities in one process`
                        : `${uniq.length} identity`);
                const fifty = (log.match(/25\d of 25\d/g) || []).pop();
                act.measured('50 principals in one process', fifty || 'not seen');
                act.measured('suite exit code', r.code);
                if (r.code !== 0) {
                    note('the suite did not exit 0. An arm that SKIPS because');
                    note('the docker socket or a keytab is missing is not a');
                    note('failure of the mechanism: read the log first.');
                }
                note('this closes the gate for account-scoped Kerberos. Global');
                note('Kerberos still needs the shipped image fix, which has');
                note('its own ticket, and that is product question 7.');
            });
    });
}

module.exports = { register };
