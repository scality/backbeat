'use strict';

/**
 * The BNaaS delivery pool demo, as a functional test suite.
 *
 *   docker compose up      (demo/bin/stack-up.sh)
 *   yarn ft_test:demo
 *
 * and watch it with Grafana, Kafka UI and the ZooKeeper browser open beside
 * the terminal. Every act narrates what it is doing and ends with the
 * outcome the rig measured next to the one this run measured. The
 * assertions are the design's own promises: no loss anywhere, no reordering
 * except where an act deliberately shows it, counted drops on the pool, and
 * the stall on the legacy path.
 *
 * Environment:
 *   DEMO_ACTS=02,04,06     run only these acts, in this order
 *   DEMO_PACE=slow|normal|fast
 *   DEMO_WORKGROUPS_STOP_EARLY=1   include the deliberate loss variant
 *   DEMO_SKIP_UNIT_TESTS=1         act 01 points at the suites instead of
 *                                  running them (they take about 4 minutes)
 */

const env = require('./lib/env');
const setup = require('./lib/setup');
const { line } = require('./lib/narrate');

const ACTS = [
    ['01', './acts/01-code-and-tests'],
    ['02', './acts/02-legacy-baseline'],
    ['03', './acts/03-dead-destination'],
    ['04', './acts/04-switch-and-drain'],
    ['05', './acts/05-crashes'],
    ['06', './acts/06-workgroups'],
    ['07', './acts/07-kerberos'],
    ['08', './acts/08-semantics'],
];

const wanted = env.ACTS.length
    ? ACTS.filter(([id]) => env.ACTS.includes(id))
        .sort((a, b) => env.ACTS.indexOf(a[0]) - env.ACTS.indexOf(b[0]))
    : ACTS;

describe('BNaaS delivery pool demo', function demoSuite() {
    this.timeout(30 * 60 * 1000);

    before(async () => {
        line('');
        line(`acts in this run: ${wanted.map(([id]) => id).join(', ')}`);
        await setup.globalSetup();
    });

    after(async () => {
        await setup.globalTeardown();
    });

     
    wanted.forEach(([, mod]) => require(mod).register(setup.ctx));
});
