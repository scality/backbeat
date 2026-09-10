#!/usr/bin/env node
'use strict';

/**
 * Render every config the demo needs into demo/conf/generated, from the
 * templates in demo/conf/templates and the ports in demo/.env.
 *
 * The test suite generates its own per-act configs through the same module,
 * so what an operator starts by hand and what an act starts cannot drift.
 *
 * Usage: node render-cli.js [--quiet]
 */

const env = require('./env');
const conf = require('./conf');

const quiet = process.argv.includes('--quiet');
const out = [];

function write(name, doc) {
    const file = conf.write(name, doc);
    out.push(file);
    return file;
}

write('cloudserver-config.json', conf.cloudserver());
write('backbeat-legacy.json', conf.backbeat({ pool: false }));
write('backbeat-pool.json', conf.backbeat({ pool: true }));
write('backbeat-pool-chaos.json',
    conf.backbeat({ pool: true, threeLanes: true, allHealthy: true }));
write('backbeat-pool-wg.json',
    conf.backbeat({ pool: true, allHealthy: true, workgroups: true }));

if (!quiet) {
    process.stdout.write(`demo configs rendered from ${conf.TEMPLATES}\n`);
    process.stdout.write(`port offset ${env.PORT_OFFSET}: kafka `
        + `${env.KAFKA_PORT}, zookeeper ${env.ZK_PORT}, mongo `
        + `${env.MONGO_PORT}, cloudserver ${env.CLOUDSERVER_PORT}\n`);
    process.stdout.write(`worker probes ${env.probePort(1)} and up, `
        + `populator probe ${env.POPULATOR_PROBE_PORT}, all bound 0.0.0.0 so `
        + 'prometheus can reach them from its container\n');
    out.forEach(f => process.stdout.write(`  ${f}\n`));
    process.stdout.write(`shims loaded with --require from ${conf.SHIM_DIR}\n`);
    process.stdout.write(`backbeat ${env.BACKBEAT_DIR}\n`);
    process.stdout.write(`cloudserver ${env.CLOUDSERVER_DIR}\n`);
    process.stdout.write(`node ${env.NODE}\n`);
}
