'use strict';

/**
 * Delivery pool seeding tool, for the deployment model where every
 * processor or worker container is stopped and the new ones started in one
 * run, with the workers reading today's internal topic.
 *
 * Before the new workers start, each of their consumer groups is committed at
 * the lowest offset any destination it owns had been served up to, and each
 * destination's own offset is written to zookeeper as a watermark, so the
 * workers deliver nothing twice and a destination that was behind receives
 * its backlog.
 *
 * Usage: node bin/notificationDeliverySeed.js <command> [options]
 *
 *   seed-from-processors --generation N     migration from the per-destination
 *                                           queue processors
 *   seed-from-generation --from N --to M    a workgroup layout change
 *
 * Run it after the previous containers are stopped and before the new ones
 * start: a group with live members cannot be seeded.
 */

const { program } = require('commander');
const werelogs = require('werelogs');

const config = require('../lib/Config');
const DeliverySeeder =
    require('../extensions/notification/deliveryWorker/DeliverySeeder');

const log = new werelogs.Logger('Backbeat:Notification:DeliverySeed');

werelogs.configure({
    level: config.log.logLevel,
    dump: config.log.dumpLevel,
});

const EXIT_OK = 0;
const EXIT_FAILED = 1;

let tool = null;

function write(text) {
    process.stdout.write(`${text}\n`);
}

function run(method, options) {
    tool = new DeliverySeeder({
        kafkaConfig: config.kafka,
        zkConfig: config.zookeeper,
        notifConfig: config.extensions.notification,
        options,
        logger: log,
    });
    return tool[method]((err, result) => {
        if (err) {
            log.error('delivery seed command failed', {
                method: 'notificationDeliverySeed',
                command: method,
                error: err.description || err.message,
            });
            write(`error: ${err.description || err.message}`);
            return tool.close(() => process.exit(EXIT_FAILED));
        }
        write(DeliverySeeder.formatResult(result));
        write('');
        write('every group of the generation is seeded: the workers can start');
        return tool.close(() => process.exit(EXIT_OK));
    });
}

program
    .name('notificationDeliverySeed')
    .description('seed the consumer groups of a delivery pool generation ' +
        'before its workers start');

program.command('seed-from-processors')
    .description('seed generation N from the per-destination queue ' +
        'processor groups, for the migration off the processors')
    .requiredOption('--generation <n>', 'generation to seed')
    .option('--force', 'seed even when the document in zookeeper is at ' +
        'another generation')
    .option('--timeout <ms>', 'kafka call timeout', '10000')
    .action(options => run('seedFromProcessors', options));

program.command('seed-from-generation')
    .description('seed generation M from the groups of generation N, for a ' +
        'workgroup layout change')
    .requiredOption('--from <n>', 'previous generation')
    .requiredOption('--to <m>', 'generation to seed')
    .option('--previous-spec <path>', 'JSON file holding the previous ' +
        'generation\'s workgroups document, when it is not in zookeeper ' +
        'under history/')
    .option('--force', 'seed even when the document in zookeeper is at ' +
        'another generation')
    .option('--timeout <ms>', 'kafka call timeout', '10000')
    .action(options => run('seedFromGeneration', options));

program.parse(process.argv);

process.on('SIGTERM', () => {
    log.info('received SIGTERM, exiting');
    if (!tool) {
        process.exit(EXIT_FAILED);
    }
    tool.close(() => process.exit(EXIT_FAILED));
});
