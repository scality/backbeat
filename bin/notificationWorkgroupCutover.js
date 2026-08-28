'use strict';

/**
 * Delivery pool workgroups cutover tool.
 *
 * A workgroup is one consumer group over the delivery topic plus the slice
 * of destinations it owns. Moving from one set of workgroups to the next is
 * a generation change: this tool writes a barrier record on every partition,
 * publishes the new document with those barrier offsets in zookeeper, and
 * commits the barrier offsets into the new generation's consumer groups
 * while they are still empty.
 *
 * It never writes the offsets of a previous generation's group, and never
 * reads them to seed a new one. Pre-seed strictly before starting any worker
 * of the new generation, and stop the previous generation only once verify
 * exits 0.
 *
 * Usage: node bin/notificationWorkgroupCutover.js <command> [options]
 */

const { program } = require('commander');
const werelogs = require('werelogs');

const config = require('../lib/Config');
const WorkgroupCutover =
    require('../extensions/notification/deliveryWorker/WorkgroupCutover');

const log = new werelogs.Logger('Backbeat:Notification:WorkgroupCutover');

werelogs.configure({
    level: config.log.logLevel,
    dump: config.log.dumpLevel,
});

const EXIT_OK = 0;
const EXIT_FAILED = 1;
const EXIT_NOT_DRAINED = 2;

let tool = null;

function collect(value, previous) {
    return previous.concat([value]);
}

function write(text) {
    process.stdout.write(`${text}\n`);
}

function documentOptions(command) {
    return command
        .option('--modulo <n>', 'hashmod modulo for the new document')
        .option('--workgroup <id:remainders>',
            'hashmod workgroup, as <id>:<r,r,...>, repeatable', collect, [])
        .option('--static <id:destinationIds>',
            'static workgroup, as <id>:<destId,...>, repeatable', collect, [])
        .option('--spec <path>',
            'JSON file holding the workgroups array, instead of ' +
            '--modulo/--workgroup/--static')
        .option('--generation <n>',
            'generation to write, defaults to the current one plus one')
        .option('--force',
            'allow a generation that is not the current one plus one');
}

function fromGroupOption(command) {
    return command.option('--from-group <groupId>',
        'previous generation group to drain-report on, repeatable, ' +
        'defaults to the groups the document records having replaced, and ' +
        'to a set derived from the workgroups it lists for a document ' +
        'written before the tool recorded them', collect, []);
}

function timeoutOption(command) {
    return command.option('--timeout <ms>', 'kafka call timeout', '10000');
}

function printDocument(doc) {
    write(JSON.stringify(doc, null, 4));
}

function printDrainReport(report) {
    write('');
    write(WorkgroupCutover.formatDrainReport(report));
}

function printPlan(result) {
    printDocument(result.doc);
    write('');
    write(`consumer groups: ${result.groupIds.join(', ')}`);
    write(`previous groups: ${result.previousGroupIds.join(', ')}`);
    write('');
    write('destination                          workgroup');
    result.destinations.forEach(entry => write(
        `${entry.destinationId.padEnd(36)} ${entry.workgroupId}`));
}

function printCutover(result) {
    printDocument(result.doc);
    write('');
    write(`seeded consumer groups: ${result.groupIds.join(', ')}`);
    printDrainReport(result.report);
    write('');
    write('the previous generation is still responsible for everything ' +
        'before its barrier: do not stop it, and do not start the new ' +
        'generation, until verify exits 0');
}

/**
 * Prints what a command produced and resolves its exit code
 *
 * @param {String} command - command name
 * @param {Object} result - what the command called back with
 * @return {Number} process exit code
 */
function printResult(command, result) {
    if (command === 'show') {
        if (!result) {
            write('no workgroups document in zookeeper');
            return EXIT_OK;
        }
        printDocument(result);
        return EXIT_OK;
    }
    if (command === 'plan') {
        printPlan(result);
        return EXIT_OK;
    }
    if (command === 'cutover') {
        printCutover(result);
        return EXIT_OK;
    }
    if (command === 'preseed') {
        write(`seeded consumer groups: ${result.groupIds.join(', ')}`);
        return EXIT_OK;
    }
    printDrainReport(result);
    if (result.drained) {
        write('');
        write('every previous group is past every barrier, the previous ' +
            'generation can be stopped');
        return EXIT_OK;
    }
    write('');
    write('the previous generation has not reached every barrier yet, ' +
        'stopping it now would leave those records undelivered');
    return EXIT_NOT_DRAINED;
}

function run(command, options) {
    tool = new WorkgroupCutover({
        kafkaConfig: config.kafka,
        zkConfig: config.zookeeper,
        notifConfig: config.extensions.notification,
        options,
        logger: log,
    });
    return tool[command]((err, result) => {
        if (err) {
            log.error('workgroup cutover command failed', {
                method: 'notificationWorkgroupCutover',
                command,
                error: err.description || err.message,
            });
            return tool.close(() => process.exit(EXIT_FAILED));
        }
        const code = printResult(command, result);
        return tool.close(() => process.exit(code));
    });
}

program
    .name('notificationWorkgroupCutover')
    .description('manage the delivery pool workgroups generations');

timeoutOption(program.command('show')
    .description('print the workgroups document currently in zookeeper'))
    .action(options => run('show', options));

timeoutOption(documentOptions(program.command('plan')
    .description('print the document cutover would write and the ' +
        'destination to workgroup map it implies, writing nothing')))
    .action(options => run('plan', options));

timeoutOption(fromGroupOption(documentOptions(program.command('cutover')
    .description('produce barriers, write the document, pre-seed the new ' +
        'groups'))))
    .action(options => run('cutover', options));

timeoutOption(program.command('preseed')
    .description('pre-seed the groups of the document already in zookeeper'))
    .action(options => run('preseed', options));

timeoutOption(fromGroupOption(program.command('verify')
    .description('report how far the previous generation has drained, and ' +
        'how many records it has already had delivered twice by running on ' +
        'past its barriers')))
    .action(options => run('verify', options));

program.parse(process.argv);

process.on('SIGTERM', () => {
    log.info('received SIGTERM, exiting');
    if (!tool) {
        process.exit(EXIT_FAILED);
    }
    tool.close(() => process.exit(EXIT_FAILED));
});
