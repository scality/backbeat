const program = require('commander');

const { SetupReplication } = require('./replication');
const werelogs = require('werelogs');
const Logger = werelogs.Logger;
const config = require('../conf/Config');
werelogs.configure({
    level: config.log.logLevel,
    dump: config.log.dumpLevel,
});


program
    .version('1.0.0')
    .command('setup')
    .option('--source-bucket <name>', '[required] source bucket name')
    .option('--source-profile <name>',
            'aws/credentials profile to use for source')
    .option('--target-bucket <name>', 'target bucket name')
    .option('--target-profile <name>',
            'aws/credentials profile to use for target')
    .action(options => {
        const log = new Logger('BackbeatSetup').newRequestLogger();

        const sourceBucket = options.sourceBucket;
        const targetBucket = options.targetBucket;
        const sourceProfile = options.sourceProfile;
        const targetProfile = options.targetProfile;

        // Required options
        if (!sourceBucket || !targetBucket) {
            program.commands.find(n => n._name === 'setup').help();
            process.exit(1);
        }

        const s = new SetupReplication(sourceBucket, targetBucket,
            sourceProfile, targetProfile, log, config);
        s.setupReplication(err => {
            if (err) {
                log.error('replication setup failed', {
                    error: err.message,
                });
                process.exit(1);
            }
            log.info('replication setup successful');
            process.exit();
        });
    });

program
    .command('validate')
    .option('--source-bucket <name>', 'source bucket name')
    .option('--source-profile <name>',
            'aws/credentials profile to use for source')
    .option('--target-bucket <name>', 'target bucket name')
    .option('--target-profile <name>',
            'aws/credentials profile to use for target')
    .action(options => {
        const log = new Logger('BackbeatSetup').newRequestLogger();

        const sourceBucket = options.sourceBucket;
        const targetBucket = options.targetBucket;
        const sourceProfile = options.sourceProfile;
        const targetProfile = options.targetProfile;

        // Required options
        if (!sourceBucket || !targetBucket) {
            program.commands.find(n => n._name === 'validate').help();
            process.exit(1);
        }

        const s = new SetupReplication(sourceBucket, targetBucket,
            sourceProfile, targetProfile, log, config);
        s.checkSanity(err => {
            if (err) {
                log.error('replication validation check failed', {
                    error: err.message,
                });
                process.exit(1);
            }
            log.info('replication is correctly setup');
            process.exit();
        });
    });

program.parse(process.argv);
const validCommands = program.commands.map(n => n._name);

// Is the command given invalid or are there too few arguments passed
if (!validCommands.includes(process.argv[2])) {
    program.help();
    process.exit(1);
}
