#!/usr/bin/env node

const program = require('commander');
const url = require('url');
const https = require('https');
const http = require('http');
const { createBackbeatClient } = require('../lib/clients/utils.js');

const pkg = require('../package.json');
const werelogs = require('werelogs');

const config = require('../conf/Config');

const log = new werelogs.Logger('Backbeat:CLI');

werelogs.configure({
    level: config.log.logLevel,
    dump: config.log.dumpLevel,
});

function parseEndpoint(endpoint) {
    try {
        const parsedUrl = new url.URL(endpoint);
        const transport = parsedUrl.protocol.slice(0, -1);
        const host = parsedUrl.hostname;
        const port = parsedUrl.port;

        return { transport, host, port };
    } catch (error) {
        throw new Error(`Invalid endpoint URL: ${endpoint}`);
    }
}

function createClient() {
    const { transport, host, port } = parseEndpoint(process.env.BACKBEAT_ENDPOINT || 'http://127.0.0.1:8000');

    let agent;
    if (transport === 'https') {
        agent = new https.Agent({ keepAlive: true });
    } else {
        agent = new http.Agent({ keepAlive: true });
    }

    return createBackbeatClient({
        transport,
        host,
        port,
        credentials: {
            accessKeyId: process.env.AWS_ACCESS_KEY_ID || 'accessKey1',
            secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY || 'verySecretKey1',
        },
        agent,
    });
}

program
    .version(pkg.version)
    .description('CLI for interacting with the Backbeat service');

program
    .command('list-lifecycle-currents')
    .description('List current lifecycle objects in a bucket')
    .option('-b, --bucket <bucket>', 'Name of the bucket')
    .option('-d, --before-date <date>', 'Limit to keys modified before this date (YYYY-MM-DD)')
    .option('-e, --excluded-data-store-name <name>', 'Exclude specific data store name')
    .option('-enc, --encoding-type <type>', 'Encoding type (e.g., url)', 'url')
    .option('-m, --marker <marker>', 'Marker for pagination')
    .option('-k, --max-keys <number>', 'Maximum number of keys to return', parseInt)
    .option('-p, --prefix <prefix>', 'Filter keys by prefix')
    .action(options => {
        if (!options.bucket) {
            log.error('Error: --bucket option is required.', { options });
            process.exit(1);
        }

        const client = createClient();

        const params = {
            Bucket: options.bucket,
            BeforeDate: options.beforeDate,
            ExcludedDataStoreName: options.excludedDataStoreName,
            EncodingType: options.encodingType,
            Marker: options.marker,
            MaxKeys: options.maxKeys,
            Prefix: options.prefix,
        };

        // Remove undefined parameters
        Object.keys(params).forEach(
            key => params[key] === undefined && delete params[key]
        );

        client.listLifecycleCurrents(params, (err, data) => {
            if (err) {
                log.error('Error listing lifecycle currents', { error: err });
                process.exit(1);
            } else {
                console.log(data);
            }
        });
    });

program
    .command('list-lifecycle-noncurrents')
    .description('List non-current lifecycle objects in a bucket')
    .option('-b, --bucket <bucket>', 'Name of the bucket')
    .option('-d, --before-date <date>', 'Limit to keys modified before this date (YYYY-MM-DD)')
    .option('-e, --excluded-data-store-name <name>', 'Exclude specific data store name')
    .option('-enc, --encoding-type <type>', 'Encoding type (e.g., url)', 'url')
    .option('-km, --key-marker <marker>', 'Key marker for pagination')
    .option('-vm, --version-id-marker <id>', 'Version ID marker for pagination')
    .option('-k, --max-keys <number>', 'Maximum number of keys to return', parseInt)
    .option('-p, --prefix <prefix>', 'Filter keys by prefix')
    .action(options => {
        if (!options.bucket) {
            log.error('Error: --bucket option is required.', { options });
            process.exit(1);
        }

        const client = createClient();

        const params = {
            Bucket: options.bucket,
            BeforeDate: options.beforeDate,
            ExcludedDataStoreName: options.excludedDataStoreName,
            EncodingType: options.encodingType,
            KeyMarker: options.keyMarker,
            VersionIdMarker: options.versionIdMarker,
            MaxKeys: options.maxKeys,
            Prefix: options.prefix,
        };

        // Remove undefined parameters
        Object.keys(params).forEach(
            key => params[key] === undefined && delete params[key]
        );

        client.listLifecycleNonCurrents(params, (err, data) => {
            if (err) {
                log.error('Error listing lifecycle non currents', { error: err });
                process.exit(1);
            } else {
                console.log(data);
            }
        });
    });

program
    .command('list-lifecycle-orphans')
    .description('List orphan lifecycle objects in a bucket')
    .option('-b, --bucket <bucket>', 'Name of the bucket')
    .option('-d, --before-date <date>', 'Limit to keys modified before this date (YYYY-MM-DD)')
    .option('-e, --excluded-data-store-name <name>', 'Exclude specific data store name')
    .option('-enc, --encoding-type <type>', 'Encoding type (e.g., url)', 'url')
    .option('-m, --marker <marker>', 'Marker for pagination')
    .option('-k, --max-keys <number>', 'Maximum number of keys to return', parseInt)
    .option('-p, --prefix <prefix>', 'Filter keys by prefix')
    .action(options => {
        if (!options.bucket) {
            log.error('Error: --bucket option is required.', { options });
            process.exit(1);
        }

        const client = createClient();

        const params = {
            Bucket: options.bucket,
            BeforeDate: options.beforeDate,
            ExcludedDataStoreName: options.excludedDataStoreName,
            EncodingType: options.encodingType,
            Marker: options.marker,
            MaxKeys: options.maxKeys,
            Prefix: options.prefix,
        };

        // Remove undefined parameters
        Object.keys(params).forEach(
            key => params[key] === undefined && delete params[key]
        );

        client.listLifecycleOrphans(params, (err, data) => {
            if (err) {
                log.error('Error listing lifecycle orphans', { error: err });
                process.exit(1);
            } else {
                console.log(data);
            }
        });
    });

// Parse the command-line arguments
program.parse(process.argv);

// If no command is provided, display help
if (!process.argv.slice(2).length) {
    program.outputHelp();
}
